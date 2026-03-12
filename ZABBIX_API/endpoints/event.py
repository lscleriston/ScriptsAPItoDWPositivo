from typing import Tuple, List
import time
from datetime import datetime, timedelta
import json
import os

_IMPORT_ERROR = None
try:
    from postgres_utils import get_pg_conn, get_last_id_and_range, update_last_id
    from db_table_utils import ensure_table_and_columns, insert_records as ps_insert_records, recreate_events_table_with_schema
except Exception as e:
    # don't raise at import time; surface error when handler actually runs
    get_pg_conn = None
    get_last_id_and_range = None
    update_last_id = None
    ensure_table_and_columns = None
    ps_insert_records = None
    _IMPORT_ERROR = e


def _obter_ultimo_id_banco(client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    conn = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        # read last id and range for this client+endpoint from control table
        ultimo_id_tb, range_id = get_last_id_and_range(cursor, client_name, nome_tabela, operacao, nome_tabela_ultimo_id)
        cursor.close()
        conn.close()
        return ultimo_id_tb, range_id
    except Exception as err:
        print(f"[ERRO] Postgres ao obter ultimo id: {err}")
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass
        return None, 0


def _atualizar_tb_ultimo_id(client_name, nome_tabela_ultimo_id, nome_tabela, operacao, ultimo_id, pagina_ultimo_id):
    conn = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        update_last_id(cursor, nome_tabela_ultimo_id, client_name, nome_tabela, operacao, ultimo_id, pagina_ultimo_id)
        conn.commit()
        cursor.close()
        conn.close()
        print("[INFO] Atualizacao da tb_ultimo_id realizada.")
    except Exception as err:
        print(f"[ERRO] Postgres ao atualizar tb_ultimo_id: {err}")
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass
        raise


def _processar_events(client, client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    """Processa o endpoint event usando o client Zabbix."""
    ultimo_id, range_id = _obter_ultimo_id_banco(client_name, nome_tabela, operacao, nome_tabela_ultimo_id)
    # Allow forcing a full historical collection even if control table has a last id
    try:
        force_full = str(__import__('os').environ.get('FORCE_FULL_HISTORY', '')).lower() in ('1', 'true', 'yes')
        if force_full:
            ultimo_id = None
    except Exception:
        pass

    # Se for a tabela de events, normalmente recria/limpa o schema legado.
    # Para reprocessamento seletivo, permita pular a recriação via env `SKIP_RECREATE_EVENTS_TABLE=1`.
    try:
        skip_recreate = False
        try:
            skip_recreate = str(__import__('os').environ.get('SKIP_RECREATE_EVENTS_TABLE', '')).lower() in ('1', 'true', 'yes')
        except Exception:
            skip_recreate = False

        if not skip_recreate and (nome_tabela.lower().endswith('tb_zabbix_event') or nome_tabela.lower().endswith('tb_zabbix_events')):
            conn_tmp = get_pg_conn()
            if conn_tmp:
                recreate_events_table_with_schema(conn_tmp, nome_tabela)
                conn_tmp.close()
    except Exception as e:
        print(f"[WARN] Falha ao recriar tabela de events antes da coleta: {e}")

    try:
        # Define período de coleta
        if ultimo_id:
            # Se existe ultimo_id, continua incremental a partir dele
            time_from = ultimo_id
        else:
            # Pega um histórico maior quando não há último id — configurável via HISTORY_DAYS (dias)
            try:
                history_days = int(os.environ.get('HISTORY_DAYS', '90'))
            except Exception:
                history_days = 90
            time_from = int((datetime.now() - timedelta(days=history_days)).timestamp())

        # time_till: fim do dia anterior para evitar dados parciais
        time_till = int((datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59).timestamp())

        # Try to collect trigger IDs from DB (legacy behavior used trigger list to scope event.get)
        trigger_ids = None
        try:
            conn_tr = get_pg_conn()
            if conn_tr:
                cur_tr = conn_tr.cursor()
                try:
                    schema = __import__('os').environ.get('PG_SCHEMA', 'dw_positivo')
                    # Filter by operacao (client name) to avoid sending other clients' trigger IDs,
                    # which would generate a massive payload that WAFs may block.
                    cur_tr.execute(f"SELECT DISTINCT triggerid FROM {schema}.tb_zabbix_triggers WHERE operacao = %s", (operacao,))
                    rows = cur_tr.fetchall()
                    trigger_ids = [r[0] for r in rows if r and r[0]]
                except Exception:
                    trigger_ids = None
                try:
                    cur_tr.close()
                except:
                    pass
                try:
                    conn_tr.close()
                except:
                    pass
        except Exception:
            trigger_ids = None

        # Option to force collecting without objectids (collect everything)
        force_no_objectids = False
        try:
            force_no_objectids = str(__import__('os').environ.get('FORCE_NO_OBJECTIDS', '')).lower() in ('1', 'true', 'yes')
        except Exception:
            force_no_objectids = False

        # Obtém eventos em páginas (passa objectids se tivermos trigger ids, para replicar comportamento legado)
        if force_no_objectids:
            trigger_ids = None
        try:
            page_size = int(__import__('os').environ.get('EVENT_PAGE_SIZE', '10000'))
        except Exception:
            page_size = 10000

        # Some Zabbix servers don't accept offset-based pagination. Use time-window slices instead.
        try:
            chunk_days = int(__import__('os').environ.get('EVENT_CHUNK_DAYS', '1'))
        except Exception:
            chunk_days = 1

        events_data = []
        slice_start = int(time_from)
        slice_end_limit = int(time_till)
        chunk_seconds = chunk_days * 86400
        # Optionally reprocess only missing slices: if `REPROCESS_MISSING_SLICES=1`, skip slice when DB already has events for it.
        try:
            reprocess_missing = str(__import__('os').environ.get('REPROCESS_MISSING_SLICES', '')).lower() in ('1', 'true', 'yes')
        except Exception:
            reprocess_missing = False

        schema = __import__('os').environ.get('PG_SCHEMA', 'dw_positivo')
        table_short = nome_tabela.split('.')[-1]

        while slice_start <= slice_end_limit:
            slice_end = min(slice_start + chunk_seconds - 1, slice_end_limit)

            # If requested, check DB for existing events in this time window and skip if present
            if reprocess_missing:
                try:
                    conn_chk = get_pg_conn()
                    cur_chk = conn_chk.cursor()
                    cur_chk.execute(f"SELECT count(*) FROM {schema}.{table_short} WHERE operacao = %s AND start_time BETWEEN to_timestamp(%s) AND to_timestamp(%s)", (operacao, slice_start, slice_end))
                    cnt = cur_chk.fetchone()[0]
                    cur_chk.close(); conn_chk.close()
                    if cnt and cnt > 0:
                        slice_start = slice_end + 1
                        continue
                except Exception:
                    # if check fails, proceed to attempt API call for this slice
                    try:
                        if cur_chk:
                            cur_chk.close()
                    except:
                        pass
                    try:
                        if conn_chk:
                            conn_chk.close()
                    except:
                        pass

            try:
                if trigger_ids:
                    page = client.get_events(time_from=slice_start, time_till=slice_end, objectids=trigger_ids)
                else:
                    page = client.get_events(time_from=slice_start, time_till=slice_end)
            except Exception as e:
                print(f"[ZabbixClient ERROR] call event.get failed for slice {slice_start}-{slice_end}: {e}")
                page = []

            if page:
                events_data.extend(page)
            slice_start = slice_end + 1

        # dedupe by eventid
        seen_e = set()
        unique_events = []
        for ev in events_data:
            eid = ev.get('eventid')
            if eid and eid not in seen_e:
                seen_e.add(eid)
                unique_events.append(ev)
        events_data = unique_events

        if not events_data:
            print('[INFO] Nenhum evento encontrado.')
            return 0, []

        # Processa os dados dos eventos e aplica lógica de resolução/duração/SLA
        events_processados = []
        current_date = datetime.now()

        # Prepare batch resolution lookup: coletar todos os r_eventid válidos e buscar em uma chamada
        r_ids = []
        event_clock_map = {}
        for event in events_data:
            r_eventid = event.get('r_eventid') or event.get('rEventid') or event.get('r_event')
            try:
                problem_time = int(event.get('clock'))
            except Exception:
                problem_time = None
            if r_eventid:
                r_ids.append(str(r_eventid))
                # guardar o clock por precaução (não usado na busca em lote)
                event_clock_map[str(r_eventid)] = problem_time

        resolution_map = {}
        if r_ids:
            try:
                resolutions = client.get_events_by_ids(list(dict.fromkeys(r_ids)), value=0)
                for r in resolutions:
                    if r and r.get('eventid'):
                        resolution_map[str(r.get('eventid'))] = r
            except Exception:
                resolution_map = {}

            # Buscar acknowledgements em lote (primeiro ack por evento) e resolver nomes de usuário
            ack_map = {}
            try:
                BATCH = 200
                event_ids = [e.get('eventid') for e in events_data if e.get('eventid')]
                for i in range(0, len(event_ids), BATCH):
                    batch = event_ids[i:i+BATCH]
                    try:
                        a_res = client.get_acknowledges_for_events(batch)
                    except Exception:
                        a_res = []
                    for ev in a_res:
                        eid = ev.get('eventid')
                        acks = ev.get('acknowledges') or []
                        if not acks:
                            continue
                        # pegar primeiro ack (ordenar por clock)
                        try:
                            first = sorted(acks, key=lambda x: int(x.get('clock', 0)))[0]
                        except Exception:
                            first = acks[0]
                        ack_map[eid] = {
                            'ack_time': int(first.get('clock')) if first.get('clock') else None,
                            'ack_description': first.get('message') if first.get('message') else None,
                            'ack_user_id': first.get('userid') if first.get('userid') else None
                        }
            except Exception:
                ack_map = {}

            # Resolver nomes de usuário em lote
            user_map = {}
            try:
                user_ids = [v.get('ack_user_id') for v in ack_map.values() if v.get('ack_user_id')]
                user_ids = list(dict.fromkeys(user_ids))
                if user_ids:
                    users = client.get_users(user_ids)
                    for u in users:
                        uid = u.get('userid')
                        full = (u.get('name') or '').strip()
                        if u.get('surname'):
                            full = (full + ' ' + u.get('surname').strip()).strip()
                        display = full or u.get('username') or uid
                        user_map[str(uid)] = display
            except Exception:
                user_map = {}
        for event in events_data:
            r_eventid = event.get('r_eventid') or event.get('rEventid') or event.get('r_event')
            try:
                problem_time = int(event.get('clock'))
            except Exception:
                problem_time = None

            # obter resolução a partir do mapa de resoluções em lote
            resolution = resolution_map.get(str(r_eventid)) if r_eventid else None

            start_time_dt = datetime.fromtimestamp(problem_time) if problem_time else None
            end_time_dt = None
            if resolution and resolution.get('clock'):
                try:
                    end_time_dt = datetime.fromtimestamp(int(resolution.get('clock')))
                except Exception:
                    end_time_dt = None

            duration_seconds = (end_time_dt - start_time_dt).total_seconds() if end_time_dt and start_time_dt else None

            # segundos no período de referência
            seconds_in_period = None
            if end_time_dt:
                if end_time_dt.year == current_date.year and end_time_dt.month == current_date.month:
                    reference_end = current_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
                    reference_start = end_time_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    seconds_in_period = (reference_end - reference_start).total_seconds() + 86400
                else:
                    first_day_of_month = end_time_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    if end_time_dt.month == 12:
                        first_day_next_month = end_time_dt.replace(year=end_time_dt.year+1, month=1, day=1)
                    else:
                        first_day_next_month = end_time_dt.replace(month=end_time_dt.month+1, day=1)
                    seconds_in_period = (first_day_next_month - first_day_of_month).total_seconds()

            # cálculo do SLA
            event_sla_percentage = None
            if end_time_dt and duration_seconds is not None and seconds_in_period and seconds_in_period > 0:
                event_sla_percentage = (1 - (duration_seconds / seconds_in_period)) * 100

            hosts = event.get('hosts') or []
            host_id = hosts[0]['hostid'] if hosts and isinstance(hosts, list) and hosts[0].get('hostid') else None
            host_name = hosts[0].get('name') if hosts and isinstance(hosts, list) and hosts[0].get('name') else 'N/A'

            # attach ack info if available
            ack_info = None
            try:
                ack_info = ack_map.get(str(event.get('eventid'))) if isinstance(ack_map, dict) else None
            except Exception:
                ack_info = None

            events_processados.append({
                'eventid': event.get('eventid'),
                'trigger_id': event.get('objectid'),
                'host_id': host_id,
                'host_name': host_name,
                'event_name': event.get('name'),
                'start_time': start_time_dt.strftime('%Y-%m-%d %H:%M:%S') if start_time_dt else None,
                'end_time': end_time_dt.strftime('%Y-%m-%d %H:%M:%S') if end_time_dt else None,
                'duration_seconds': duration_seconds,
                'duration_human': (str(end_time_dt - start_time_dt) if end_time_dt and start_time_dt else None),
                'seconds_in_period': seconds_in_period,
                'sla_percentage': (round(event_sla_percentage, 6) if event_sla_percentage is not None else None),
                'period_type': ('current_month_partial' if end_time_dt and end_time_dt.year == current_date.year and end_time_dt.month == current_date.month else 'full_month'),
                'severity': event.get('severity', 'N/A'),
                'problem_eventid': event.get('eventid'),
                'resolution_eventid': (resolution.get('eventid') if resolution else None),
                'status': ('resolved' if resolution else 'unresolved'),
                'operacao': operacao,
                'clock': event.get('clock'),
                'ack_time': (datetime.fromtimestamp(int(ack_info.get('ack_time'))).strftime('%Y-%m-%d %H:%M:%S') if ack_info and ack_info.get('ack_time') else None),
                'ack_time_seconds': ((ack_info.get('ack_time') - int(event.get('clock'))) if ack_info and ack_info.get('ack_time') and event.get('clock') else None),
                'ack_description': (ack_info.get('ack_description') if ack_info else None),
                'ack_user': (ack_info.get('ack_user_id') if ack_info else None),
                'ack_user_name': (user_map.get(str(ack_info.get('ack_user_id'))) if ack_info and ack_info.get('ack_user_id') else None)
            })

        # Salva no banco
        inseridos, ids_inseridos = _salvar_events_no_banco(events_processados, nome_tabela, operacao)

        if inseridos and events_processados:
            # Usa o timestamp do último evento (do payload original) como referência
            try:
                clocks = [int(e.get('clock')) for e in events_data if e.get('clock')]
                if clocks:
                    ultimo_clock = max(clocks)
                    _atualizar_tb_ultimo_id(client_name, nome_tabela_ultimo_id, nome_tabela, operacao, ultimo_clock, 0)
            except Exception:
                pass

        return inseridos, ids_inseridos

    except Exception as e:
        print(f"[ERROR] Falha ao processar events: {e}")
        return 0, []


def _salvar_events_no_banco(events, nome_tabela, operacao):
    """Salva events no banco de dados PostgreSQL"""
    conn = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()

        # Garante que a tabela existe e tem colunas necessárias.
        # (Não recriar a tabela aqui — a recriação/limpeza deve ser feita uma única vez
        #  antes da coleta quando necessário, para evitar locks e perda de dados.)
        ensure_table_and_columns(conn, nome_tabela, events[:1], operacao)

        # Insere events (não remove antigos - events são históricos)
        inseridos, ids_inseridos = ps_insert_records(conn, nome_tabela, events, operacao)

        conn.commit()
        cursor.close()

        return inseridos, ids_inseridos

    except Exception as err:
        print(f"[ERRO] Falha ao salvar events: {err}")
        if conn:
            conn.rollback()
        return 0, []
    finally:
        if conn:
            conn.close()


# generic entrypoint required by runner
process = _processar_events