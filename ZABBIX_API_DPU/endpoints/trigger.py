from typing import Tuple, List
import json
from postgres_utils import get_pg_conn
from db_table_utils import ensure_table_and_columns, insert_records


def _process_triggers(client, client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    """Fetch triggers from Zabbix and store them into Postgres table `nome_tabela`"""
    try:
        triggers = client.get_triggers()
    except Exception as e:
        print(f"[ERROR] Falha ao obter triggers do Zabbix: {e}")
        return 0, []

    if not triggers:
        print("[INFO] Nenhuma trigger retornada pela API.")
        return 0, []

    # Normalize records to match expected DB columns
    records = []
    for t in triggers:
        hostid = None
        hostname = None
        if t.get('hosts') and isinstance(t.get('hosts'), list) and t['hosts']:
            hostid = t['hosts'][0].get('hostid')
            hostname = t['hosts'][0].get('name')
        lastchange = None
        try:
            if t.get('lastchange'):
                lastchange = int(t.get('lastchange'))
        except Exception:
            lastchange = None
        # Serializa tags como JSON string para armazenar no banco
        tags_raw = t.get('tags')
        tags = json.dumps(tags_raw, ensure_ascii=False) if tags_raw else None
        records.append({
            'triggerid': t.get('triggerid'),
            'description': t.get('description'),
            'priority': int(t.get('priority')) if t.get('priority') is not None else None,
            'status': int(t.get('status')) if t.get('status') is not None else None,
            'hostid': hostid,
            'hostname': hostname,
            'lastchange': lastchange,
            'tags': tags,
        })

    conn = None
    try:
        conn = get_pg_conn()
        if not conn:
            print("[ERROR] Não foi possível conectar ao Postgres para salvar triggers.")
            return 0, []
        # Use schema-qualified plural table name to match legacy scripts
        import os
        schema = os.environ.get('PG_SCHEMA', 'dw_positivo')
        triggers_table = f"{schema}.tb_zabbix_triggers"

        # Ensure table exists and columns inferred
        ensure_table_and_columns(conn, triggers_table, records, operacao)

        # Insert records
        inseridos, ids = insert_records(conn, triggers_table, records, operacao)
        return inseridos, ids
    except Exception as e:
        print(f"[ERROR] Falha ao salvar triggers: {e}")
        return 0, []
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


process = _process_triggers
