from typing import Tuple, List
import json

_IMPORT_ERROR = None
try:
    from postgres_utils import get_pg_conn
    from db_table_utils import ensure_table_and_columns, insert_records
except Exception as e:
    # don't raise at import time; surface error when handler actually runs
    get_pg_conn = None
    ensure_table_and_columns = None
    insert_records = None
    _IMPORT_ERROR = e


def _process_maintenances(client, client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    if _IMPORT_ERROR:
        print(f"[ERROR] Erro de importação em maintenance.py: {_IMPORT_ERROR}")
        return 0, []

    """Fetch maintenances from Zabbix and store them into Postgres table `nome_tabela`"""
    try:
        maintenances = client.get_maintenances()
    except Exception as e:
        print(f"[ERROR] Falha ao obter maintenances do Zabbix: {e}")
        return 0, []

    if not maintenances:
        print("[INFO] Nenhuma maintenance retornada pela API.")
        return 0, []

    # Normalize records to match expected DB columns
    records = []
    for m in maintenances:
        active_since = m.get('active_since')
        active_till = m.get('active_till')
        duration_seconds = None
        if active_since and active_till:
            try:
                active_since_int = int(active_since)
                active_till_int = int(active_till)
                if active_till_int > active_since_int:
                    duration_seconds = active_till_int - active_since_int
            except (ValueError, TypeError):
                duration_seconds = None

        records.append({
            'maintenanceid': m.get('maintenanceid'),
            'name': m.get('name'),
            'maintenance_type': m.get('maintenance_type'),
            'description': m.get('description'),
            'active_since': active_since,
            'active_till': active_till,
            'duration_seconds': duration_seconds,
            'hostgroups': json.dumps(m.get('hostgroups', [])),
            'hosts': json.dumps(m.get('hosts', [])),
            'timeperiods': json.dumps(m.get('timeperiods', [])),
            'tags': json.dumps(m.get('tags', [])),
        })

    conn = None
    try:
        conn = get_pg_conn()
        if not conn:
            print("[ERROR] Não foi possível conectar ao Postgres para salvar maintenances.")
            return 0, []

        # Ensure table exists and columns inferred
        ensure_table_and_columns(conn, nome_tabela, records, operacao)

        # Insert records
        inseridos, ids = insert_records(conn, nome_tabela, records, operacao)
        return inseridos, ids
    except Exception as e:
        print(f"[ERROR] Falha ao salvar maintenances: {e}")
        return 0, []
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


process = _process_maintenances