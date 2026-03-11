import os
import psycopg2
from typing import Tuple, Optional
from datetime import date


def _pg_params():
    return {
        'host': os.environ.get('PG_HOST', 'localhost'),
        'port': int(os.environ.get('PG_PORT', 5432)) if os.environ.get('PG_PORT') else 5432,
        'dbname': os.environ.get('PG_DB') or os.environ.get('PG_DATABASE'),
        'user': os.environ.get('PG_USER'),
        'password': os.environ.get('PG_PASSWORD'),
    }


def get_pg_conn(connect_timeout: int = 5):
    params = _pg_params()
    if not params.get('dbname') or not params.get('user'):
        raise RuntimeError('Postgres configuration missing in environment')
    # add a short connect timeout to avoid hanging on DNS/network issues
    conn = psycopg2.connect(host=params['host'], port=params['port'], dbname=params['dbname'], user=params['user'], password=params['password'], connect_timeout=connect_timeout)
    # Use autocommit to avoid transaction-abort state carrying over between failed statements
    try:
        conn.autocommit = True
    except Exception:
        pass
    return conn


def _qualify(name: str) -> str:
    schema = os.environ.get('PG_SCHEMA') or 'public'
    if '.' in name:
        return name
    return f"{schema}.{name}"


def _ensure_control_table(cursor, control_table: str):
    # create table if missing and ensure expected columns exist
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {control_table} (client_name text, table_name text, operacao text, last_data timestamp, metadata jsonb, updated_at timestamptz default now(), PRIMARY KEY (client_name, table_name, operacao))")
    # Ensure columns exist (use ALTER TABLE ... ADD COLUMN IF NOT EXISTS for robustness)
    try:
        cursor.execute(f"ALTER TABLE {control_table} ADD COLUMN IF NOT EXISTS last_data timestamp")
        cursor.execute(f"ALTER TABLE {control_table} ADD COLUMN IF NOT EXISTS metadata jsonb")
        cursor.execute(f"ALTER TABLE {control_table} ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now()")
    except Exception:
        # best-effort: ignore failures here
        pass


def get_last_date(cursor, client_name: str, nome_tabela: str, operacao: str, nome_tabela_ultimo_id: Optional[str] = None) -> Optional[date]:
    """Return last processed date for a client/table/operacao.

    Priority:
    1. tb_controler_consulta_api cache (fastest, updated after each insert).
    2. MAX(data) from the data table filtered by operacao (source of truth).
    3. None — caller falls back to start_date from .env.
    """
    client_key = (client_name or '').strip().lower()
    operacao_key = (operacao or '').strip().upper()
    control_name = nome_tabela_ultimo_id or os.environ.get('TB_CONTROLER_CONSULTA_API', 'tb_controler_consulta_api')
    control_table = _qualify(control_name)

    # 1 — try cache
    try:
        _ensure_control_table(cursor, control_table)
        cursor.execute(
            f"SELECT last_data FROM {control_table} WHERE lower(client_name) = %s AND table_name = %s AND upper(operacao) = %s LIMIT 1",
            (client_key, nome_tabela, operacao_key),
        )
        row = cursor.fetchone()
        if row and row[0] is not None:
            return row[0].date() if hasattr(row[0], 'date') else row[0]
    except Exception:
        pass

    # 2 — fallback: MAX(data) in data table is the real source of truth
    try:
        data_table = _qualify(nome_tabela)
        cursor.execute(
            f"SELECT MAX(data) FROM {data_table} WHERE upper(operacao) = %s",
            (operacao_key,),
        )
        r = cursor.fetchone()
        if r and r[0] is not None:
            return r[0]
    except Exception:
        pass

    return None


def update_last_date(cursor, nome_tabela_ultimo_id: Optional[str], client_name: str, nome_tabela: str, operacao: str, ultima_data: date):
    """Upsert last_data in control table. Keys are always lowercase client_name / UPPERCASE operacao."""
    client_key = (client_name or '').strip().lower()
    operacao_key = (operacao or '').strip().upper()
    control_name = nome_tabela_ultimo_id or os.environ.get('TB_CONTROLER_CONSULTA_API', 'tb_controler_consulta_api')
    control_table = _qualify(control_name)
    _ensure_control_table(cursor, control_table)
    try:
        cursor.execute(
            f"INSERT INTO {control_table} (client_name, table_name, operacao, last_data) VALUES (%s,%s,%s,%s) "
            f"ON CONFLICT (client_name, table_name, operacao) DO UPDATE SET last_data = EXCLUDED.last_data, updated_at = now()",
            (client_key, nome_tabela, operacao_key, ultima_data),
        )
    except Exception as e:
        print(f"[WARN] Falha ao atualizar last_data para {client_name}/{nome_tabela}/{operacao}: {e}")




def create_table_from_sample(cursor, nome_tabela: str, dados_exemplo: dict, unique_cols=None):
    qualified = _qualify(nome_tabela)
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {qualified} (id bigint PRIMARY KEY, operacao text, payload jsonb)")