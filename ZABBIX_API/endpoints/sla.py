"""Handler responsável por coletar SLAs e tags associadas via `sla.get`."""
from typing import List, Dict, Any, Optional
import time
import os
from psycopg2 import sql

_IMPORT_ERROR = None
try:
    from postgres_utils import get_pg_conn, update_last_id
    from db_table_utils import ensure_table_and_columns, insert_records as ps_insert_records
except Exception as exc:  # pragma: no cover - carregamento controlado
    get_pg_conn = None
    update_last_id = None
    ensure_table_and_columns = None
    ps_insert_records = None
    _IMPORT_ERROR = exc


_OPERATOR_LABELS = {
    0: "equals",
    2: "contains",
}


def _normalize_sla_records(slas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Expande cada SLA em múltiplos registros (um por tag)."""
    normalized: List[Dict[str, Any]] = []
    for sla in slas:
        tags = sla.get('service_tags') or [None]
        for tag in tags:
            operator_value: Optional[int] = None
            operator_label: Optional[str] = None
            if tag and tag.get('operator') is not None:
                try:
                    operator_value = int(tag.get('operator'))
                except (TypeError, ValueError):
                    operator_value = None
                if operator_value is not None:
                    operator_label = _OPERATOR_LABELS.get(operator_value, 'custom')

            normalized.append({
                'slaid': sla.get('slaid'),
                'sla_name': sla.get('name') or sla.get('SLA'),
                'slo': sla.get('slo'),
                'status': sla.get('status'),
                'period': sla.get('period'),
                'effective_date': sla.get('effective_date'),
                'timezone': sla.get('timezone'),
                'description': sla.get('description'),
                'tag': tag.get('tag') if tag else None,
                'tag_operator': operator_value,
                'tag_operator_label': operator_label,
                'tag_value': tag.get('value') if tag else None,
            })
    return normalized


def _delete_previous_rows(conn, nome_tabela: str, operacao: str) -> None:
    schema = nome_tabela.split('.')[0] if '.' in nome_tabela else os.environ.get('PG_SCHEMA', 'dw_positivo')
    table = nome_tabela.split('.')[-1]
    cursor = conn.cursor()
    try:
        delete_query = sql.SQL("DELETE FROM {}.{} WHERE operacao = %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        cursor.execute(delete_query, (operacao,))
        conn.commit()
    finally:
        cursor.close()


def _process_slas(client, client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    if _IMPORT_ERROR:
        raise RuntimeError(f"Falha ao importar dependências para SLA endpoint: {_IMPORT_ERROR}")

    slas = client.get_slas(include_service_tags=True)
    if not slas:
        print('[INFO] Nenhum SLA retornado pela API.')
        return 0, []

    records = _normalize_sla_records(slas)
    if not records:
        print('[INFO] SLAs sem tags foram retornados, nada para armazenar.')
        return 0, []

    conn = get_pg_conn()
    if not conn:
        raise RuntimeError('Não foi possível conectar ao PostgreSQL para salvar SLAs.')

    try:
        ensure_table_and_columns(conn, nome_tabela, records[:1], operacao)
        _delete_previous_rows(conn, nome_tabela, operacao)
        inseridos, ids = ps_insert_records(conn, nome_tabela, records, operacao)

        cursor = conn.cursor()
        try:
            update_last_id(cursor, nome_tabela_ultimo_id, client_name, nome_tabela, operacao, int(time.time()), 0)
            conn.commit()
        finally:
            cursor.close()

        return inseridos, ids
    finally:
        try:
            conn.close()
        except Exception:
            pass


process = _process_slas
