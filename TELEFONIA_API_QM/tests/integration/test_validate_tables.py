import os
import json
import psycopg2
from telefonia_api import postgres_utils
import pytest


def _require_integration_opt_in():
    if os.environ.get("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("Teste de integracao desabilitado. Defina RUN_INTEGRATION_TESTS=1 para executar.")


def q(cur, sql, params=None):
    cur.execute(sql, params if params else ())
    return cur.fetchone()


def main():
    _require_integration_opt_in()
    conn = postgres_utils.get_pg_conn()
    cur = conn.cursor()
    schema = os.environ.get('PG_SCHEMA') or 'public'
    parent = postgres_utils._qualify('tb_telefonia_atendidas')
    detail = postgres_utils._qualify('tb_telefonia_atendidas__detailsdo_callsok_details')

    # parent counts
    pcount = q(cur, f"SELECT COUNT(*) FROM {parent}")[0]
    p_nonnull = q(cur, f"SELECT COUNT(*) FROM {parent} WHERE detailsdo_callsok IS NOT NULL")[0]

    # columns starting with detailsdo_callsok_c_
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (schema, 'tb_telefonia_atendidas'))
    cols = [r[0] for r in cur.fetchall()]
    per_day_cols = [c for c in cols if c.startswith('detailsdo_callsok_c_')]

    # detail counts
    try:
        dcount = q(cur, f"SELECT COUNT(*) FROM {detail}")[0]
    except Exception as e:
        dcount = None

    print('parent_count', pcount)
    print('parent_detailsdo_callsok_nonnull', p_nonnull)
    print('per_day_columns_count', len(per_day_cols))
    if per_day_cols:
        print('per_day_columns_sample', per_day_cols[:20])

    print('detail_table', detail)
    print('detail_total_count', dcount)

    # fetch 21st parent row (ordered by id)
    cur.execute(f"SELECT id, payload::text, detailsdo_callsok::text FROM {parent} ORDER BY id OFFSET 20 LIMIT 1")
    row = cur.fetchone()
    if not row:
        print('parent_row_21', None)
        return
    parent_id = row[0]
    print('parent_row_21_id', parent_id)
    print('parent_row_21_payload_preview', (row[1][:1000] + '...') if row[1] and len(row[1])>1000 else row[1])
    print('parent_row_21_detailsdo_callsok_preview', (row[2][:1000] + '...') if row[2] and len(row[2])>1000 else row[2])

    # detail rows for that parent
    if dcount is not None:
        cur.execute(f"SELECT COUNT(*) FROM {detail} WHERE parent_id = %s", (parent_id,))
        cnt = cur.fetchone()[0]
        print('detail_count_for_parent', cnt)
        cur.execute(f"SELECT id, parent_id, raw_row::text FROM {detail} WHERE parent_id = %s ORDER BY id LIMIT 10", (parent_id,))
        samples = cur.fetchall()
        print('detail_samples_count', len(samples))
        for s in samples:
            print('-', s[0], (s[2][:400] + '...') if s[2] and len(s[2])>400 else s[2])


if __name__ == '__main__':
    main()


def test_validate_tables_integration():
    main()
