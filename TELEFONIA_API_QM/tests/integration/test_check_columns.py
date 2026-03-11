import os

import pytest

from telefonia_api.config import load_config
from telefonia_api.postgres_utils import get_pg_conn


def _require_integration_opt_in():
    if os.environ.get("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("Teste de integracao desabilitado. Defina RUN_INTEGRATION_TESTS=1 para executar.")


def main():
    _require_integration_opt_in()
    load_config()

    conn = get_pg_conn()
    cur = conn.cursor()
    schema = os.environ.get("PG_SCHEMA", "public")

    print(f"PG_USER: {os.environ.get('PG_USER', '(nao definido)')}")
    print(f"PG_HOST: {os.environ.get('PG_HOST', '(nao definido)')}")
    print(f"PG_DB: {os.environ.get('PG_DB') or os.environ.get('PG_DATABASE', '(nao definido)')}")
    print(f"PG_SCHEMA: {schema}")

    cur.execute(
        """
        SELECT table_schema, column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'tb_controler_consulta_api'
        ORDER BY table_schema, ordinal_position
        """
    )
    rows = cur.fetchall()

    if rows:
        print("Colunas da tabela tb_controler_consulta_api:")
        for row in rows:
            print(f"  schema={row[0]:15s} {row[1]:30s} {row[2]}")
    else:
        print("Tabela tb_controler_consulta_api NAO EXISTE no banco.")

    print(f"\nTentando ADD COLUMN last_date no schema '{schema}'...")
    cur.execute(f"ALTER TABLE {schema}.tb_controler_consulta_api ADD COLUMN IF NOT EXISTS last_date date")

    cur.execute(
        """
        SELECT table_schema, column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'tb_controler_consulta_api'
        ORDER BY table_schema, ordinal_position
        """
    )
    rows = cur.fetchall()
    assert rows, "Tabela tb_controler_consulta_api nao foi encontrada apos validacao."

    conn.close()


if __name__ == "__main__":
    main()


def test_check_columns_integration():
    main()
