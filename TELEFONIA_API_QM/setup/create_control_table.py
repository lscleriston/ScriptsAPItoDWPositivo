"""Cria a tabela de controle para sincronização incremental."""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def main():
    conn = psycopg2.connect(
        host=os.environ.get('PG_HOST'),
        port=os.environ.get('PG_PORT', 5432),
        dbname=os.environ.get('PG_DB'),
        user=os.environ.get('PG_USER'),
        password=os.environ.get('PG_PASSWORD')
    )
    cursor = conn.cursor()

    schema = os.environ.get('PG_SCHEMA', 'public')
    table = f"{schema}.tb_controler_consulta_api"

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            client_name text,
            table_name text,
            operacao text,
            last_date date,
            metadata jsonb,
            updated_at timestamptz default now(),
            PRIMARY KEY (client_name, table_name, operacao)
        )
    """)

    conn.commit()
    print(f"[INFO] Tabela {table} criada/verificada.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()