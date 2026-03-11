"""Cria a tabela de telefonia."""
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
    table = f"{schema}.tb_telefonia_atendidas"

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id bigserial PRIMARY KEY,
            operacao text,
            data date,
            payload jsonb
        )
    """)

    conn.commit()
    print(f"[INFO] Tabela {table} criada/verificada.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()