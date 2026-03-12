import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import Tuple, Optional
import time


def get_pg_conn():
    """Cria conexão com PostgreSQL usando variáveis de ambiente"""
    try:
        conn = psycopg2.connect(
            host=os.environ.get('PG_HOST', '127.0.0.1'),
            port=os.environ.get('PG_PORT', 5432),
            database=os.environ.get('PG_DB', 'dw_positivo'),
            user=os.environ.get('PG_USER', 'db_user'),
            password=os.environ.get('PG_PASSWORD', ''),
            options=f"-c search_path={os.environ.get('PG_SCHEMA', 'dw_positivo')}"
        )
        return conn
    except Exception as e:
        print(f"[ERROR] Falha ao conectar ao PostgreSQL: {e}")
        return None


def get_last_id_and_range(cursor, client_name: str, table_name: str, operation: str, control_table: str) -> Tuple[Optional[int], int]:
    """Obtém o último ID processado e range para um cliente/endpoint específico"""
    try:
        query = sql.SQL("""
            SELECT last_id, page_last
            FROM {} WHERE client_name = %s AND table_name = %s AND operacao = %s
            ORDER BY updated_at DESC LIMIT 1
        """).format(sql.Identifier(control_table.replace(f"{os.environ.get('PG_SCHEMA', 'dw_positivo')}.", "")))

        cursor.execute(query, (client_name, table_name, operation))
        result = cursor.fetchone()

        if result:
            return result[0], result[1] or 0
        else:
            return None, 0

    except Exception as e:
        print(f"[ERROR] Falha ao obter último ID: {e}")
        return None, 0


def update_last_id(cursor, control_table: str, client_name: str, table_name: str, operation: str, last_id: int, range_start: int = 0):
    """Atualiza o último ID processado na tabela de controle"""
    try:
        # Evita depender de DELETE: atualiza a linha existente, e só insere se não existir.
        update_query = sql.SQL("""
            UPDATE {}
               SET last_id = %s,
                   page_last = %s,
                   updated_at = CURRENT_TIMESTAMP
             WHERE client_name = %s AND table_name = %s AND operacao = %s
        """).format(sql.Identifier(control_table.replace(f"{os.environ.get('PG_SCHEMA', 'dw_positivo')}.", "")))

        cursor.execute(update_query, (last_id, range_start, client_name, table_name, operation))

        if cursor.rowcount > 0:
            return

        # Sem registro prévio: insere nova entrada
        insert_query = sql.SQL("""
            INSERT INTO {} (client_name, table_name, operacao, last_id, page_last, updated_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """).format(sql.Identifier(control_table.replace(f"{os.environ.get('PG_SCHEMA', 'dw_positivo')}.", "")))

        cursor.execute(insert_query, (client_name, table_name, operation, last_id, range_start))

    except Exception as e:
        print(f"[ERROR] Falha ao atualizar último ID: {e}")
        raise