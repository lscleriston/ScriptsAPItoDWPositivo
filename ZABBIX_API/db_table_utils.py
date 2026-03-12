import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Tuple
import time
import os


def ensure_table_and_columns(conn, table_name: str, sample_data: List[Dict[str, Any]], operation: str) -> str:
    """Cria/verifica tabela dinamicamente baseada nos dados de exemplo"""
    if not sample_data:
        return table_name

    cursor = conn.cursor()

    try:
        # Verifica se a tabela existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """, (table_name.split('.')[0] if '.' in table_name else 'dw_positivo', table_name.split('.')[-1]))

        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Cria tabela baseada no sample_data
            columns = _infer_columns_from_data(sample_data)

            # Adiciona colunas padrão
            columns.extend([
                ("operacao", "VARCHAR(100)"),
                ("data_atualizacao", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ])

            create_query = _build_create_table_query(table_name, columns)
            cursor.execute(create_query)
            conn.commit()
            print(f"[INFO] Tabela {table_name} criada com sucesso")

        # Verifica e adiciona colunas que podem estar faltando
        existing_columns = _get_existing_columns(cursor, table_name)
        required_columns = _infer_columns_from_data(sample_data)

        for col_name, col_type in required_columns:
            if col_name not in existing_columns:
                alter_query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    sql.Identifier(table_name.split('.')[-1]),
                    sql.Identifier(col_name),
                    sql.SQL(col_type)
                )
                cursor.execute(alter_query)
                print(f"[INFO] Coluna {col_name} adicionada à tabela {table_name}")

        conn.commit()

    except Exception as e:
        print(f"[ERROR] Falha ao criar/verificar tabela {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

    return table_name


def insert_records(conn, table_name: str, records: List[Dict[str, Any]], operation: str) -> Tuple[int, List[Any]]:
    """Insere registros na tabela, fazendo upsert quando necessário"""
    if not records:
        return 0, []

    cursor = conn.cursor()
    inserted_count = 0
    inserted_ids = []

    try:
        # Obtém as colunas da tabela
        columns = _get_existing_columns(cursor, table_name)
        if not columns:
            raise Exception(f"Tabela {table_name} não encontrada ou sem colunas")

        # Filtra apenas as colunas que existem na tabela
        filtered_records = []
        for record in records:
            filtered_record = {k: v for k, v in record.items() if k in columns}
            filtered_record['operacao'] = operation
            # Não definir data_atualizacao aqui - usa DEFAULT da tabela
            filtered_records.append(filtered_record)

        if not filtered_records:
            return 0, []

        # Prepara query de upsert
        column_names = list(filtered_records[0].keys())
        schema = table_name.split('.')[0] if '.' in table_name else os.environ.get('PG_SCHEMA', 'dw_positivo')
        table_short_name = table_name.split('.')[-1]
        qualified_table = f"{schema}.{table_short_name}"

        # Detecta chave primária ou campos únicos para upsert
        unique_columns = _detect_unique_columns(cursor, table_short_name, column_names)

        # Para Zabbix, sempre usar insert simples (as tabelas são limpas antes)
        # Exceto para events, que acumulam e podem ter conflitos
        cols = ', '.join(f'"{col}"' for col in column_names)
        if 'eventid' in column_names and 'tb_zabbix_event' in table_name:
            insert_query = f"INSERT INTO {qualified_table} ({cols}) VALUES %s ON CONFLICT (eventid) DO NOTHING"
        else:
            insert_query = f"INSERT INTO {qualified_table} ({cols}) VALUES %s"
        values = [tuple(record[col] for col in column_names) for record in filtered_records]

        execute_values(cursor, insert_query, values, page_size=1000)

        inserted_count = len(filtered_records)
        inserted_ids = [record.get('id') or record.get('hostid') or record.get('eventid') for record in filtered_records]

        conn.commit()

    except Exception as e:
        print(f"[ERROR] Falha ao inserir registros em {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

    return inserted_count, inserted_ids


def _infer_columns_from_data(sample_data: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """Infere tipos de colunas baseado nos dados de exemplo"""
    if not sample_data:
        return []

    # Coleta todos os campos possíveis
    all_fields = {}
    for record in sample_data:
        for key, value in record.items():
            if key not in all_fields:
                all_fields[key] = _infer_sql_type(value)
            else:
                # Atualiza tipo se necessário (prioriza tipos mais gerais)
                current_type = all_fields[key]
                new_type = _infer_sql_type(value)
                all_fields[key] = _merge_types(current_type, new_type)

    return [(key, sql_type) for key, sql_type in all_fields.items()]


def _infer_sql_type(value) -> str:
    """Infere tipo SQL baseado no valor Python"""
    if value is None:
        return "TEXT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "BIGINT"
    elif isinstance(value, float):
        return "DECIMAL(20,6)"
    elif isinstance(value, str):
        if len(value) > 1000:
            return "TEXT"
        elif len(value) > 255:
            return "VARCHAR(1000)"
        else:
            return "VARCHAR(255)"
    elif isinstance(value, list):
        return "JSONB"
    elif isinstance(value, dict):
        return "JSONB"
    else:
        return "TEXT"


def _merge_types(type1: str, type2: str) -> str:
    """Mescla dois tipos SQL, escolhendo o mais geral"""
    type_hierarchy = {
        'BOOLEAN': 0,
        'BIGINT': 1,
        'DECIMAL(20,6)': 2,
        'VARCHAR(255)': 3,
        'VARCHAR(1000)': 4,
        'TEXT': 5,
        'JSONB': 6
    }

    level1 = type_hierarchy.get(type1, 5)
    level2 = type_hierarchy.get(type2, 5)

    # Retorna o tipo de maior nível
    max_level = max(level1, level2)
    for sql_type, level in type_hierarchy.items():
        if level == max_level:
            return sql_type

    return "TEXT"


def _get_existing_columns(cursor, table_name: str) -> List[str]:
    """Obtém lista de colunas existentes na tabela"""
    try:
        schema = table_name.split('.')[0] if '.' in table_name else 'dw_positivo'
        table = table_name.split('.')[-1]

        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))

        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []


def _detect_unique_columns(cursor, table_name: str, column_names: List[str]) -> List[str]:
    """Detecta colunas que podem ser usadas como chave única para upsert"""
    try:
        # Verifica índices únicos ou chaves primárias
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisunique = true
        """, (f"dw_positivo.{table_name}",))

        unique_cols = [row[0] for row in cursor.fetchall()]

        # Se não encontrou índices únicos, tenta detectar campos comuns que são únicos
        if not unique_cols:
            # Para Zabbix, campos comuns únicos
            zabbix_unique_fields = ['hostid', 'eventid', 'triggerid', 'itemid', 'groupid']
            unique_cols = [col for col in column_names if col in zabbix_unique_fields]

            # Se encontrou campos únicos do Zabbix, cria um índice composto com operacao
            if unique_cols:
                unique_cols.append('operacao')

        return unique_cols

    except Exception:
        # Fallback: usa campos comuns
        zabbix_unique_fields = ['hostid', 'eventid', 'triggerid', 'itemid', 'groupid']
        unique_cols = [col for col in column_names if col in zabbix_unique_fields]
        if unique_cols:
            unique_cols.append('operacao')
        return unique_cols


def _build_create_table_query(table_name: str, columns: List[Tuple[str, str]]) -> str:
    """Constrói query SQL para criar tabela"""
    table_short_name = table_name.split('.')[-1]

    column_defs = []
    for col_name, col_type in columns:
        if col_name == 'id':
            column_defs.append(f'"{col_name}" {col_type} PRIMARY KEY')
        else:
            column_defs.append(f'"{col_name}" {col_type}')

    return f"""
    CREATE TABLE IF NOT EXISTS {table_short_name} (
        {', '.join(column_defs)}
    )
    """


def recreate_events_table_with_schema(conn, table_name: str):
    """Descarta (se existir) e cria a tabela de events com o schema esperado pelo script antigo."""
    cursor = conn.cursor()
    try:
        schema = table_name.split('.')[0] if '.' in table_name else 'dw_positivo'
        table = table_name.split('.')[-1]

        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

        # Create with exact schema matching legacy script
        create_sql = f"""
        CREATE TABLE {schema}.{table} (
            eventid VARCHAR(20) PRIMARY KEY,
            trigger_id VARCHAR(20),
            host_id VARCHAR(20),
            host_name VARCHAR(100),
            event_name VARCHAR(255),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds NUMERIC(12,4),
            duration_human VARCHAR(50),
            seconds_in_period NUMERIC(14,4),
                sla_percentage NUMERIC(10,6),
            period_type VARCHAR(50),
            severity VARCHAR(8),
            problem_eventid VARCHAR(20),
            resolution_eventid VARCHAR(20),
            status VARCHAR(20),
            operacao VARCHAR(50),
            UNIQUE (eventid, operacao)
        )
        """

        cursor.execute(create_sql)

        # Create some useful indexes
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_trigger_id ON {schema}.{table}(trigger_id)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_host_id ON {schema}.{table}(host_id)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_start_time ON {schema}.{table}(start_time)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_status ON {schema}.{table}(status)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_severity ON {schema}.{table}(severity)")

        conn.commit()
        print(f"[INFO] Tabela {schema}.{table} recriada com schema de events legados")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Falha ao recriar tabela de events {table_name}: {e}")
        raise
    finally:
        cursor.close()