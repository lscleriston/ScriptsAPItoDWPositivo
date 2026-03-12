from typing import Tuple, List
import json

_IMPORT_ERROR = None
try:
    from postgres_utils import get_pg_conn, get_last_id_and_range, update_last_id
    from db_table_utils import ensure_table_and_columns, insert_records as ps_insert_records
except Exception as e:
    # don't raise at import time; surface error when handler actually runs
    get_pg_conn = None
    get_last_id_and_range = None
    update_last_id = None
    ensure_table_and_columns = None
    ps_insert_records = None
    _IMPORT_ERROR = e


def _obter_ultimo_id_banco(client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    conn = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        # read last id and range for this client+endpoint from control table
        ultimo_id_tb, range_id = get_last_id_and_range(cursor, client_name, nome_tabela, operacao, nome_tabela_ultimo_id)
        cursor.close()
        conn.close()
        return ultimo_id_tb, range_id
    except Exception as err:
        print(f"[ERRO] Postgres ao obter ultimo id: {err}")
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass
        return None, 0


def _atualizar_tb_ultimo_id(client_name, nome_tabela_ultimo_id, nome_tabela, operacao, ultimo_id, pagina_ultimo_id):
    conn = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        update_last_id(cursor, nome_tabela_ultimo_id, client_name, nome_tabela, operacao, ultimo_id, pagina_ultimo_id)
        conn.commit()
        cursor.close()
        conn.close()
        print("[INFO] Atualizacao da tb_ultimo_id realizada.")
    except Exception as err:
        print(f"[ERRO] Postgres ao atualizar tb_ultimo_id: {err}")
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass
        raise


def _processar_hosts(client, client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    """Processa o endpoint host usando o client Zabbix."""
    ultimo_id, range_id = _obter_ultimo_id_banco(client_name, nome_tabela, operacao, nome_tabela_ultimo_id)

    try:
        # Obtém todos os hosts
        hosts_data = client.get_hosts()

        if not hosts_data:
            print('[INFO] Nenhum host encontrado.')
            return 0, []

        # Processa os dados dos hosts
        hosts_processados = []
        for host in hosts_data:
            # Expande grupos em registros separados
            groups = host.get('groups', [])
            if not groups:
                # Host sem grupos
                host_record = {
                    'hostid': host.get('hostid'),
                    'host': host.get('host'),
                    'name': host.get('name'),
                    'status': host.get('status'),
                    'ip': _extract_ip_from_interfaces(host.get('interfaces', [])),
                    'groupid': None,
                    'group_name': None,
                    'interfaces': json.dumps(host.get('interfaces', [])),
                    'inventory': json.dumps(host.get('inventory', {})),
                }
                hosts_processados.append(host_record)
            else:
                # Um registro por grupo
                for group in groups:
                    host_record = {
                        'hostid': host.get('hostid'),
                        'host': host.get('host'),
                        'name': host.get('name'),
                        'status': host.get('status'),
                        'ip': _extract_ip_from_interfaces(host.get('interfaces', [])),
                        'groupid': group.get('groupid'),
                        'group_name': group.get('name'),
                        'interfaces': json.dumps(host.get('interfaces', [])),
                        'inventory': json.dumps(host.get('inventory', {})),
                    }
                    hosts_processados.append(host_record)

        # Salva no banco
        inseridos, ids_inseridos = _salvar_hosts_no_banco(hosts_processados, nome_tabela, operacao)

        if inseridos and hosts_processados:
            # Usa o maior hostid como referência
            ultimo_hostid = max([int(h.get('hostid', 0)) for h in hosts_processados if h.get('hostid')])
            _atualizar_tb_ultimo_id(client_name, nome_tabela_ultimo_id, nome_tabela, operacao, ultimo_hostid, 0)

        return inseridos, ids_inseridos

    except Exception as e:
        print(f"[ERROR] Falha ao processar hosts: {e}")
        return 0, []


def _extract_ip_from_interfaces(interfaces):
    """Extrai o primeiro IP válido das interfaces"""
    if not interfaces:
        return None

    for interface in interfaces:
        ip = interface.get('ip')
        if ip and ip != '0.0.0.0':
            return ip

    return None


def _salvar_hosts_no_banco(hosts, nome_tabela, operacao):
    """Salva hosts no banco de dados PostgreSQL"""
    conn = None
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()

        # Garante que a tabela existe
        ensure_table_and_columns(conn, nome_tabela, hosts[:1], operacao)

        # Remove hosts antigos desta operação
        delete_query = f"DELETE FROM {nome_tabela} WHERE operacao = %s"
        cursor.execute(delete_query, (operacao,))

        # Insere hosts
        inseridos, ids_inseridos = ps_insert_records(conn, nome_tabela, hosts, operacao)

        conn.commit()
        cursor.close()

        return inseridos, ids_inseridos

    except Exception as err:
        print(f"[ERRO] Falha ao salvar hosts: {err}")
        if conn:
            conn.rollback()
        return 0, []
    finally:
        if conn:
            conn.close()


# generic entrypoint required by runner
process = _processar_hosts