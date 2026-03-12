"""Orquestrador principal para executar handlers em `endpoints/` por cliente definido em .env

Cria um `ZabbixClient` por cliente detectado em `config.load_config()` e executa
os módulos `endpoints.<resource>` conforme listado em cada cliente.
"""
from config import load_config
from zabbix_client import ZabbixClient
import importlib


def main():
    cfg = load_config()
    clients = cfg.get("clients", [])
    if not clients:
        print("[ERROR] Nenhum cliente Zabbix encontrado na configuração.")
        return

    nome_tabela_base = "tb_zabbix_host"
    # Use new control table name as requested
    schema = cfg.get('pg', {}).get('schema') or 'dw_positivo'
    nome_tabela_ultimo_id = f"{schema}.tb_controler_consulta_api"

    for client_cfg in clients:
        name = client_cfg.get("name") or "default"
        print(f"[INFO] Iniciando coleta para cliente: {name}")
        client = ZabbixClient(client_cfg["base_url"], client_cfg["api_token"])
        try:
            # Inicia processamento sem teste prévio de conexão
            print(f"[INFO] Iniciando processamento para {name}")
        except Exception as e:
            print(f"[ERROR] Falha ao iniciar processamento para {name}: {e}")
            continue

        for endpoint in client_cfg.get("endpoints", []):
            # try several module name variants: singular/plural/lower
            candidates = [endpoint.lower(), endpoint.lower() + 's', endpoint.lower().rstrip('s')]
            mod = None
            for cand in candidates:
                module_name = f"endpoints.{cand}"
                try:
                    mod = importlib.import_module(module_name)
                    break
                except Exception:
                    continue
            if not mod:
                print(f"[WARN] Módulo para endpoint '{endpoint}' não encontrado entre: {candidates}")
                continue

            if not hasattr(mod, "process"):
                print(f"[WARN] Módulo '{module_name}' não exporta função 'process'. Pulando.")
                continue

            import os
            schema = cfg.get('pg', {}).get('schema') or 'dw_positivo'
            # separate table per endpoint
            nome_tabela = f"{schema}.tb_zabbix_{endpoint.lower()}"
            operacao = name  # use client name as operation label
            print(f"[INFO] Executando handler '{endpoint}' para cliente '{name}' (tabela: {nome_tabela}, operacao: {operacao})")
            try:
                inseridos, ids_inseridos = mod.process(client, name, nome_tabela, operacao, nome_tabela_ultimo_id)
                print(f"[INFO] Resultado {name}:{endpoint}: inseridos={inseridos}")
            except Exception as err:
                print(f"[ERROR] Falha ao processar {name}:{endpoint}: {err}")


if __name__ == "__main__":
    main()