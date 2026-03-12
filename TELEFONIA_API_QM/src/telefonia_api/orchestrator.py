"""Orquestrador principal para executar handlers em `endpoints/` por cliente definido em .env"""
from .config import load_config
import importlib


def main():
    cfg = load_config()
    clients = cfg.get("clients", {})
    if not clients:
        print("[ERROR] Nenhum cliente encontrado na configuração.")
        return

    for client_name, client_cfg in clients.items():
        print(f"[INFO] Iniciando coleta para cliente: {client_name}")
        
        client_config = {
            'url': client_cfg['url'],
            'auth_user': client_cfg['auth_user'],
            'auth_pass': client_cfg['auth_pass'],
            'queues': client_cfg.get('queues', 'filadpu'),
            'start_date': client_cfg.get('start_date', '2024-01-01'),
        }

        for endpoint in client_cfg.get("endpoints", []):
            module_name = f"telefonia_api.endpoints.{endpoint}"
            try:
                mod = importlib.import_module(module_name)
            except Exception as e:
                print(f"[WARN] Módulo para endpoint '{endpoint}' não encontrado: {e}")
                continue

            if not hasattr(mod, "process"):
                print(f"[WARN] Módulo '{module_name}' não exporta função 'process'. Pulando.")
                continue

            try:
                print(f"[INFO] Executando {endpoint} para {client_name}")
                mod.process(client_config, client_name)
                print(f"[INFO] Sincronização concluída para {endpoint}")
                
            except Exception as e:
                print(f"[ERROR] Falha ao processar {endpoint} para {client_name}: {e}")
                import traceback
                traceback.print_exc()


if __name__ == "__main__":
    main()