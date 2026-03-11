try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None
import os
from typing import Dict, List, Any

_ROOT = os.path.dirname(os.path.abspath(__file__))
# O .env pode estar no pacote, em src/ ou na raiz do projeto.
_DOTENV_CANDIDATES = [
    os.path.join(_ROOT, '.env'),
    os.path.join(os.path.dirname(_ROOT), '.env'),
    os.path.join(os.path.dirname(os.path.dirname(_ROOT)), '.env'),
]
_DOTENV_PATH = next((p for p in _DOTENV_CANDIDATES if os.path.exists(p)), _DOTENV_CANDIDATES[0])
if load_dotenv:
    for _dotenv_path in _DOTENV_CANDIDATES:
        try:
            load_dotenv(_dotenv_path)
        except Exception:
            continue

# Fallback: se dotenv não carregou variáveis, tente parsear .env manualmente
def _ensure_env_loaded_from_file(dotenv_path=None):
    env_keys = [k for k in os.environ.keys() if k.upper().startswith(('QUEUEMETRICS', 'PG_'))]
    if env_keys:
        return
    if not dotenv_path:
        dotenv_path = _DOTENV_PATH
    try:
        with open(dotenv_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                k, v = line.split('=', 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                # don't overwrite already existing env vars
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        return

_ensure_env_loaded_from_file()

def _parse_endpoints(raw: str) -> List[str]:
    if not raw:
        return ["chamadas_atendidas"]
    return [e.strip() for e in raw.split(",") if e.strip()]

def load_config() -> Dict[str, Any]:
    config = {
        'clients': {},
        'postgres': {}
    }

    # Variáveis globais compartilhadas (URL, auth, endpoints)
    global_url = os.getenv('QUEUEMETRICS_URL', '')
    global_auth_user = os.getenv('QUEUEMETRICS_AUTH_USER', '')
    global_auth_pass = os.getenv('QUEUEMETRICS_AUTH_PASS', '')
    global_endpoints = _parse_endpoints(os.getenv('QUEUEMETRICS_ENDPOINTS', ''))

    # Detectar clientes a partir das variáveis QUEUEMETRICS_QUEUES_<CLIENTE>
    for key, value in os.environ.items():
        if key.startswith('QUEUEMETRICS_QUEUES_'):
            client_name = key.replace('QUEUEMETRICS_QUEUES_', '').lower()
            # Usa endpoints específicos do cliente se existir, senão usa o global
            client_endpoints_raw = os.getenv(f'QUEUEMETRICS_ENDPOINTS_{client_name.upper()}')
            client_endpoints = _parse_endpoints(client_endpoints_raw) if client_endpoints_raw else global_endpoints
            # Obter data de início (padrão: 2024-01-01)
            start_date = os.getenv(f'{client_name.upper()}_START_DATE', '2024-01-01')
            config['clients'][client_name] = {
                'url': os.getenv(f'QUEUEMETRICS_URL_{client_name.upper()}', global_url),
                'auth_user': os.getenv(f'QUEUEMETRICS_AUTH_USER_{client_name.upper()}', global_auth_user),
                'auth_pass': os.getenv(f'QUEUEMETRICS_AUTH_PASS_{client_name.upper()}', global_auth_pass),
                'endpoints': client_endpoints,
                'queues': value,
                'start_date': start_date,
            }

    # Configurações PostgreSQL
    config['postgres'] = {
        'host': os.getenv('PG_HOST', '127.0.0.1'),
        'port': int(os.getenv('PG_PORT', '5432')),
        'database': os.getenv('PG_DB', 'dw_positivo'),
        'user': os.getenv('PG_USER', 'db_user'),
        'password': os.getenv('PG_PASSWORD', ''),
        'schema': os.getenv('PG_SCHEMA', 'dw_positivo')
    }

    return config

if __name__ == "__main__":
    config = load_config()
    print("Configuração carregada:")
    for client, data in config['clients'].items():
        print(f"Cliente: {client.upper()}")
        for k, v in data.items():
            print(f"  {k}: {v}")
    print("PostgreSQL:")
    for k, v in config['postgres'].items():
        print(f"  {k}: {v}")