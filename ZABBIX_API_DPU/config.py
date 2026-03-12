try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None
import os
from typing import Dict, List, Any

_ROOT = os.path.dirname(os.path.abspath(__file__))
_DOTENV_PATH = os.path.join(_ROOT, '.env')
if load_dotenv:
    try:
        load_dotenv(_DOTENV_PATH)
    except Exception:
        pass

# Fallback: se dotenv não carregou variáveis ZABBIX, tente parsear .env manualmente
def _ensure_env_loaded_from_file(dotenv_path=None):
    env_keys = [k for k in os.environ.keys() if k.upper().startswith('ZABBIX')]
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
        return ["host", "event"]
    return [e.strip() for e in raw.split(",") if e.strip()]


def load_config() -> Dict[str, Any]:
    """Carrega configuração do ambiente e detecta múltiplos clientes Zabbix.

    Padrões suportados:
    - Variáveis genéricas: `ZABBIX_BASE_URL`, `ZABBIX_API_TOKEN`, `ZABBIX_ENDPOINTS`
    - Variáveis por cliente: `ZABBIX_URL_<SUFFIX>`, `ZABBIX_API_TOKEN_<SUFFIX>`, opcional `ZABBIX_ENDPOINTS_<SUFFIX>`

    Retorna dicionário com chaves:
    - `clients`: lista de clientes, cada um com `name`, `base_url`, `api_token`, `endpoints`
    - `pg`: dicionário com configurações Postgres (se presentes)
    """
    env = os.environ

    clients: List[Dict[str, Any]] = []

    # Detecta clientes por sufixo: ZABBIX_URL_<SUFFIX>
    for key in list(env.keys()):
        if key.upper().startswith("ZABBIX_URL_"):
            suffix = key.split("ZABBIX_URL_", 1)[1].strip()
            if not suffix:
                continue
            base_url = env.get(key)
            api_token = env.get(f"ZABBIX_API_TOKEN_{suffix}") or env.get(f"ZABBIX_API_TOKEN_{suffix.lower()}")
            endpoints_raw = env.get(f"ZABBIX_ENDPOINTS_{suffix}") or env.get("ZABBIX_ENDPOINTS")
            endpoints = _parse_endpoints(endpoints_raw)
            name = suffix
            if base_url and api_token:
                clients.append({
                    "name": name,
                    "base_url": base_url.rstrip('/'),
                    "api_token": api_token,
                    "endpoints": endpoints,
                })

    # Fallback para configuração única genérica
    if not clients:
        base_url = env.get("ZABBIX_BASE_URL")
        api_token = env.get("ZABBIX_API_TOKEN")
        endpoints_raw = env.get("ZABBIX_ENDPOINTS", "host,event")
        if base_url and api_token:
            clients.append({
                "name": "default",
                "base_url": base_url.rstrip('/'),
                "api_token": api_token,
                "endpoints": _parse_endpoints(endpoints_raw),
            })

    # Postgres settings (pass through)
    pg = {
        "host": env.get("PG_HOST"),
        "port": env.get("PG_PORT"),
        "database": env.get("PG_DB") or env.get("PG_DATABASE"),
        "user": env.get("PG_USER"),
        "password": env.get("PG_PASSWORD"),
        "schema": env.get("PG_SCHEMA"),
    }

    return {"clients": clients, "pg": pg}