"""
Teste de diagnóstico da API QueueMetrics
- Testa todos os clientes configurados no .env
- Detecta erro de permissão ROBOT explicitamente
- Testa período dos últimos 7 dias para aumentar chance de encontrar dados
- Mostra JSON bruto quando não há chamadas, para diagnóstico
"""
import os
import re
import json
import requests
from datetime import datetime, timedelta, date
from requests.auth import HTTPBasicAuth
import urllib3
import pytest

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from telefonia_api.config import load_config


def _require_integration_opt_in():
    if os.environ.get("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("Teste de integracao desabilitado. Defina RUN_INTEGRATION_TESTS=1 para executar.")


def _call_api_raw(url, user, password, queue, from_p, to_p, block):
    """Faz a requisição e retorna (status_http, json_data, erro_str)."""
    params = {
        "method": "Stats.get",
        "queues": queue,
        "from": from_p,
        "to": to_p,
        "block": block,
    }
    try:
        r = requests.get(
            url, params=params,
            auth=HTTPBasicAuth(user, password),
            headers={"Content-Type": "application/json"},
            verify=False, timeout=30,
        )
        return r.status_code, r.json(), None
    except Exception as e:
        return None, None, str(e)


def _parse_result(data):
    """Normaliza resposta no formato {'result': [['Status','OK'], ...]}."""
    if not isinstance(data, dict):
        return data, None
    if not isinstance(data.get("result"), list):
        return data, None
    normalized = {item[0]: item[1] for item in data["result"] if isinstance(item, (list, tuple)) and len(item) >= 2}
    status = str(normalized.get("Status", "")).upper()
    if status == "ERR":
        return None, normalized.get("Description", "Erro desconhecido")
    return normalized, None


def _count_rows(normalized, block_key):
    """Conta linhas de dados na chave block_key (desconta o cabeçalho)."""
    section = normalized.get(block_key, []) if normalized else []
    if not section:
        return 0
    data_rows = [r for r in section if isinstance(r, list) and r and not all(isinstance(h, str) for h in r)]
    return len(data_rows)


def main():
    _require_integration_opt_in()
    cfg = load_config()
    clients = cfg.get("clients", {})

    if not clients:
        print("[ERRO] Nenhum cliente encontrado no .env")
        return

    # Últimos 7 dias para aumentar chance de encontrar dados
    hoje = datetime.today().date()
    data_fim = hoje - timedelta(days=1)
    data_ini = hoje - timedelta(days=7)
    from_p = data_ini.strftime("%Y-%m-%d.00:00:00")
    to_p   = data_fim.strftime("%Y-%m-%d.23:59:59")

    print("=" * 72)
    print(f"  TESTE API QueueMetrics — Período: {data_ini} → {data_fim}")
    print("=" * 72)

    for client_name, client_cfg in clients.items():
        url      = client_cfg["url"]
        user     = client_cfg["auth_user"]
        password = client_cfg["auth_pass"]
        queues   = [q.strip() for q in client_cfg.get("queues", "").split(",") if q.strip()]

        print(f"\n{'─'*72}")
        print(f"  CLIENTE: {client_name.upper()}")
        print(f"  URL    : {url}")
        print(f"  Filas  : {queues}")
        print(f"{'─'*72}")

        for queue in queues:
            print(f"\n  Fila: {queue}")
            for block, label in [("DetailsDO.CallsOK", "ATENDIDAS"), ("DetailsDO.CallsKO", "PERDIDAS")]:
                status_http, raw_json, conn_err = _call_api_raw(url, user, password, queue, from_p, to_p, block)

                if conn_err:
                    print(f"    [{label}] ✗ Erro de conexão: {conn_err}")
                    continue

                normalized, api_err = _parse_result(raw_json)

                if api_err:
                    # Detecta erro ROBOT explicitamente
                    if "ROBOT" in str(api_err):
                        print(f"    [{label}] ✗ PERMISSÃO NEGADA: usuário não tem chave ROBOT no QueueMetrics")
                    else:
                        print(f"    [{label}] ✗ Erro API: {str(api_err)[:200]}")
                    # Mostra JSON bruto truncado para diagnóstico
                    print(f"             JSON bruto: {json.dumps(raw_json, ensure_ascii=False)[:300]}")
                    continue

                rows = _count_rows(normalized, block)
                if rows > 0:
                    print(f"    [{label}] ✓ {rows} chamada(s) encontrada(s)")
                    # Mostra primeiras chaves disponíveis
                    chaves = [k for k in normalized.keys() if k not in ("Status", "Description")]
                    print(f"             Chaves na resposta: {chaves[:10]}")
                else:
                    print(f"    [{label}] — Nenhuma chamada no período")
                    # Mostra chaves disponíveis para diagnóstico
                    if normalized:
                        chaves = [k for k in normalized.keys() if k not in ("Status", "Description")]
                        if chaves:
                            print(f"             Chaves disponíveis na resposta: {chaves[:10]}")


if __name__ == "__main__":
    main()


def test_api_queue_metrics_integration():
    main()
