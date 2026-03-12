#!/usr/bin/env python3
"""
Script standalone para descobrir filas acessiveis de um usuario na API do QueueMetrics.

Objetivo:
- Nao depende dos modulos do projeto.
- Testa diferentes metodos/consultas na API para encontrar onde as filas aparecem.
- Mostra no console a melhor fonte encontrada e uma sugestao de linha .env.

Uso rapido:
python descobrir_filas_queuemetrics.py \
  --url "https://host:8080/queuemetrics/QmStats/jsonStatsApi.do" \
  --user "APIGOV" \
  --password "sua_senha" \
  --client-name "DPU" \
  --insecure
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests.auth import HTTPBasicAuth


COMMON_QUEUE_METHODS: List[str] = [
    "Queues.list",
    "Queue.list",
    "Queues.get",
    "Queue.get",
    "Queues.getAll",
    "Queue.getAll",
    "Stats.listQueues",
    "Stats.getQueues",
    "Queues",
    "Queue",
]

INTROSPECTION_METHODS: List[str] = [
    "Help",
    "help",
    "API.help",
    "Api.help",
    "Methods.list",
    "API.methods",
    "Stats.help",
]

# Tentativas de Stats.get para extrair nomes de filas de respostas agregadas.
STATS_GET_PROBES: List[Dict[str, str]] = [
    {"block": "QueueDO"},
    {"block": "QueueDO.Details"},
    {"block": "SummaryDO"},
    {"block": "DetailsDO.CallsOK"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Descobre filas acessiveis para um usuario na API QueueMetrics."
    )
    parser.add_argument("--url", required=True, help="URL jsonStatsApi.do")
    parser.add_argument("--user", required=True, help="Usuario Basic Auth")
    parser.add_argument("--password", required=True, help="Senha Basic Auth")
    parser.add_argument(
        "--client-name",
        default="CLIENTE",
        help="Nome do cliente para gerar sugestao QUEUEMETRICS_QUEUES_<CLIENTE>",
    )
    parser.add_argument(
        "--from-date",
        default=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Data inicial para probes Stats.get (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to-date",
        default=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Data final para probes Stats.get (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=25,
        help="Timeout em segundos para cada requisicao",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Desabilita verificacao SSL (verify=False)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Mostra detalhes de cada tentativa",
    )
    parser.add_argument(
        "--raw-output",
        help="Caminho opcional para salvar JSON bruto das tentativas bem-sucedidas",
    )
    return parser.parse_args()


def call_api(
    url: str,
    auth: HTTPBasicAuth,
    timeout: int,
    verify_ssl: bool,
    method: str,
    extra_params: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    params: Dict[str, str] = {"method": method}
    if extra_params:
        params.update({k: v for k, v in extra_params.items() if v is not None})

    response = requests.get(
        url,
        params=params,
        auth=auth,
        timeout=timeout,
        verify=verify_ssl,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    try:
        return response.json()
    except ValueError:
        return {"_raw_text": response.text}


def looks_like_queue_name(value: str) -> bool:
    v = value.strip()
    if not v:
        return False

    # Evita datas/horas, numeros puros e termos comuns de erro.
    if re.fullmatch(r"\d+", v):
        return False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return False
    if re.fullmatch(r"\d{2}/\d{2}(\s*-\s*\d{2}:\d{2}:\d{2})?", v):
        return False

    low = v.lower()
    blacklist = {
        "ok",
        "error",
        "success",
        "failed",
        "detailsdo.callsok",
        "detailsdo.callsko",
        "stats.get",
    }
    if low in blacklist:
        return False

    # Nome de fila costuma ser token simples com letras/numeros/_/-/.
    if re.fullmatch(r"[A-Za-z0-9_.\-]{2,80}", v):
        return True

    return False


def extract_queues_from_json(data: Any) -> Set[str]:
    found: Set[str] = set()

    def walk(node: Any, parent_key: str = "") -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                k_low = key.lower()

                # Chaves que indicam forte chance de fila.
                if k_low in {"queue", "queues", "queuename", "queue_name", "fila", "filas"}:
                    if isinstance(value, str):
                        for part in re.split(r"[,;\s]+", value):
                            part = part.strip()
                            if looks_like_queue_name(part):
                                found.add(part)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, str) and looks_like_queue_name(item):
                                found.add(item)
                            elif isinstance(item, dict):
                                for field in ("name", "queue", "queue_name", "id"):
                                    raw = item.get(field)
                                    if isinstance(raw, str) and looks_like_queue_name(raw):
                                        found.add(raw)

                walk(value, key)

        elif isinstance(node, list):
            for item in node:
                walk(item, parent_key)

        elif isinstance(node, str):
            # Se ja estamos em um contexto sugestivo, coleta com regras mais permissivas.
            if parent_key.lower() in {"queue", "queues", "queuename", "queue_name", "fila", "filas"}:
                for part in re.split(r"[,;\s]+", node):
                    part = part.strip()
                    if looks_like_queue_name(part):
                        found.add(part)

    walk(data)
    return found


def normalize_queue_list(queues: Iterable[str]) -> List[str]:
    cleaned = []
    for q in queues:
        qq = q.strip()
        if qq:
            cleaned.append(qq)
    return sorted(set(cleaned), key=lambda x: x.lower())


def print_probe(debug: bool, title: str, ok: bool, detail: str) -> None:
    if debug:
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {title}: {detail}")


def run_discovery(args: argparse.Namespace) -> Tuple[List[str], List[Dict[str, Any]]]:
    verify_ssl = not args.insecure
    auth = HTTPBasicAuth(args.user, args.password)

    successful_calls: List[Dict[str, Any]] = []
    queue_candidates: Set[str] = set()

    # 1) Introspeccao: tenta descobrir metodos disponiveis.
    for method in INTROSPECTION_METHODS:
        try:
            payload = call_api(
                args.url,
                auth,
                args.timeout,
                verify_ssl,
                method,
                extra_params=None,
            )
            successful_calls.append(
                {"method": method, "params": {}, "response": payload}
            )
            extracted = extract_queues_from_json(payload)
            queue_candidates.update(extracted)
            print_probe(args.debug, f"Introspection {method}", True, f"queues={len(extracted)}")
        except Exception as exc:
            print_probe(args.debug, f"Introspection {method}", False, str(exc))

    # 2) Metodos comuns de listagem de fila.
    for method in COMMON_QUEUE_METHODS:
        try:
            payload = call_api(
                args.url,
                auth,
                args.timeout,
                verify_ssl,
                method,
                extra_params=None,
            )
            successful_calls.append(
                {"method": method, "params": {}, "response": payload}
            )
            extracted = extract_queues_from_json(payload)
            queue_candidates.update(extracted)
            print_probe(args.debug, f"Queue method {method}", True, f"queues={len(extracted)}")
        except Exception as exc:
            print_probe(args.debug, f"Queue method {method}", False, str(exc))

    # 3) Probes Stats.get com diferentes blocos e wildcard de queue.
    wildcard_values = ["*", "all", "ALL", "%", ""]
    for probe in STATS_GET_PROBES:
        for wildcard in wildcard_values:
            params = {
                "from": f"{args.from_date}.00:00:00",
                "to": f"{args.to_date}.23:59:59",
                "queues": wildcard,
            }
            params.update(probe)

            try:
                payload = call_api(
                    args.url,
                    auth,
                    args.timeout,
                    verify_ssl,
                    "Stats.get",
                    extra_params=params,
                )
                successful_calls.append(
                    {"method": "Stats.get", "params": params, "response": payload}
                )
                extracted = extract_queues_from_json(payload)
                queue_candidates.update(extracted)
                tag = f"Stats.get block={probe.get('block')} queues='{wildcard}'"
                print_probe(args.debug, tag, True, f"queues={len(extracted)}")
            except Exception as exc:
                tag = f"Stats.get block={probe.get('block')} queues='{wildcard}'"
                print_probe(args.debug, tag, False, str(exc))

    return normalize_queue_list(queue_candidates), successful_calls


def main() -> int:
    args = parse_args()

    print("=== Descoberta de filas permitidas (QueueMetrics) ===")
    print(f"URL: {args.url}")
    print(f"Usuario: {args.user}")
    print(f"SSL verify: {not args.insecure}")

    try:
        queues, successful_calls = run_discovery(args)
    except requests.exceptions.RequestException as exc:
        print(f"[ERRO] Falha HTTP ao consultar API: {exc}")
        return 2
    except Exception as exc:
        print(f"[ERRO] Falha inesperada: {exc}")
        return 3

    if args.raw_output:
        try:
            with open(args.raw_output, "w", encoding="utf-8") as f:
                json.dump(successful_calls, f, ensure_ascii=False, indent=2)
            print(f"[INFO] JSON bruto salvo em: {args.raw_output}")
        except Exception as exc:
            print(f"[WARN] Nao foi possivel salvar --raw-output: {exc}")

    if not successful_calls:
        print("[ERRO] Nenhuma chamada de descoberta foi aceita pela API.")
        print("[ERRO] Nao foi possivel identificar um local para listar filas com este usuario.")
        return 4

    if not queues:
        print("[ERRO] A API respondeu, mas nenhuma lista de filas foi identificada nas respostas.")
        print("[ERRO] Isso indica que sua instancia pode nao expor listagem de filas nesse endpoint.")
        print("[DICA] Rode com --debug e --raw-output para inspecionar os metodos aceitos.")
        return 5

    client_token = re.sub(r"[^A-Za-z0-9_]", "_", args.client_name.strip().upper()) or "CLIENTE"
    env_line = f"QUEUEMETRICS_QUEUES_{client_token}=" + ",".join(queues)

    print("\n[OK] Filas descobertas para o usuario:")
    for q in queues:
        print(f" - {q}")

    print("\nSugestao para .env:")
    print(env_line)

    print("\n[INFO] Total de filas identificadas:", len(queues))
    return 0


if __name__ == "__main__":
    sys.exit(main())
