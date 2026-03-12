"""Endpoint: chamadas atendidas (DetailsDO.CallsOK)
Cada chamada do grid é expandida em uma linha individual com colunas próprias,
replicando o comportamento do script legado CARREFOUR_chamadas_atendidas.py.
"""
import re
from datetime import datetime, timedelta, date
from calendar import monthrange
from typing import Dict, Any

from ..telefonia_client import TelefoniaClient
from ..postgres_utils import get_pg_conn, get_last_data, update_last_data
from ..db_table_utils import ensure_table_and_columns, insert_records


# ---------------------------------------------------------------------------
# Helpers (mesma lógica do script antigo)
# ---------------------------------------------------------------------------

def _padronizar_tempo(valor):
    """Normaliza valores de tempo para HH:MM:SS, removendo &nbsp;."""
    if not isinstance(valor, str):
        return valor
    valor = valor.replace("&nbsp;", "").strip()
    if re.match(r"^\d{1,2}$", valor):
        return f"00:00:{valor.zfill(2)}"
    elif re.match(r"^\d{1,2}:\d{2}$", valor):
        return f"00:{valor}"
    elif re.match(r"^\d{1,2}:\d{2}:\d{2}$", valor):
        return valor
    return valor


def _extrair_chamadas(api_response, fila, operacao, ano, block_key="DetailsDO.CallsOK"):
    """Extrai cada chamada do grid da API como um dict com colunas individuais.

    Replica a lógica de ``tratar_dados`` do script antigo:
    - Localiza o cabeçalho (1ª lista cujo 1º elemento é "Date")
    - Parseia "MM/DD - HH:MM:SS" em campos ``Data`` (date) e ``Hora`` (text)
    - Normaliza tempos
    - Adiciona ``Fila`` e ``operacao``
    """
    chamadas = []
    resultado = api_response.get(block_key, [])
    if not resultado:
        return chamadas

    # Localizar cabeçalho
    header = None
    data_rows = []
    for i, bloco in enumerate(resultado):
        if isinstance(bloco, list) and bloco and all(isinstance(h, str) for h in bloco):
            header = bloco
            data_rows = resultado[i + 1:]
            break

    if not header or not data_rows:
        return chamadas

    # Deduplicar nomes de colunas (ex: múltiplos "&nbsp;")
    seen = {}
    header_dedup = []
    for h in header[1:]:                       # pula "Date" — será convertido em Data + Hora
        key = h.strip() if h.strip() else "Campo"
        if key in seen:
            seen[key] += 1
            header_dedup.append(f"{key}_{seen[key]}")
        else:
            seen[key] = 0
            header_dedup.append(key)

    for row in data_rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue

        # Normalizar tempos
        row = [_padronizar_tempo(c) if isinstance(c, str) else c for c in row]

        # Parsear data/hora  — formato da API: "MM/DD - HH:MM:SS"
        raw_dt = str(row[0]) if row[0] else ""
        match = re.match(r"(\d{2})/(\d{2})\s*-\s*(\d{2}):(\d{2}):(\d{2})", raw_dt)
        if match:
            mm, dd = int(match.group(1)), int(match.group(2))
            hora = f"{match.group(3)}:{match.group(4)}:{match.group(5)}"
            try:
                data_date = date(ano, mm, dd)
            except ValueError:
                data_date = None
        else:
            data_date = None
            hora = ""

        # Montar registro plano
        record = {"Data": data_date, "Hora": hora}
        for j, col_name in enumerate(header_dedup):
            record[col_name] = row[j + 1] if (j + 1) < len(row) else None
        record["Fila"] = fila
        record["operacao"] = operacao
        chamadas.append(record)

    return chamadas


# ---------------------------------------------------------------------------
# Função principal chamada pelo orchestrator
# ---------------------------------------------------------------------------

def process(client_config: Dict[str, Any], client_name: str):
    url = client_config["url"]
    auth_user = client_config["auth_user"]
    auth_pass = client_config["auth_pass"]
    queues_str = client_config.get("queues", "filadpu")
    queues = [q.strip() for q in queues_str.split(",")] if "," in queues_str else [queues_str]

    client = TelefoniaClient(url, auth_user, auth_pass)

    conn = get_pg_conn()
    cursor = conn.cursor()

    nome_tabela = "tb_telefonia_atendidas"
    operacao = client_name.upper()

    # Verificar última data coletada
    ultima_data = get_last_data(cursor, client_name, nome_tabela, operacao)
    hoje = datetime.today().date()
    data_fim = hoje - timedelta(days=1)

    if ultima_data and ultima_data >= data_fim:
        print(f"[INFO] Dados já atualizados para {operacao}")
        return

    if ultima_data:
        data_inicio = ultima_data + timedelta(days=1)
    else:
        _start = client_config.get('start_date', '2024-01-01')
        data_inicio = datetime.strptime(_start, '%Y-%m-%d').date()
        print(f"[INFO] Primeira coleta para {operacao}. Data inicial: {_start}")

    # Gerar lista de meses
    meses = []
    atual = datetime(data_inicio.year, data_inicio.month, 1)
    data_fim_dt = datetime.combine(data_fim, datetime.min.time())
    while atual <= data_fim_dt:
        meses.append((atual.year, atual.month))
        atual = datetime(
            atual.year + (1 if atual.month == 12 else 0),
            (atual.month % 12) + 1,
            1,
        )

    # Coletar e expandir chamadas — mês a mês com commit incremental
    import os as _os
    schema = _os.environ.get('PG_SCHEMA', 'public')
    qualified = nome_tabela if '.' in nome_tabela else f"{schema}.{nome_tabela}"
    total_inseridos = 0

    for queue in queues:
        for ano, mes in meses:
            primeiro_dia = date(ano, mes, 1)
            ultimo_dia = date(ano, mes, monthrange(ano, mes)[1])
            inicio_real = max(data_inicio, primeiro_dia)
            fim_real = min(data_fim, ultimo_dia)

            if inicio_real > fim_real:
                continue

            print(f"[INFO] Coletando chamadas atendidas de {queue} para {mes:02d}/{ano} "
                  f"({inicio_real} a {fim_real})")

            from_p = inicio_real.strftime("%Y-%m-%d.00:00:00")
            to_p = fim_real.strftime("%Y-%m-%d.23:59:59")

            month_data = []
            responses = client.get_data(queue, from_p, to_p, "DetailsDO.CallsOK")
            for resp in responses:
                chamadas = _extrair_chamadas(resp, queue, operacao, ano)
                if chamadas:
                    month_data.extend(chamadas)

            if not month_data:
                print(f"[INFO] Nenhuma chamada atendida em {queue} {mes:02d}/{ano}")
                continue

            print(f"[INFO] {len(month_data)} chamadas extraídas de {queue} {mes:02d}/{ano}")

            # Garantir tabela/colunas
            ensure_table_and_columns(conn, cursor, nome_tabela, month_data[0])

            # Dedup: remover registros existentes para esse intervalo/operação
            try:
                cursor.execute(
                    f"DELETE FROM {qualified} WHERE operacao = %s AND data >= %s AND data <= %s",
                    (operacao, inicio_real, fim_real),
                )
                removed = cursor.rowcount
                if removed:
                    print(f"[INFO] Removidos {removed} registros antigos de {inicio_real} a {fim_real} (dedup)")
            except Exception:
                pass

            # Inserir novos registros
            inseridos, ids = insert_records(conn, cursor, nome_tabela, month_data, operacao)
            total_inseridos += inseridos
            print(f"[INFO] Inseridos {inseridos} registros para {queue} {mes:02d}/{ano}")

            # Atualizar controle apenas após gravação real no banco
            if inseridos > 0:
                update_last_data(cursor, None, client_name, nome_tabela, operacao, fim_real)
                print(f"[INFO] last_data atualizado para {fim_real}")

    print(f"[INFO] Total inseridos em {nome_tabela}: {total_inseridos} registros")
    conn.close()