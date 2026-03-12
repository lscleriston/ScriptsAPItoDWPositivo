# Telefonia API QM

Coletor da API QueueMetrics com carga incremental em PostgreSQL.

## Estrutura do projeto

```text
Telefonia_API/
+-- src/
|   +-- telefonia_api/
|       +-- __init__.py
|       +-- __main__.py
|       +-- orchestrator.py
|       +-- config.py
|       +-- telefonia_client.py
|       +-- postgres_utils.py
|       +-- db_table_utils.py
|       +-- endpoints/
|           +-- __init__.py
|           +-- chamadas_atendidas.py
|           +-- chamadas_perdidas.py
+-- setup/
|   +-- create_control_table.py
|   +-- create_telefonia_table.py
|   +-- create_telefonia_perdidas_table.py
+-- scripts/
|   +-- discover_queues.py
|   +-- legacy/
|       +-- export_legacy_backends.py
+-- tests/
|   +-- conftest.py
|   +-- integration/
|       +-- test_api_integration.py
|       +-- test_check_columns.py
|       +-- test_validate_tables.py
+-- docs/
|   +-- reference/
|       +-- qm_api_doc_extracted.txt
+-- .env
+-- requirements.txt
+-- pytest.ini
```

## Configuracao (.env)

Defina no `.env` as variaveis de QueueMetrics e PostgreSQL.

Variaveis globais:
- `QUEUEMETRICS_URL`
- `QUEUEMETRICS_AUTH_USER`
- `QUEUEMETRICS_AUTH_PASS`
- `QUEUEMETRICS_ENDPOINTS` (ex.: `chamadas_atendidas,chamadas_perdidas`)

Variaveis por cliente:
- `QUEUEMETRICS_QUEUES_<CLIENTE>`
- `QUEUEMETRICS_ENDPOINTS_<CLIENTE>` (opcional, sobrescreve endpoints globais)
- `<CLIENTE>_START_DATE` (opcional, padrao `2024-01-01`)

PostgreSQL:
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`, `PG_SCHEMA`
- `TB_CONTROLER_CONSULTA_API` (opcional, padrao `tb_controler_consulta_api`)

Importante:
- Em `QUEUEMETRICS_QUEUES_<CLIENTE>`, use o ID tecnico da fila (ex.: `filadpu`, `filafnde`, `FILAIPHAN`), nao o nome amigavel.

## Controle incremental

A tabela de controle usa chave composta:
- `client_name` (lowercase)
- `table_name` (nome fisico da tabela de destino)
- `operacao` (UPPERCASE)

A coluna de data de controle no banco e:
- `last_data` (timestamp)

O processo atualiza `last_data` somente apos inserir registros no banco.

## Como executar

### 1. Instalar dependencias

```powershell
pip install -r requirements.txt
```

### 2. Criar tabelas iniciais

```powershell
python setup/create_control_table.py
python setup/create_telefonia_table.py
python setup/create_telefonia_perdidas_table.py
```

### 3. Executar orquestrador

```powershell
$env:PYTHONPATH = "src"
python -m telefonia_api
```

## Permissoes minimas no banco

Para o usuario de aplicacao, garantir ao menos:
- `SELECT`, `INSERT`, `UPDATE` na tabela de controle (`tb_controler_consulta_api`)
- `SELECT`, `INSERT`, `DELETE` nas tabelas de destino de telefonia (para deduplicacao por intervalo)

`DELETE` e `TRUNCATE` na tabela de controle nao sao necessarios para execucao normal.

## Testes

Os arquivos em `tests/integration` sao testes de integracao (dependem de API e/ou banco).

```powershell
$env:PYTHONPATH = "src"
pytest
```

## Scripts auxiliares

- Descobrir filas no QueueMetrics: `python scripts/discover_queues.py --help`
- Exportacoes legadas: `scripts/legacy/export_legacy_backends.py`
