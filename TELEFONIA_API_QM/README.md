# Telefonia API QM

Coletor da API QueueMetrics com carga incremental em PostgreSQL.

## Estrutura do projeto

```text
Telefonia_API/
+-- src/
¦   +-- telefonia_api/
¦       +-- __init__.py
¦       +-- __main__.py
¦       +-- orchestrator.py
¦       +-- config.py
¦       +-- telefonia_client.py
¦       +-- postgres_utils.py
¦       +-- db_table_utils.py
¦       +-- endpoints/
¦           +-- __init__.py
¦           +-- chamadas_atendidas.py
¦           +-- chamadas_perdidas.py
+-- tests/
¦   +-- conftest.py
¦   +-- integration/
¦       +-- test_api_integration.py
¦       +-- test_check_columns.py
¦       +-- test_validate_tables.py
+-- scripts/
¦   +-- discover_queues.py
¦   +-- legacy/
¦       +-- export_legacy_backends.py
+-- setup/
¦   +-- create_control_table.py
¦   +-- create_telefonia_table.py
¦   +-- create_telefonia_perdidas_table.py
+-- docs/
¦   +-- reference/
¦       +-- qm_api_doc_extracted.txt
+-- .env
+-- requirements.txt
+-- pytest.ini
```

## Configuracao

Defina no `.env` as variaveis de QueueMetrics e PostgreSQL:

- `QUEUEMETRICS_URL`, `QUEUEMETRICS_AUTH_USER`, `QUEUEMETRICS_AUTH_PASS`
- `QUEUEMETRICS_QUEUES_<CLIENTE>`
- `QUEUEMETRICS_ENDPOINTS` (ex.: `chamadas_atendidas,chamadas_perdidas`)
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`, `PG_SCHEMA`

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

## Testes

Os arquivos em `tests/integration` sao testes de integracao (dependem de API e/ou banco).

```powershell
$env:PYTHONPATH = "src"
pytest
```

## Scripts auxiliares

- Descobrir filas no QueueMetrics: `python scripts/discover_queues.py --help`
- Exportacoes legadas: `scripts/legacy/export_legacy_backends.py`
