# Zabbix API Collector

Sistema de coleta automatizada de dados da API REST do Zabbix para múltiplas instâncias, com armazenamento em PostgreSQL.

## 📋 Visão Geral

Este projeto implementa uma arquitetura modular para coletar dados de diferentes endpoints da API Zabbix de múltiplas organizações, armazenando os dados em um banco PostgreSQL com controle incremental de sincronização.

### Funcionalidades Principais

- ✅ **Múltiplas Instâncias Zabbix**: Suporte a diferentes organizações
- ✅ **Endpoints Modulares**: Coleta de hosts, events, e outros endpoints
- ✅ **Sincronização Incremental**: Controle de último ID/timestamp processado por endpoint/cliente
- ✅ **Processamento Paralelo**: Execução concorrente para endpoints de log
- ✅ **Tabelas Dinâmicas**: Criação automática de tabelas baseada nos dados

## 🏗️ Arquitetura e Estrutura

### Arquivos Principais

```
Zabbix_Api/
├── zabbix_orchestrator.py     # 🏃 Orquestrador principal - ponto de entrada
├── config.py                  # ⚙️ Carregamento e parsing de configurações
├── zabbix_client.py          # 🌐 Cliente HTTP para API Zabbix
├── postgres_utils.py         # 🗄️ Utilitários PostgreSQL (conexão, controle incremental)
├── db_table_utils.py         # 📊 Utilitários para criação dinâmica de tabelas
├── endpoints/                # 📁 Módulos de processamento por endpoint
│   ├── host.py               # 🖥️ Hosts (dados dos servidores monitorados)
│   ├── event.py              # 📝 Events (eventos e alertas)
│   ├── sla.py                # 🏷️ SLAs e tags associadas
│   └── __init__.py
├── .env                      # 🔐 Configurações de ambiente (credenciais)
├── requirements.txt          # 📦 Dependências Python
└── README.md                # 📖 Esta documentação
```

### Fluxo de Execução

1. **Configuração**: `config.py` carrega variáveis de ambiente e detecta clientes Zabbix
2. **Orquestração**: `zabbix_orchestrator.py` itera por cada cliente configurado
3. **Coleta**: Para cada endpoint, importa o módulo correspondente em `endpoints/`
4. **Processamento**: Cada módulo `process()` coleta dados via `zabbix_client.py`
5. **Armazenamento**: Dados inseridos no PostgreSQL com controle incremental

## ⚙️ Configuração

### 1. Arquivo .env

O sistema suporta múltiplas instâncias Zabbix através de variáveis de ambiente:

```bash
# Instância INCRA
ZABBIX_URL_INCRA=https://monitoramento.incra.gov.br/zabbix/api_jsonrpc.php
ZABBIX_API_TOKEN_INCRA=seu_token_aqui
ZABBIX_ENDPOINTS_INCRA=host,event

# Instância Outra ORG
ZABBIX_URL_ORG2=https://zabbix.org2.gov.br/api_jsonrpc.php
ZABBIX_API_TOKEN_ORG2=token_org2_aqui
ZABBIX_ENDPOINTS_ORG2=host,event

# PostgreSQL
PG_HOST=10.34.5.183
PG_PORT=3306
PG_DB=dw_positivo
PG_USER=db_brian
PG_PASSWORD=suasenha
PG_SCHEMA=dw_positivo
```

### 2. Instalação de Dependências

```bash
cd /home/Scripts/Zabbix_Api
pip install -r requirements.txt
```

**Dependências:**
- `python-dotenv`: Carregamento de variáveis de ambiente
- `requests`: Cliente HTTP para API Zabbix
- `psycopg2-binary`: Driver PostgreSQL
- `pandas`: Manipulação de dados (opcional)

## 🚀 Como Usar

### Teste da Estrutura

Antes de executar em produção, teste se tudo está configurado corretamente:

```bash
cd /home/Scripts/Zabbix_Api
python test_structure.py
```

### Execução Manual

```bash
cd /home/Scripts/Zabbix_Api
python zabbix_orchestrator.py
```

### Execução Agendada

Adicione ao crontab para execução automática:

```bash
# A cada 2 horas das 08h às 20h (horas pares)
0 8,10,12,14,16,18,20 * * * /home/Scripts/venv_scripts/bin/python /home/Scripts/Zabbix_Api/zabbix_orchestrator.py >> /home/Scripts/zabbix_cron.log 2>&1
```

## 📊 Estrutura do Banco de Dados

### Tabelas Principais

- **`tb_controler_consulta_api`**: Controle incremental por cliente/endpoint
- **`tb_zabbix_host`**: Hosts monitorados
- **`tb_zabbix_event`**: Eventos e alertas
- **`tb_zabbix_sla`**: SLAs configuradas e suas tags (service tags)

### Dados Coletados

#### Hosts (`tb_zabbix_host`)
- `hostid`: ID único do host no Zabbix
- `host`: Nome técnico do host
- `name`: Nome de exibição
- `status`: Status do host
- `ip`: Endereço IP
- `groupid`: ID do grupo
- `group_name`: Nome do grupo
- `operacao`: Cliente/organização

#### Events (`tb_zabbix_event`)
- `eventid`: ID único do evento
- `source`: Origem do evento
- `object`: Tipo de objeto
- `objectid`: ID do objeto relacionado
- `clock`: Timestamp Unix
- `value`: Valor do evento
- `acknowledged`: Se foi reconhecido
- `name`: Nome/descrição do evento
- `severity`: Severidade
- `operacao`: Cliente/organização

#### SLA (`tb_zabbix_sla`)
- `slaid`: ID do SLA
- `sla_name`: Nome configurado no Zabbix
- `tag`: Nome da tag de serviço
- `tag_value`: Valor associado
- `tag_operator` / `tag_operator_label`: Operador (0 = equals, 2 = contains)
- `slo`, `period`, `effective_date`, `timezone`, `status`, `description`: Metadados do SLA
- `operacao`: Cliente/organização

## 🔧 Desenvolvimento

### Adicionando Novo Endpoint

1. **Criar módulo**: `endpoints/<recurso>.py`
2. **Implementar função**: `process(client, client_name, nome_tabela, operacao, nome_tabela_ultimo_id)`
3. **Adicionar endpoint**: Na variável `ZABBIX_ENDPOINTS_<SUFIXO>` no `.env`

> Novo: para coletar SLAs e suas tags, adicione `sla` no endpoint do cliente (ex.: `ZABBIX_ENDPOINTS_DPU=host,event,sla`).
> O handler `endpoints/sla.py` invoca `sla.get` com `selectServiceTags=extend`, expande cada tag em registros individuais e persiste em `tb_zabbix_sla`, substituindo o snapshot da operação a cada execução.

Exemplo de estrutura do módulo:

```python
def process(client, client_name, nome_tabela, operacao, nome_tabela_ultimo_id):
    """Processa dados do endpoint específico"""
    # 1. Obter último ID processado
    ultimo_id, range_id = _obter_ultimo_id_banco(client_name, nome_tabela, operacao, nome_tabela_ultimo_id)

    # 2. Coletar dados da API
    dados = client.get_<endpoint>(params)

    # 3. Processar e inserir dados
    # ... lógica específica do endpoint ...

    # 4. Atualizar controle incremental
    _atualizar_tb_ultimo_id(client_name, nome_tabela, operacao, nome_tabela_ultimo_id, novo_ultimo_id)

    return len(dados), ids_inseridos
```

### Adicionando Nova Instância Zabbix

1. **Adicionar variáveis** no `.env`:
   ```bash
   ZABBIX_URL_NOVAORG=https://zabbix.novaorg.gov.br/api_jsonrpc.php
   ZABBIX_API_TOKEN_NOVAORG=token_aqui
   ZABBIX_ENDPOINTS_NOVAORG=host,event
   ```

2. **Executar coleta**: O sistema detectará automaticamente a nova instância

## 📈 Monitoramento e Logs

- **Logs de execução**: Arquivo de log definido no crontab
- **Status do serviço**: `sudo systemctl status cron`
- **Verificação manual**: Execute `python zabbix_orchestrator.py` e verifique output

## 🔍 Troubleshooting

### Problemas Comuns

1. **Erro de autenticação Zabbix**: Verificar API token no `.env`
2. **Erro de conexão PostgreSQL**: Verificar credenciais e conectividade
3. **Endpoint não encontrado**: Verificar se módulo existe em `endpoints/`
4. **Dados não atualizando**: Verificar tabela de controle `tb_controler_consulta_api`

### Comandos Úteis

```bash
# Testar conexão PostgreSQL
python -c "from postgres_utils import get_pg_conn; print('Conexão OK' if get_pg_conn() else 'Erro')"

# Verificar tabelas criadas
PGHOST=10.34.5.183 PGUSER=db_brian PGPASSWORD=RFAXB@r PGDATABASE=dw_positivo psql -c "\dt tb_zabbix_*"

# Verificar último processamento
PGHOST=10.34.5.183 PGUSER=db_brian PGPASSWORD=RFAXB@r PGDATABASE=dw_positivo psql -c "SELECT * FROM dw_positivo.tb_controler_consulta_api ORDER BY updated_at DESC LIMIT 5;"
```

## 📝 Notas Técnicas

- **Controle Incremental**: Usa tabela `tb_controler_consulta_api` para evitar reprocessamento
- **API Zabbix**: Utiliza JSON-RPC 2.0 com Bearer token
- **Schema PostgreSQL**: Dados armazenados em schema `dw_positivo`
- **Nomes de Tabelas**: Padrão `tb_zabbix_<endpoint>` em minúsculas

---

**Última atualização**: Janeiro 2026
**Mantenedor**: Equipe de Dados