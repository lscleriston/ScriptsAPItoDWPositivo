"""Script para testar conexão com a API Zabbix"""
import requests
import json
from dotenv import load_dotenv
import os

# Carrega variáveis do .env
load_dotenv()

ZABBIX_URL = os.getenv('ZABBIX_URL_DPU')
ZABBIX_TOKEN = os.getenv('ZABBIX_API_TOKEN_DPU')

print(f"[INFO] Testando conectividade com API Zabbix")
print(f"[INFO] URL: {ZABBIX_URL}")
print(f"[INFO] Token: {ZABBIX_TOKEN[:20]}..." if ZABBIX_TOKEN else "[ERROR] Token não configurado")
print("-" * 80)

# Teste 1: Verificar se URL responde
try:
    print("[TESTE 1] Verificando se a URL responde...")
    response = requests.get(ZABBIX_URL, timeout=10, verify=False)
    print(f"  Status Code: {response.status_code}")
    print(f"  Content-Type: {response.headers.get('Content-Type', 'N/A')}")
    if response.status_code == 404:
        print("  ❌ URL retorna 404 - endpoint não existe")
    elif response.status_code != 200:
        print(f"  ⚠️ Resposta: {response.text[:200]}")
except Exception as e:
    print(f"  ❌ Erro ao acessar URL: {e}")

print()

# Teste 2: Tentar chamar a API com autenticação
try:
    print("[TESTE 2] Tentando autenticar na API...")
    
    payload = {
        "jsonrpc": "2.0",
        "method": "user.get",
        "params": {},
        "auth": ZABBIX_TOKEN,
        "id": 1
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    response = requests.post(ZABBIX_URL, json=payload, headers=headers, timeout=10, verify=False)
    
    print(f"  Status Code: {response.status_code}")
    print(f"  Response: {response.text[:500]}")
    
    # Verifica se é JSON válido
    try:
        data = response.json()
        if 'error' in data:
            print(f"  ❌ Erro da API: {data['error']}")
        elif 'result' in data:
            print(f"  ✅ Autenticação bem-sucedida! Usuários encontrados: {len(data['result'])}")
        else:
            print(f"  Response JSON: {data}")
    except json.JSONDecodeError:
        print(f"  ⚠️ Resposta não é JSON válido")
        
except Exception as e:
    print(f"  ❌ Erro ao conectar: {e}")

print()

# Teste 3: Verificar se é um problema de URL
print("[TESTE 3] Testando URL sem '/zabbix/': https://monitoramento.dpu.def.br/api_jsonrpc.php")
try:
    alt_url = "https://monitoramento.dpu.def.br/api_jsonrpc.php"
    response = requests.get(alt_url, timeout=10, verify=False)
    print(f"  Status Code: {response.status_code}")
    if response.status_code == 200:
        print(f"  ✅ Esta URL responde melhor!")
    else:
        print(f"  Response: {response.text[:200]}")
except Exception as e:
    print(f"  Erro: {e}")

print()
print("-" * 80)
print("[CONCLUSÃO] Verifique os testes acima para identificar o problema.")
