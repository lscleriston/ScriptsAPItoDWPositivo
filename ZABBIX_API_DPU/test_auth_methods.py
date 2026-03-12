"""Script para testar diferentes formas de autenticacao na API Zabbix"""
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

ZABBIX_URL = os.getenv('ZABBIX_URL_DPU')
ZABBIX_TOKEN = os.getenv('ZABBIX_API_TOKEN_DPU')

print(f"[DEBUG] Testando diferentes métodos de autenticação")
print(f"[DEBUG] URL: {ZABBIX_URL}")
print(f"[DEBUG] Token (primeiros 20 chars): {ZABBIX_TOKEN[:20]}")
print("=" * 80)

 # Metodo 1: Usando auth como esta agora
print("\n[MÉTODO 1] Usando parâmetro 'auth' (atual)")
try:
    payload = {
        "jsonrpc": "2.0",
        "method": "host.get",
        "params": {"limit": 1},
        "auth": ZABBIX_TOKEN,
        "id": 1
    }
    response = requests.post(ZABBIX_URL, json=payload, timeout=10, verify=False)
    data = response.json()
    print("  Status: %d" % response.status_code)
    if 'error' in data:
        print("  [ERRO] %s" % data['error']['message'])
    else:
        print("  [OK] Hosts encontrados: %d" % len(data.get('result', [])))
except Exception as e:
    print("  [ERRO] %s" % str(e))

 # Metodo 2: Testar simples sem autenticacao (pode falhar)
print("\n[MÉTODO 2] Sem autenticação (test de conectividade)")
try:
    payload = {
        "jsonrpc": "2.0",
        "method": "apiinfo.version",
        "params": {},
        "id": 1
    }
    response = requests.post(ZABBIX_URL, json=payload, timeout=10, verify=False)
    data = response.json()
    print("  Status: %d" % response.status_code)
    if 'error' in data:
        print("  [ERRO] %s" % data['error']['message'])
    elif 'result' in data:
        print("  [OK] Versao Zabbix: %s" % data['result'])
    else:
        print("  Response: %s" % data)
except Exception as e:
    print("  [ERRO] %s" % str(e))

 # Metodo 3: Verificar se pode fazer login com user.login
print("\n[MÉTODO 3] Testando user.login (para obter auth token)")
try:
    payload = {
        "jsonrpc": "2.0",
        "method": "user.login",
        "params": {
            "username": "Admin",
            "password": "zabbix"  # Credencial padrão
        },
        "id": 1
    }
    response = requests.post(ZABBIX_URL, json=payload, timeout=10, verify=False)
    data = response.json()
    print("  Status: %d" % response.status_code)
    if 'error' in data:
        print("  [ERRO] %s" % data['error']['message'])
    elif 'result' in data and data['result']:
        print("  [OK] Login bem-sucedido!")
        print("     Token: %s..." % str(data['result'])[:50])
    else:
        print("  Response: %s" % data)
except Exception as e:
    print("  [ERRO] %s" % str(e))

print("\n" + "=" * 80)
print("[CONCLUSAO] Se METODO 2 funcionar, a API esta ok mas ha problema com autenticacao.")
print("[CONCLUSAO] Se nenhum funcionar, pode ser problema de SSL/certificado.")
