import os
import csv
import json
import mysql.connector
import requests

# Directories for legacy exports
BASE_DIR = os.path.join(os.path.dirname(__file__), 'extracoes')
CSV_DIR = os.path.join(BASE_DIR, 'legacy_chamadas_atendidas_csv')
JSON_DIR = os.path.join(BASE_DIR, 'legacy_chamadas_atendidas_json')
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)


def salvar_csv(header, dados, ano, mes):
    if not dados:
        return None, None
    nome_arquivo = f"chamadas_atendidas_{mes:02d}_{ano}.csv"
    caminho = os.path.join(CSV_DIR, nome_arquivo)
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(header)
        writer.writerows(dados)
    return caminho, nome_arquivo


def salvar_json(header, dados, ano, mes):
    if not dados:
        return None
    nome_arquivo = f"chamadas_atendidas_{mes:02d}_{ano}.json"
    caminho = os.path.join(JSON_DIR, nome_arquivo)
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump([header] + dados, f, indent=2, ensure_ascii=False, default=str)
    return caminho


def _mysql_conn():
    host = os.environ.get('MYSQL_HOST') or os.environ.get('MARIADB_HOST')
    user = os.environ.get('MYSQL_USER') or os.environ.get('MARIADB_USER')
    password = os.environ.get('MYSQL_PASSWORD') or os.environ.get('MARIADB_PASSWORD')
    database = os.environ.get('MYSQL_DB') or os.environ.get('MARIADB_DB')
    port = int(os.environ.get('MYSQL_PORT') or os.environ.get('MARIADB_PORT') or 3306)
    if not host or not user or not password or not database:
        return None
    return mysql.connector.connect(host=host, port=port, user=user, password=password, database=database)


def salvar_dados_no_banco(caminho_csv, nome_arquivo, nome_tabela):
    """Reads CSV and inserts rows into a MySQL/MariaDB table using ON DUPLICATE KEY UPDATE if possible.
    Requires MYSQL_HOST/USER/PASSWORD/DB env vars. Returns number of inserted rows or None."""
    conn = _mysql_conn()
    if not conn:
        print('[legacy_backends] MySQL credentials not found in env; skipping DB save')
        return None
    cursor = conn.cursor()
    import csv as _csv
    inserted = 0
    try:
        with open(caminho_csv, mode='r', encoding='utf-8') as f:
            reader = _csv.DictReader(f)
            for row in reader:
                cols = ", ".join([f"`{c}`" for c in row.keys()])
                placeholders = ", ".join(["%s"] * len(row))
                vals = list(row.values())
                update_clause = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in row.keys()])
                query = f"INSERT INTO {nome_tabela} ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
                cursor.execute(query, vals)
                inserted += 1
        conn.commit()
        print(f'[legacy_backends] Inseridos {inserted} registros em {nome_tabela}')
        return inserted
    except Exception as e:
        print(f'[legacy_backends] Erro ao inserir no MySQL: {e}')
        return None
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


# OneDrive helpers: requires ONE_DRIVE_TOKENS_PATH and client credentials optionally
def _obter_novo_access_token(tokens_path, tenant_id, client_id, client_secret):
    try:
        with open(tokens_path, 'r') as file:
            tokens = json.load(file)
    except Exception as e:
        print('[legacy_backends] tokens file not found or invalid:', e)
        return None
    refresh_token = tokens.get('refresh_token')
    if not refresh_token:
        print('[legacy_backends] refresh_token not found in tokens file')
        return None
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    data = {
        'client_id': client_id,
        'scope': 'https://graph.microsoft.com/.default',
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token',
        'client_secret': client_secret
    }
    resp = requests.post(url, data=data)
    if resp.status_code == 200:
        tokens = resp.json()
        with open(tokens_path, 'w') as f:
            json.dump(tokens, f)
        return tokens.get('access_token')
    print('[legacy_backends] failed to refresh onedrive token', resp.status_code, resp.text)
    return None


def salvar_dados_no_onedrive(caminho_arquivo, nome_arquivo):
    tokens_path = os.environ.get('ONE_DRIVE_TOKENS_PATH')
    tenant_id = os.environ.get('ONE_DRIVE_TENANT_ID')
    client_id = os.environ.get('ONE_DRIVE_CLIENT_ID')
    client_secret = os.environ.get('ONE_DRIVE_CLIENT_SECRET')
    if not tokens_path or not tenant_id or not client_id or not client_secret:
        print('[legacy_backends] OneDrive credentials missing; skipping OneDrive upload')
        return False
    access_token = _obter_novo_access_token(tokens_path, tenant_id, client_id, client_secret)
    if not access_token:
        return False
    upload_url = f'https://graph.microsoft.com/v1.0/me/drive/root:/Carrefour/Carrefour_atendidas/{nome_arquivo}:/content'
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/octet-stream'}
    try:
        with open(caminho_arquivo, 'rb') as f:
            resp = requests.put(upload_url, headers=headers, data=f)
        if resp.status_code in (200, 201):
            print('[legacy_backends] Arquivo enviado para OneDrive com sucesso')
            return True
        print('[legacy_backends] OneDrive upload failed', resp.status_code, resp.text)
        return False
    except Exception as e:
        print('[legacy_backends] Exception uploading to OneDrive:', e)
        return False
