import json
from typing import List, Tuple
import re
import html
from datetime import datetime


def _sanitize_col(name: str) -> str:
    # keep letters, numbers and underscore; replace others with underscore
    s = re.sub(r'[^0-9a-zA-Z_]', '_', name)
    # collapse multiple underscores and strip edges
    s = re.sub(r'_+', '_', s).strip('_')
    if not s:
        s = 'col'
    if s[0].isdigit():
        s = 'c_' + s
    return s.lower()


def ensure_table_and_columns(conn, cursor, nome_tabela, sample_row=None):
    """Ensure a table exists with typed columns based on sample data."""
    import os
    schema = os.environ.get('PG_SCHEMA') or 'public'
    qualified = nome_tabela if '.' in nome_tabela else f"{schema}.{nome_tabela}"

    # standard columns and types
    cols = {
        'id': 'BIGSERIAL PRIMARY KEY',
        'operacao': 'TEXT',
        'data': 'DATE',
        'payload': 'JSONB'
    }

    # Add columns from sample_row if provided
    if sample_row:
        for key, value in sample_row.items():
            if key in ('id', 'operacao', 'Data'):
                continue
            sanitized = _sanitize_col(key)
            if sanitized in cols:
                continue
            # Determine column type: prefer JSONB for complex types, numeric for numbers
            # Special-case: list of pairs like [['Time elapsed (ms)',1001], ...] -> create separate columns per pair
            if isinstance(value, list):
                # detect list-of-pairs (each element is list/tuple of len>=2 and first is str)
                is_pairs = all(isinstance(el, (list, tuple)) and len(el) == 2 and isinstance(el[0], str) for el in value)
                if is_pairs:
                    # add a column for each pair key using parent_child naming
                    for el in value:
                        child_key = el[0]
                        child_sanit = _sanitize_col(child_key)
                        colname = f"{sanitized}_{child_sanit}"
                        if colname in cols:
                            continue
                        # infer child type
                        sample_val = el[1]
                        if isinstance(sample_val, bool):
                            child_type = 'BOOLEAN'
                        elif isinstance(sample_val, int):
                            child_type = 'BIGINT'
                        elif isinstance(sample_val, float):
                            child_type = 'DOUBLE PRECISION'
                        else:
                            child_type = 'TEXT'
                        cols[colname] = child_type
                    # also keep the parent as JSONB backup
                    cols[sanitized] = 'JSONB'
                    continue
                # otherwise fall back to JSONB for lists
                col_type = 'JSONB'
            elif isinstance(value, dict):
                col_type = 'JSONB'
            elif 'date' in key.lower() or 'hora' in key.lower():
                col_type = 'TEXT'
            elif isinstance(value, bool):
                col_type = 'BOOLEAN'
            elif isinstance(value, int):
                col_type = 'BIGINT'
            elif isinstance(value, float):
                col_type = 'DOUBLE PRECISION'
            else:
                col_type = 'TEXT'
            cols[sanitized] = col_type

    # create table if not exists
    cols_sql = ', '.join([f"{c} {t}" for c, t in cols.items()])
    create_sql = f"CREATE TABLE IF NOT EXISTS {qualified} ({cols_sql})"
    cursor.execute(create_sql)

    # ensure any missing columns
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (schema, qualified.split('.')[-1]))
    existing = {r[0] for r in cursor.fetchall()}
    for c, t in cols.items():
        if c in existing:
            continue
        try:
            cursor.execute(f"ALTER TABLE {qualified} ADD COLUMN {c} {t}")
        except Exception:
            pass

    # commit after ensuring table
    try:
        conn.commit()
    except Exception:
        pass


def _strip_html(s):
    if s is None:
        return s
    if not isinstance(s, str):
        return s
    # unescape common HTML entities and remove non-breaking spaces
    return html.unescape(s).replace('\xa0', ' ').replace('\u00a0', ' ').replace('&nbsp;', ' ').strip()


def _parse_duration_to_seconds(s: str):
    """Parse strings like '1:36', '0:08', '1:02:34' into total seconds. Returns int or None."""
    if s is None:
        return None
    if not isinstance(s, str):
        return None
    s = _strip_html(s)
    s = s.strip()
    if not s:
        return None
    # remove stray non-digit/colon characters
    s = re.sub(r'[^0-9:]', '', s)
    parts = s.split(':')
    try:
        parts = [int(p) for p in parts if p != '']
    except Exception:
        return None
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    return None


def _ensure_detail_table(conn, cursor, schema, parent_table, detail_table_name, headers):
    qualified_parent = parent_table if '.' in parent_table else f"{schema}.{parent_table}"
    qualified = detail_table_name if '.' in detail_table_name else f"{schema}.{detail_table_name}"
    # standard columns: id, parent_id, raw_row
    base_cols = [("id", "BIGSERIAL PRIMARY KEY"), ("parent_id", "BIGINT"), ("raw_row", "JSONB")]
    # create table if not exists with base cols
    cols_sql = ', '.join([f"{c} {t}" for c, t in base_cols])
    sql = f"CREATE TABLE IF NOT EXISTS {qualified} ({cols_sql})"
    cursor.execute(sql)
    # ensure header columns exist (add if missing) with unique sanitized names
    def _unique_names(headers_list):
        seen = set()
        names = []
        for h in headers_list:
            base = _sanitize_col(h)
            if base in ('id', 'parent_id', 'raw_row'):
                base = 'c_' + base
            name = base
            i = 1
            while name in seen:
                i += 1
                name = f"{base}_{i}"
            seen.add(name)
            names.append(name)
        return names

    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (schema, qualified.split('.')[-1]))
    existing = {r[0] for r in cursor.fetchall()}
    unique_cols = _unique_names(headers)
    for col in unique_cols:
        if col not in existing:
            try:
                cursor.execute(f"ALTER TABLE {qualified} ADD COLUMN {col} TEXT")
            except Exception:
                pass
    try:
        conn.commit()
    except Exception:
        pass
    return qualified


def _insert_detail_rows(conn, cursor, qualified_detail, parent_id, headers, rows):
    # headers: list of header names; rows: list of row lists (including header maybe)
    if not rows or len(rows) <= 1:
        return 0
    hdr = rows[0]
    inserted = 0
    # build unique sanitized column list from headers (must match _ensure_detail_table)
    def _unique_names(headers_list):
        seen = set()
        names = []
        for h in headers_list:
            base = _sanitize_col(h)
            if base in ('id', 'parent_id', 'raw_row'):
                base = 'c_' + base
            name = base
            i = 1
            while name in seen:
                i += 1
                name = f"{base}_{i}"
            seen.add(name)
            names.append(name)
        return names

    cols = _unique_names(hdr)
    col_list = ','.join(['parent_id', 'raw_row'] + cols)
    placeholders = ','.join(['%s'] * (2 + len(cols)))
    for row in rows[1:]:
        # ensure row length matches header length
        cells = list(row) if isinstance(row, (list, tuple)) else [row]
        # normalize cells to strings, strip html
        norm = []
        for v in cells[:len(cols)]:
            if isinstance(v, str):
                vv = _strip_html(v)
            else:
                vv = v
            # attempt to parse durations for known columns
            norm.append(vv)
        raw = None
        try:
            raw = json.dumps(row, ensure_ascii=False, default=str)
        except Exception:
            raw = json.dumps([str(x) for x in row], ensure_ascii=False)
        vals = [parent_id, raw] + [str(x) if x is not None else None for x in norm[:len(cols)]]
        try:
            sql = f"INSERT INTO {qualified_detail} ({col_list}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(vals))
            inserted += 1
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            continue
    try:
        conn.commit()
    except Exception:
        pass
    return inserted


def insert_records(conn, cursor, nome_tabela, records: List[dict], operacao: str) -> Tuple[int, List[int]]:
    """Insert records into the table."""
    inseridos = 0
    ids = []
    if not records:
        return 0, []

    import os
    schema = os.environ.get('PG_SCHEMA') or 'public'
    qualified = nome_tabela if '.' in nome_tabela else f"{schema}.{nome_tabela}"

    # We'll refresh existing columns per-record so we can add missing child columns
    canonical = ['id', 'operacao', 'data', 'payload']

    def _original_key_for_col(rec: dict, col: str):
        # find original key in rec whose sanitized name matches col
        for k in rec.keys():
            if _sanitize_col(k) == col:
                return k
        return None

    for rec in records:
        # refresh schema metadata for this table
        cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, qualified.split('.')[-1]))
        cols_info = {r[0]: r[1] for r in cursor.fetchall()}
        existing_cols = list(cols_info.keys())

        # Detect list-of-pairs in this record and create missing child columns
        for k, v in list(rec.items()):
            if isinstance(v, (list, tuple)) and v:
                # Only treat as pairs if EVERY element has exactly 2 items (key, value)
                # Grids (len > 2) are handled later as detail tables
                is_pairs = all(isinstance(el, (list, tuple)) and len(el) == 2 and isinstance(el[0], str) for el in v)
                if is_pairs:
                    parent_s = _sanitize_col(k)
                    for el in v:
                        child_key = el[0]
                        child_s = _sanitize_col(child_key)
                        colname = f"{parent_s}_{child_s}"
                        if colname not in cols_info:
                            # add as TEXT to be safe; try to infer basic numeric types
                            sample_val = el[1]
                            if isinstance(sample_val, bool):
                                ctype = 'BOOLEAN'
                            elif isinstance(sample_val, int):
                                ctype = 'BIGINT'
                            elif isinstance(sample_val, float):
                                ctype = 'DOUBLE PRECISION'
                            else:
                                ctype = 'TEXT'
                            try:
                                cursor.execute(f"ALTER TABLE {qualified} ADD COLUMN {colname} {ctype}")
                                try:
                                    conn.commit()
                                except Exception:
                                    pass
                            except Exception:
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass
        # refresh cols_info after any potential ALTERs
        cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, qualified.split('.')[-1]))
        cols_info = {r[0]: r[1] for r in cursor.fetchall()}
        existing_cols = list(cols_info.keys())

        # build insert column list (exclude id so DB default bigserial applies)
        insert_cols = [c for c in existing_cols if c != 'id']
        col_list_sql = ','.join(insert_cols)
        placeholders = ','.join(['%s'] * len(insert_cols))

        # build values in the exact order
        vals = []
        for c in insert_cols:
            if c == 'operacao':
                vals.append(operacao)
                continue
            if c == 'data':
                vals.append(rec.get('Data'))
                continue
            if c == 'payload':
                # ensure any non-serializable objects (date, datetime, etc.) are stringified
                try:
                    vals.append(json.dumps(rec, ensure_ascii=False, default=str))
                except TypeError:
                    vals.append(json.dumps({k: (v.isoformat() if hasattr(v, 'isoformat') else str(v)) for k, v in rec.items()}, ensure_ascii=False))
                continue
            # dynamic columns: try to map from original keys
            orig = _original_key_for_col(rec, c)
            val = None
            if orig:
                val = rec.get(orig)
            else:
                # try parent_child mapping for list-of-pairs: split column into parent and child
                parts = c.split('_', 1)
                if len(parts) == 2:
                    parent_s, child_s = parts
                    # find original parent key
                    parent_key = None
                    for k in rec.keys():
                        if _sanitize_col(k) == parent_s:
                            parent_key = k
                            break
                    if parent_key:
                        parent_val = rec.get(parent_key)
                        # if parent_val is list of pairs, find matching child
                        if isinstance(parent_val, (list, tuple)):
                            for el in parent_val:
                                try:
                                    label = el[0]
                                    value_el = el[1]
                                except Exception:
                                    continue
                                if _sanitize_col(str(label)) == child_s:
                                    val = value_el
                                    break
            # coerce based on column type
            ctype = cols_info.get(c, '').lower()
            if val is None:
                vals.append(None)
            else:
                if 'json' in ctype:
                    # store structured values as JSON string (psycopg2 will adapt)
                    vals.append(json.dumps(val, ensure_ascii=False, default=str))
                elif ctype in ('bigint', 'integer'):
                    try:
                        vals.append(int(val))
                    except Exception:
                        vals.append(None)
                elif ctype in ('double precision', 'numeric', 'real'):
                    try:
                        vals.append(float(val))
                    except Exception:
                        vals.append(None)
                else:
                    vals.append(str(val))

        try:
            sql = f"INSERT INTO {qualified} ({col_list_sql}) VALUES ({placeholders}) RETURNING id"
            cursor.execute(sql, tuple(vals))
            row = cursor.fetchone()
            if row:
                inseridos += 1
                ids.append(row[0])
                # Detail grids (e.g. DetailsDO.CallsOK) are stored as JSONB in the parent row.
                # No separate detail tables are created.
        except Exception:
            import traceback
            print(f"[ERROR] Falha ao inserir na tabela {qualified}")
            traceback.print_exc()
            try:
                conn.rollback()
            except Exception:
                pass
            continue

    try:
        conn.commit()
    except Exception:
        pass

    return inseridos, ids