import os
import re
import json
import csv
import io
import time
import psycopg2
import psycopg2.extras
import psycopg2.sql as sql
import hashlib
import traceback
from datetime import datetime, timedelta, timezone as _tz
from zoneinfo import ZoneInfo
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# ============================================================
# CONFIG BANCO (SOMENTE ENV/SECRETS)
# Configure na Lambda (prod e test):
# DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
#
# Para teste por schema:
#   ANALYTICS_SCHEMA=analytics_test
# Para produção (dbt target=prod):
#   ANALYTICS_SCHEMA=analytics
# Para desenvolvimento (dbt target=dev concatena schema → analytics_analytics):
#   ANALYTICS_SCHEMA=analytics_analytics
# ============================================================

ANALYTICS_SCHEMA = os.getenv("ANALYTICS_SCHEMA", "analytics").strip() or "analytics"
RT_SCHEMA = os.getenv("RT_SCHEMA", "rt").strip() or "rt"
MQTT_COMMAND_PREFIX = os.getenv("MQTT_COMMAND_PREFIX", "dev/write").strip().strip("/") or "dev/write"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "172.31.70.48"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "scada_ingestion_v2"),
    "user": os.getenv("DB_USER", "iot_user"),
    "password": os.getenv("DB_PASSWORD", "Maxwell1617!"),
    "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
}

_CONN = None
_CONN_LAST_OK_TS = 0
_LAST_PING_TS = 0
CONN_MAX_AGE_SEC = int(os.getenv("PG_CONN_MAX_AGE_SEC", "900"))
CONN_PING_EVERY_SEC = int(os.getenv("PG_CONN_PING_EVERY_SEC", "30"))

_IOT_DATA_CLIENT = None
_IOT_DATA_ENDPOINT = None


def _require_db_config():
    missing = []
    for k in ("host", "dbname", "user", "password"):
        if not DB_CONFIG.get(k):
            missing.append(k)
    if missing:
        raise Exception(
            f"DB_CONFIG inválido. Faltando: {', '.join(missing)}. "
            f"Configure env vars/Secrets: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD (e DB_PORT opcional)."
        )


def close_quiet(conn):
    try:
        if conn:
            conn.close()
    except Exception:
        pass


def rollback_quiet(conn):
    try:
        if conn:
            conn.rollback()
    except Exception:
        pass


def commit_quiet(conn):
    try:
        if conn:
            conn.commit()
    except Exception:
        pass


def db_connect():
    _require_db_config()
    import socket
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(8)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    finally:
        socket.setdefaulttimeout(old_timeout)
    return conn


def get_conn():
    global _CONN, _CONN_LAST_OK_TS, _LAST_PING_TS

    now = time.time()

    if _CONN is not None and getattr(_CONN, "closed", 1) != 0:
        close_quiet(_CONN)
        _CONN = None

    if _CONN is not None:
        too_old = (now - _CONN_LAST_OK_TS) > CONN_MAX_AGE_SEC
        if too_old:
            close_quiet(_CONN)
            _CONN = None

    if _CONN is not None and (now - _LAST_PING_TS) >= CONN_PING_EVERY_SEC:
        try:
            with _CONN.cursor() as c:
                c.execute("SELECT 1")
            rollback_quiet(_CONN)
            _LAST_PING_TS = now
            _CONN_LAST_OK_TS = now
        except Exception:
            close_quiet(_CONN)
            _CONN = None

    if _CONN is None:
        _CONN = db_connect()
        _CONN.autocommit = False
        _CONN_LAST_OK_TS = now
        _LAST_PING_TS = now

    return _CONN


def invalidate_conn():
    global _CONN
    close_quiet(_CONN)
    _CONN = None


def is_connection_error(exc):
    return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))


def end_request(conn):
    rollback_quiet(conn)


# ============================================================
# CORS
# ============================================================

def build_cors_headers():
    allow_auth = str(os.environ.get("CORS_ALLOW_AUTH", "true")).lower() == "true"
    allow_headers = "Content-Type,X-Customer-Id,X-Is-Superuser,X-User-Id,X-Username,X-User-Role"
    if allow_auth:
        allow_headers += ",Authorization"

    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": allow_headers,
        "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
        "Access-Control-Max-Age": "3600",
    }


def http_response(status, body):
    return {
        "statusCode": status,
        "headers": build_cors_headers(),
        "body": json.dumps(body, default=str),
    }


def file_response(status: int, content: str, filename: str, content_type: str = "text/csv; charset=utf-8"):
    return {
        "statusCode": status,
        "headers": {
            **build_cors_headers(),
            "Content-Type": content_type,
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
        "body": content,
    }


# ============================================================
# HELPERS
# ============================================================

def hash_password(password: str) -> str:
    return hashlib.md5(password.encode()).hexdigest()


def safe_lower(v):
    return str(v or "").strip().lower()


def get_header(event, name: str):
    headers = event.get("headers") or {}
    for k, v in headers.items():
        if k and k.lower() == name.lower():
            return v
    return None


def parse_bool(v, default=False):
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "t")


def get_user_context(event):
    params = event.get("queryStringParameters") or {}
    customer_id = get_header(event, "X-Customer-Id") or params.get("customer_id")
    is_superuser = get_header(event, "X-Is-Superuser")
    user_id = get_header(event, "X-User-Id") or params.get("user_id")
    username = get_header(event, "X-Username") or params.get("username")
    role = get_header(event, "X-User-Role") or params.get("role") or "viewer"
    return {
        "customer_id": int(customer_id) if customer_id and str(customer_id).isdigit() else None,
        "is_superuser": parse_bool(is_superuser, default=False),
        "user_id": int(user_id) if user_id and str(user_id).isdigit() else None,
        "username": str(username).strip() if username else None,
        "role_key": None,
        "permissions": {},
        "role": role,
    }


def normalize_permissions(raw):
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw) or {}
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def load_current_user(cur, ctx: dict):
    if not ctx.get("user_id") and not ctx.get("username"):
        return None

    if ctx.get("user_id"):
        cur.execute("""
            SELECT id, username, customer_id, is_superuser, is_active, role_key, permissions
            FROM public.app_user
            WHERE id = %(id)s
            AND is_active = true
            LIMIT 1;
        """, {"id": int(ctx["user_id"])})
    else:
        cur.execute("""
            SELECT id, username, customer_id, is_superuser, is_active, role_key, permissions
            FROM public.app_user
            WHERE lower(username) = lower(%(username)s)
            AND is_active = true
            LIMIT 1;
        """, {"username": ctx.get("username")})

    user = cur.fetchone()
    if not user:
        return None

    is_superuser = bool(user.get("is_superuser"))
    user_customer_id = user.get("customer_id")
    ctx_customer_id = ctx.get("customer_id")

    if not is_superuser:
        if user_customer_id is None:
            ctx["_auth_error"] = http_response(403, {"error": "customer_id ausente"})
            return None
        if ctx_customer_id is not None and int(ctx_customer_id) != int(user_customer_id):
            ctx["_auth_error"] = http_response(403, {"error": "customer_id incompativel"})
            return None
        ctx_customer_id = user_customer_id

    ctx.update({
        "customer_id": ctx_customer_id,
        "is_superuser": is_superuser,
        "user_id": user.get("id"),
        "username": user.get("username"),
        "role_key": user.get("role_key"),
        "permissions": normalize_permissions(user.get("permissions")),
    })
    return ctx


def require_current_user(cur, ctx: dict):
    loaded = load_current_user(cur, ctx)
    if loaded:
        return None
    return ctx.pop("_auth_error", None) or http_response(401, {"error": "usuario ausente"})


def has_perm(ctx, key):
    if ctx.get("is_superuser"):
        return True
    permissions = normalize_permissions(ctx.get("permissions") or {})
    return permissions.get(key) is True


def get_allowed_plant_ids(ctx: dict):
    perms = normalize_permissions(ctx.get("permissions") or {})
    ids = perms.get("allowed_plant_ids")
    if ids and isinstance(ids, list):
        return [int(i) for i in ids]
    return None


def is_plant_allowed(ctx: dict, plant_id: int) -> bool:
    if ctx.get("is_superuser"):
        return True
    ids = get_allowed_plant_ids(ctx)
    if ids is None:
        return True
    return int(plant_id) in ids


def plant_filter_sql(ctx: dict, alias: str = "p"):
    ids = get_allowed_plant_ids(ctx)
    if ids is None:
        return sql.SQL("")
    return sql.SQL(" AND {}.power_plant_id IN ({})").format(
        sql.Identifier(alias),
        sql.SQL(",").join(sql.Literal(i) for i in ids)
    )


def can_edit_plant(ctx: dict) -> bool:
    return (
        ctx.get("is_superuser", False)
        or has_perm(ctx, "admin_customer")
        or has_perm(ctx, "plant_edit")
    )

def can_edit_device(ctx: dict) -> bool:
    return (
        ctx.get("is_superuser", False)
        or has_perm(ctx, "admin_customer")
        or has_perm(ctx, "device_edit")
    )

def can_edit_string_config(ctx: dict) -> bool:
    return (
        ctx.get("is_superuser", False)
        or has_perm(ctx, "admin_customer")
        or has_perm(ctx, "device_edit")
        or has_perm(ctx, "string_config_edit")
    )

def can_send_command(ctx: dict) -> bool:
    return (
        ctx.get("is_superuser", False)
        or has_perm(ctx, "admin_customer")
        or has_perm(ctx, "remote_command")
        or has_perm(ctx, "device_command")
    )


def parse_json_body(event):
    raw = event.get("body")
    if not raw:
        return {}
    try:
        if event.get("isBase64Encoded"):
            import base64
            raw = base64.b64decode(raw).decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


def normalize_path(event):
    """
    Suporta HTTP API (rawPath) e REST API (path).
    Remove prefixo de stage se vier tipo /prod/...
    """
    path = event.get("rawPath") or event.get("path") or ""
    if not isinstance(path, str):
        path = ""
    path = path.split("?")[0]

    rc = event.get("requestContext") or {}
    stage = rc.get("stage")
    if stage and path.startswith(f"/{stage}/"):
        path = path[len(stage) + 1:]
        if not path.startswith("/"):
            path = "/" + path
    return path


def get_method(event):
    rc = event.get("requestContext") or {}
    http = rc.get("http") or {}
    method = http.get("method") or event.get("httpMethod") or ""
    return str(method).upper()


def is_path(path: str, suffix: str) -> bool:
    return (path or "").endswith(suffix)


def path_contains(path: str, piece: str) -> bool:
    return piece in (path or "")


# ============================================================
# EXTRAÇÃO DE IDS (HTTP API ANY /{proxy+})
# ============================================================

def extract_ids_from_path(path: str):
    out = {"plant_id": None, "device_id": None, "inverter_id": None, "string_index": None}
    if not path:
        return out

    m = re.search(r"/plants/(\d+)", path)
    if m:
        out["plant_id"] = m.group(1)

    m = re.search(r"/inverters/(\d+)", path)
    if m:
        out["inverter_id"] = m.group(1)

    m = re.search(r"/devices/(\d+)", path)
    if m:
        out["device_id"] = m.group(1)

    m = re.search(r"/strings/(\d+)$", path)
    if m:
        out["string_index"] = m.group(1)

    return out


# ============================================================
# CONTROLE DE ACESSO / TENANT
# ============================================================

def ensure_plant_access(cur, plant_id: int, ctx: dict):
    if not is_plant_allowed(ctx, int(plant_id)):
        return False
    cur.execute("""
        SELECT 1
        FROM public.power_plant
        WHERE id = %(plant_id)s
        AND ( %(is_superuser)s = true OR customer_id = %(customer_id)s );
    """, {
        "plant_id": int(plant_id),
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"]
    })
    return cur.fetchone() is not None


def resolve_customer_id_for_plant(cur, plant_id: int, ctx: dict):
    if ctx.get("is_superuser"):
        cur.execute("SELECT customer_id FROM public.power_plant WHERE id = %s", (int(plant_id),))
        row = cur.fetchone()
        if row and row.get("customer_id") is not None:
            return int(row["customer_id"])
        return None
    if ctx.get("customer_id"):
        return int(ctx["customer_id"])
    return None


def validate_operational_user(cur, username: str, password: str, ctx: dict):
    if not username or not password:
        return None

    cur.execute("""
        SELECT
            id,
            username,
            password_hash,
            customer_id,
            is_superuser,
            is_active,
            role_key,
            permissions
        FROM public.app_user
        WHERE username = %(username)s
        LIMIT 1;
    """, {"username": username})
    user = cur.fetchone()
    if not user or not user.get("is_active"):
        return None

    if hash_password(password) != user.get("password_hash"):
        return None

    if not ctx.get("is_superuser"):
        if ctx.get("customer_id") is None or int(user.get("customer_id") or -1) != int(ctx["customer_id"]):
            return None

    user["permissions"] = normalize_permissions(user.get("permissions"))
    return user


def resolve_device_command_target(cur, plant_id: int, device_id: int, ctx: dict):
    cur.execute("""
        SELECT
        d.id AS device_id,
        d.name AS device_name,
        d.power_plant_id,
        p.name AS power_plant_name,
        p.customer_id,
        dt.name AS device_type
        FROM public.device d
        JOIN public.device_type dt ON dt.id = d.device_type_id
        JOIN public.power_plant p ON p.id = d.power_plant_id
        WHERE d.id = %(device_id)s
        AND d.power_plant_id = %(plant_id)s
        AND d.is_active = true
        AND ( %(is_superuser)s = true OR p.customer_id = %(customer_id)s )
        LIMIT 1;
    """, {
        "device_id": int(device_id),
        "plant_id": int(plant_id),
        "customer_id": ctx.get("customer_id"),
        "is_superuser": ctx.get("is_superuser", False),
    })
    return cur.fetchone()


def infer_device_index(device_name: str, default_index: int = 1) -> int:
    s = str(device_name or "").strip()
    m = re.search(r"(\d+)\s*$", s)
    if not m:
        return int(default_index)
    try:
        return int(m.group(1))
    except Exception:
        return int(default_index)


def sanitize_topic_part(value: str) -> str:
    s = str(value or "").strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^A-Za-z0-9_-]", "", s)
    return s or "unknown"


def normalize_command_device_type(device_type: str) -> str:
    dt = safe_lower(device_type)
    if dt in ("rele", "relé", "relé de proteção", "rele de protecao", "relay"):
        return "relay"
    if dt in ("inversor", "inverter"):
        return "inverter"
    if dt in ("multimeter", "multimedidor", "medidor", "meter"):
        return "meter"
    if dt in ("tracker", "tcu", "rsu"):
        return dt
    return sanitize_topic_part(device_type)


def build_device_command_topic(power_plant_name: str, device_type: str, device_index: int) -> str:
    plant = sanitize_topic_part(power_plant_name)
    dtype = normalize_command_device_type(device_type)
    idx = int(device_index) if str(device_index).isdigit() else 1
    return f"{MQTT_COMMAND_PREFIX}/UFV/{plant}/{dtype}/{idx}"


def build_device_command_payload(*, action: str, target: dict, requested_by: str, value=None, command_id=None):
    payload = {
        "action": action,
        "device_id": int(target["device_id"]),
        "device_type": target.get("device_type"),
        "power_plant_id": int(target["power_plant_id"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "requested_by": requested_by,
    }
    if value is not None:
        payload["value_kw"] = float(value)
    if command_id is not None:
        payload["command_id"] = int(command_id)  # CLP usa para correlacionar feedback
    return payload


def insert_device_command_audit(cur, *, customer_id, target: dict, action: str, mqtt_topic: str, command_payload: dict, requested_by: str, requested_username: str, value_kw=None):
    cur.execute("""
        INSERT INTO public.device_command (
        customer_id,
        power_plant_id,
        device_id,
        device_type,
        action,
        mqtt_topic,
        command_payload,
        requested_by,
        requested_username,
        value_kw,
        status,
        created_at
        ) VALUES (
        %(customer_id)s,
        %(power_plant_id)s,
        %(device_id)s,
        %(device_type)s,
        %(action)s,
        %(mqtt_topic)s,
        %(command_payload)s::jsonb,
        %(requested_by)s,
        %(requested_username)s,
        %(value_kw)s,
        'PENDING',
        now()
        )
        RETURNING id;
    """, {
        "customer_id": customer_id,
        "power_plant_id": int(target["power_plant_id"]),
        "device_id": int(target["device_id"]),
        "device_type": target.get("device_type"),
        "action": action,
        "mqtt_topic": mqtt_topic,
        "command_payload": json.dumps(command_payload, ensure_ascii=False),
        "requested_by": requested_by,
        "requested_username": requested_username,
        "value_kw": float(value_kw) if value_kw is not None else None,
    })
    row = cur.fetchone() or {}
    return int(row.get("id"))


def update_device_command_audit(cur, command_id: int, *, status: str, status_message=None, response_payload=None, set_started=False, set_finished=False):
    cur.execute("""
        UPDATE public.device_command
        SET
        status = %(status)s,
        status_message = %(status_message)s,
        response_payload = COALESCE(%(response_payload)s::jsonb, response_payload),
        started_at = CASE WHEN %(set_started)s THEN now() ELSE started_at END,
        finished_at = CASE WHEN %(set_finished)s THEN now() ELSE finished_at END
        WHERE id = %(command_id)s;
    """, {
        "status": status,
        "status_message": status_message,
        "response_payload": json.dumps(response_payload, ensure_ascii=False) if response_payload is not None else None,
        "set_started": bool(set_started),
        "set_finished": bool(set_finished),
        "command_id": int(command_id),
    })


def get_iot_data_endpoint():
    """
    Descobre o endpoint do AWS IoT Data Plane.
    Se existir env var IOT_DATA_ENDPOINT, usa ela.
    Senão, resolve via AWS IoT DescribeEndpoint.
    """
    global _IOT_DATA_ENDPOINT

    if _IOT_DATA_ENDPOINT:
        return _IOT_DATA_ENDPOINT

    env_ep = os.getenv("IOT_DATA_ENDPOINT")
    if env_ep:
        _IOT_DATA_ENDPOINT = env_ep.strip()
        return _IOT_DATA_ENDPOINT

    iot = boto3.client(
        "iot",
        config=Config(connect_timeout=5, read_timeout=8, retries={"max_attempts": 1, "mode": "standard"}),
    )
    resp = iot.describe_endpoint(endpointType="iot:Data-ATS")
    _IOT_DATA_ENDPOINT = resp["endpointAddress"]
    return _IOT_DATA_ENDPOINT


def get_iot_data_client():
    global _IOT_DATA_CLIENT

    if _IOT_DATA_CLIENT is not None:
        return _IOT_DATA_CLIENT

    endpoint = get_iot_data_endpoint()

    _IOT_DATA_CLIENT = boto3.client(
        "iot-data",
        endpoint_url=f"https://{endpoint}",
        config=Config(
            retries={"max_attempts": 2, "mode": "standard"},
            connect_timeout=5,
            read_timeout=10,
        )
    )
    return _IOT_DATA_CLIENT


def publish_device_command(*, mqtt_topic: str, payload: dict):
    """
    Publica comando real no AWS IoT Core Data Plane.
    """
    client = get_iot_data_client()

    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    try:
        resp = client.publish(
            topic=mqtt_topic,
            qos=1,
            payload=payload_bytes
        )

        return {
            "ok": True,
            "provider": "aws-iot-core",
            "mqtt_topic": mqtt_topic,
            "published_at": datetime.utcnow().isoformat() + "Z",
            "payload": payload,
            "response_metadata": resp.get("ResponseMetadata", {})
        }

    except (ClientError, BotoCoreError) as e:
        raise Exception(f"Falha no publish IoT Core: {str(e)}")


# -------------------------
# TIME PARSING
# -------------------------

def parse_time_to_dt(s: str):
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None

    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        try:
            return datetime.fromisoformat(s)
        except Exception:
            pass

        if "." in s:
            left, right = s.split(".", 1)
            if "+" in right:
                _, off = right.split("+", 1)
                s2 = f"{left}+{off}"
                try:
                    return datetime.fromisoformat(s2)
                except Exception:
                    pass
            elif "-" in right and right.count(":") >= 1:
                _, off = right.split("-", 1)
                s2 = f"{left}-{off}"
                try:
                    return datetime.fromisoformat(s2)
                except Exception:
                    pass
            else:
                s2 = left
                try:
                    return datetime.fromisoformat(s2)
                except Exception:
                    pass

        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass

        return None
    except Exception:
        return None


def get_pvsyst_expected_day_kwh(cur, plant_id: int, ref_date):
    if not plant_has_active_pvsyst(cur, plant_id):
        return None

    cur.execute("""
        SELECT
            e.expected_day_kwh::double precision AS expected_day_kwh
        FROM public.pvsyst_expected_daily e
        WHERE e.power_plant_id = %(plant_id)s
        AND EXTRACT(month FROM e.date_day) = %(month)s
        AND EXTRACT(day FROM e.date_day) = %(day)s
        LIMIT 1;
    """, {
        "plant_id": int(plant_id),
        "month": int(ref_date.month),
        "day": int(ref_date.day),
    })
    row = cur.fetchone()
    if not row:
        return None
    return float(row.get("expected_day_kwh") or 0.0)


def build_intraday_shape(labels_hhmm):
    weights = []

    for hhmm in labels_hhmm or []:
        try:
            hh, mm = map(int, str(hhmm).split(":"))
            minutes = hh * 60 + mm
        except Exception:
            weights.append(0.0)
            continue

        if minutes < 5 * 60 or minutes > 18 * 60:
            weights.append(0.0)
            continue

        center = 11.5 * 60
        spread = 210.0
        x = (minutes - center) / spread
        w = max(0.0, 1.0 - (x * x))
        weights.append(w)

    total = sum(weights)
    if total <= 0:
        return [0.0 for _ in weights]

    return [w / total for w in weights]


def infer_step_minutes(labels):
    mins = []
    for s in labels or []:
        try:
            hh, mm = map(int, str(s).split(":"))
            mins.append(hh * 60 + mm)
        except Exception:
            continue

    if len(mins) < 2:
        return 5

    diffs = []
    for i in range(1, len(mins)):
        d = mins[i] - mins[i - 1]
        if d > 0 and d <= 60:
            diffs.append(d)

    return min(diffs) if diffs else 5


def build_full_day_labels(step_minutes: int, start_hour: int = 5, end_hour: int = 18):
    labels = []
    step = int(step_minutes or 5)
    if step <= 0:
        step = 5

    start_min = start_hour * 60
    end_min = end_hour * 60

    m = start_min
    while m <= end_min:
        hh = m // 60
        mm = m % 60
        labels.append(f"{hh:02d}:{mm:02d}")
        m += step

    return labels


def expected_kwh_to_power_curve(expected_day_kwh, labels_hhmm, step_minutes):
    if not labels_hhmm:
        return []

    weights = build_intraday_shape(labels_hhmm)
    hours_per_step = step_minutes / 60.0 if step_minutes and step_minutes > 0 else 0.0

    if hours_per_step <= 0:
        return [0.0 for _ in labels_hhmm]

    expected_power = []

    for w in weights:
        expected_energy_slot = float(expected_day_kwh or 0.0) * w
        kw = expected_energy_slot / hours_per_step

        expected_power.append(round(kw, 2))

    return expected_power


def expected_power_curve_kw(rated_power_kw, labels_hhmm):
    if not labels_hhmm:
        return []

    rated = float(rated_power_kw or 0.0)
    if rated <= 0:
        return [0.0 for _ in labels_hhmm]

    shape = []
    for hhmm in labels_hhmm or []:
        try:
            hh, mm = map(int, str(hhmm).split(":"))
            minutes = hh * 60 + mm
        except Exception:
            shape.append(0.0)
            continue

        if minutes < 5 * 60 or minutes > 18 * 60:
            shape.append(0.0)
            continue

        center = 11.5 * 60
        spread = 210.0
        x = (minutes - center) / spread
        shape.append(max(0.0, 1.0 - (x * x)))

    max_shape = max(shape) if shape else 0.0
    if max_shape <= 0:
        return [0.0 for _ in labels_hhmm]

    return [round(rated * (v / max_shape), 2) for v in shape]


def plant_has_active_pvsyst(cur, plant_id: int):
    cur.execute("""
        SELECT 1
        FROM public.pvsyst_simulation s
        WHERE s.power_plant_id = %(plant_id)s
        AND COALESCE(s.is_active, false) = true
        LIMIT 1;
    """, {
        "plant_id": int(plant_id),
    })
    return cur.fetchone() is not None


def get_monthly_real_and_expected(cur, plant_id: int, month_start, month_end):
    has_expected = plant_has_active_pvsyst(cur, plant_id)

    if not has_expected:
        cur.execute(sql.SQL("""
            WITH real_daily AS (
                SELECT
                    d.date_day::date AS day,
                    COALESCE(d.generation_daily_kwh, 0)::double precision AS real_kwh,
                    COALESCE(d.irradiation_daily_kwh_m2, 0)::double precision AS irradiation_daily_kwh_m2
                FROM {fct_power_plant_metrics_daily} d
                WHERE d.power_plant_id = %(plant_id)s
                AND d.date_day >= %(month_start)s::date
                AND d.date_day <= %(month_end)s::date
            )
            SELECT
                gs.day::date AS day,
                to_char(gs.day::date, 'DD') AS label,
                COALESCE(r.real_kwh, 0) AS real_kwh,
                COALESCE(r.irradiation_daily_kwh_m2, 0) AS irradiation_daily_kwh_m2,
                NULL::double precision AS expected_kwh
            FROM generate_series(%(month_start)s::date, %(month_end)s::date, interval '1 day') AS gs(day)
            LEFT JOIN real_daily r ON r.day = gs.day
            ORDER BY gs.day;
        """).format(fct_power_plant_metrics_daily=q(RT_SCHEMA, "fct_power_plant_metrics_daily")), {
            "plant_id": int(plant_id),
            "month_start": month_start,
            "month_end": month_end,
        })
        return cur.fetchall() or [], False

    cur.execute(sql.SQL("""
        WITH real_daily AS (
            SELECT
                d.date_day::date AS day,
                COALESCE(d.generation_daily_kwh, 0)::double precision AS real_kwh,
                COALESCE(d.irradiation_daily_kwh_m2, 0)::double precision AS irradiation_daily_kwh_m2
            FROM {fct_power_plant_metrics_daily} d
            WHERE d.power_plant_id = %(plant_id)s
            AND d.date_day >= %(month_start)s::date
            AND d.date_day <= %(month_end)s::date
        ),
        expected_daily AS (
            SELECT
                e.date_day::date AS ref_day,
                COALESCE(e.expected_day_kwh, 0)::double precision AS expected_kwh
            FROM public.pvsyst_expected_daily e
            WHERE e.power_plant_id = %(plant_id)s
            AND EXTRACT(month FROM e.date_day) = EXTRACT(month FROM %(month_start)s::date)
        )
        SELECT
            gs.day::date AS day,
            to_char(gs.day::date, 'DD') AS label,
            COALESCE(r.real_kwh, 0) AS real_kwh,
            COALESCE(r.irradiation_daily_kwh_m2, 0) AS irradiation_daily_kwh_m2,
            COALESCE(ed.expected_kwh, 0) AS expected_kwh
        FROM generate_series(%(month_start)s::date, %(month_end)s::date, interval '1 day') AS gs(day)
        LEFT JOIN real_daily r ON r.day = gs.day
        LEFT JOIN expected_daily ed
        ON EXTRACT(month FROM ed.ref_day) = EXTRACT(month FROM gs.day)
        AND EXTRACT(day FROM ed.ref_day) = EXTRACT(day FROM gs.day)
        ORDER BY gs.day;
    """).format(fct_power_plant_metrics_daily=q(RT_SCHEMA, "fct_power_plant_metrics_daily")), {
        "plant_id": int(plant_id),
        "month_start": month_start,
        "month_end": month_end,
    })
    return cur.fetchall() or [], True


def build_monthly_expected_payload(rows):
    labels = []
    daily_kwh = []
    expected_daily_kwh = []
    mtd_kwh = []
    expected_mtd_kwh = []
    irradiation_daily_kwh_m2 = []

    acc_real = 0.0
    acc_expected = 0.0
    has_any_expected = False

    for row in rows or []:
        real_val = float(row.get("real_kwh") or 0.0)
        raw_expected = row.get("expected_kwh")
        expected_val = float(raw_expected) if raw_expected is not None else None
        irr_val = float(row.get("irradiation_daily_kwh_m2") or 0.0)

        acc_real += real_val
        if expected_val is not None:
            has_any_expected = True
            acc_expected += expected_val

        labels.append(row.get("label"))
        daily_kwh.append(round(real_val, 2))
        expected_daily_kwh.append(round(expected_val, 2) if expected_val is not None else None)
        mtd_kwh.append(round(acc_real, 2))
        expected_mtd_kwh.append(round(acc_expected, 2) if expected_val is not None else None)
        irradiation_daily_kwh_m2.append(round(irr_val, 2))

    if not has_any_expected:
        expected_daily_kwh = []
        expected_mtd_kwh = []

    return {
        "labels": labels,
        "daily_kwh": daily_kwh,
        "expected_daily_kwh": expected_daily_kwh,
        "mtd_kwh": mtd_kwh,
        "expected_mtd_kwh": expected_mtd_kwh,
        "irradiation_daily_kwh_m2": irradiation_daily_kwh_m2,
    }


# ============================================================
# ONLINE WINDOWS
# ============================================================

INVERTER_ONLINE_WINDOW = os.getenv("INVERTER_ONLINE_WINDOW", "25 minutes")
STRING_ONLINE_WINDOW = os.getenv("STRING_ONLINE_WINDOW", "25 minutes")
RELAY_ONLINE_WINDOW = os.getenv("RELAY_ONLINE_WINDOW", "25 minutes")
MULTIMETER_ONLINE_WINDOW = os.getenv("MULTIMETER_ONLINE_WINDOW", "25 minutes")
TRACKER_ONLINE_WINDOW = os.getenv("TRACKER_ONLINE_WINDOW", "25 minutes")


# ============================================================
# INTROSPECÇÃO / IDENTIFIERS SEGUROS
# ============================================================

def get_table_columns(cur, schema: str, table: str):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %(schema)s
        AND table_name   = %(table)s
        ORDER BY ordinal_position;
    """, {"schema": schema, "table": table})
    return {r["column_name"] for r in (cur.fetchall() or [])}


def q(schema_name: str, table_name: str):
    return sql.Identifier(schema_name, table_name)


_MART_COLS_CACHE = {}
_MART_COLS_CACHE_TS = {}
_MART_COLS_TTL_SEC = int(os.getenv("MART_COLS_TTL_SEC", "300"))


def get_mart_cols_cached(cur, schema_name: str, table_name: str):
    cache_key = (schema_name, table_name)
    now = time.time()
    cache_ts = _MART_COLS_CACHE_TS.get(cache_key, 0)

    if cache_key in _MART_COLS_CACHE and (now - cache_ts) < _MART_COLS_TTL_SEC:
        return _MART_COLS_CACHE[cache_key]

    cols = get_table_columns(cur, schema_name, table_name)
    _MART_COLS_CACHE[cache_key] = cols
    _MART_COLS_CACHE_TS[cache_key] = now
    return cols


# ============================================================
# RELAY REALTIME
# ============================================================

# (conteúdo salvo - primeira parte)
# Continuação será anexada na próxima mensagem.


# ============================================================
# DATA STUDIO - MAPEAMENTO DE ROTAS DE CONSULTA
# ============================================================

DATASTUDIO_INTRADAY_PATHS = {
    "PLANT.active_power_kw": {
        "column": "active_power_kw",
        "unit": "kW",
        "data_kind": "analog",
        "source": "historico",
    },
    "PLANT.irradiance_ghi_wm2": {
        "column": "irradiance_ghi_wm2",
        "unit": "W/m²",
        "data_kind": "analog",
        "source": "historico",
    },
    "PLANT.irradiance_poa_wm2": {
        "column": "irradiance_poa_wm2",
        "unit": "W/m²",
        "data_kind": "analog",
        "source": "historico",
    },
    "PLANT.inverter_count_ok": {
        "column": "inverter_count_ok",
        "unit": "count",
        "data_kind": "discrete",
        "source": "historico",
    },
    "PLANT.inverter_count_fault": {
        "column": "inverter_count_fault",
        "unit": "count",
        "data_kind": "discrete",
        "source": "historico",
    },
    "PLANT.inverter_count_null": {
        "column": "inverter_count_null",
        "unit": "count",
        "data_kind": "discrete",
        "source": "historico",
    },
}

DATASTUDIO_DAILY_PATHS = {
    "PLANT.energy_real_kwh_daily": {
        "column": "energy_real_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.irradiance_kwh_m2_daily": {
        "column": "irradiance_kwh_m2",
        "unit": "kWh/m²",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.energy_theoretical_kwh_daily": {
        "column": "energy_theoretical_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.performance_ratio_daily": {
        "column": "performance_ratio",
        "unit": "%",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.capacity_dc": {
        "column": "capacity_dc",
        "unit": "kWp",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.energy_export_kwh_daily": {
        "column": "energy_export_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.energy_import_kwh_daily": {
        "column": "energy_import_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.energy_liquid_meter_kwh_daily": {
        "column": "energy_liquid_meter_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
}

DATASTUDIO_MONTHLY_PATHS = {
    "PLANT.energy_kwh_monthly": {
        "column": "energy_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.energy_export_kwh_monthly": {
        "column": "energy_export_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.energy_import_kwh_monthly": {
        "column": "energy_import_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
    "PLANT.energy_liquid_meter_kwh_monthly": {
        "column": "energy_liquid_meter_kwh",
        "unit": "kWh",
        "data_kind": "analog",
        "source": "consolidado",
    },
}


def datastudio_daily_rollup_strategy(pathname: str):
    """
    Define como agregamos métricas diárias quando subimos para weekly.
    Regra prática:
    - energia / irradiância diária / energia teórica -> soma
    - PR / capacity -> média
    """
    if pathname in (
        "PLANT.energy_real_kwh_daily",
        "PLANT.irradiance_kwh_m2_daily",
        "PLANT.energy_theoretical_kwh_daily",
        "PLANT.energy_export_kwh_daily",
        "PLANT.energy_import_kwh_daily",
        "PLANT.energy_liquid_meter_kwh_daily",
    ):
        return "sum"

    if pathname in (
        "PLANT.performance_ratio_daily",
        "PLANT.capacity_dc",
    ):
        return "avg"

    return "avg"


def datastudio_monthly_rollup_strategy(pathname: str):
    """
    Define como agregamos métricas mensais quando subimos para yearly.
    """
    if pathname in (
        "PLANT.energy_kwh_monthly",
        "PLANT.energy_export_kwh_monthly",
        "PLANT.energy_import_kwh_monthly",
        "PLANT.energy_liquid_meter_kwh_monthly",
    ):
        return "sum"

    return "avg"


def datastudio_consolidado_15min_value_column(pathname: str):
    """
    Para o '5min consolidado' vamos usar a tabela de 15min.
    Como são grandezas instantâneas de planta, usamos avg_value.
    """
    return "avg_value"


def datastudio_resolve_source_type(pathname: str, effective_source: str = None):
    source = safe_lower(effective_source)

    if pathname in DATASTUDIO_DAILY_PATHS:
        return "daily"

    if pathname in DATASTUDIO_MONTHLY_PATHS:
        return "monthly"

    if pathname in DATASTUDIO_INTRADAY_PATHS:
        return "historico"

    if source == "historico":
        return "historico"

    if source == "consolidado":
        return "consolidado"

    return "timeseries"


def datastudio_get_default_metadata(pathname: str):
    if pathname in DATASTUDIO_INTRADAY_PATHS:
        return DATASTUDIO_INTRADAY_PATHS[pathname]
    if pathname in DATASTUDIO_DAILY_PATHS:
        return DATASTUDIO_DAILY_PATHS[pathname]
    if pathname in DATASTUDIO_MONTHLY_PATHS:
        return DATASTUDIO_MONTHLY_PATHS[pathname]
    return {
        "column": None,
        "unit": None,
        "data_kind": None,
        "source": None,
    }


def datastudio_range_days(start_ts, end_ts):
    if not start_ts or not end_ts:
        return 0.0
    try:
        delta = end_ts - start_ts
        return max(0.0, delta.total_seconds() / 86400.0)
    except Exception:
        return 0.0


def datastudio_choose_hist_table(start_ts, end_ts):
    days = datastudio_range_days(start_ts, end_ts)

    if days <= 2:
        return "mart_datastudio_hist_5min", "hist_5min"
    if days <= 7:
        return "mart_datastudio_hist_15min", "hist_15min"
    if days <= 15:
        return "mart_datastudio_hist_hourly", "hist_hourly"
    return "mart_datastudio_hist_daily", "hist_daily"


def datastudio_hist_agg_column(aggregation: str, pathname: str = ""):
    agg = safe_lower(aggregation)

    if agg in ("sum", "soma"):
        if pathname and not _is_summable_pathname(pathname):
            return "avg_value"
        return "sum_value"

    if agg in ("max", "maxima", "máxima"):
        return "max_value"

    return "avg_value"


def fetch_datastudio_points(cur, *, customer_id: int, power_plant_id: int, pathname: str,
                            start_ts, end_ts, effective_source: str,
                            effective_aggregation: str = None,
                            limit: int = 5000):
    route_type = datastudio_resolve_source_type(pathname, effective_source)

    if route_type == "historico":
        hist_table_name, resolved_hist_route = datastudio_choose_hist_table(start_ts, end_ts)
        value_col = datastudio_hist_agg_column(effective_aggregation, pathname)

        query = sql.SQL("""
            SELECT
                ts,
                {value_col} AS value
            FROM {tbl}
            WHERE customer_id = %(customer_id)s
            AND power_plant_id = %(power_plant_id)s
            AND pathname = %(pathname)s
            AND ts >= %(start_ts)s
            AND ts <= %(end_ts)s
            ORDER BY ts
            LIMIT %(limit)s;
        """).format(
            value_col=sql.Identifier(value_col),
            tbl=q(ANALYTICS_SCHEMA, hist_table_name)
        )

        cur.execute(query, {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "pathname": pathname,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], resolved_hist_route

    if route_type == "daily":
        meta = DATASTUDIO_DAILY_PATHS[pathname]
        query = sql.SQL("""
            SELECT
                date::timestamptz AS ts,
                {value_col} AS value
            FROM {tbl}
            WHERE customer_id = %(customer_id)s
            AND power_plant_id = %(power_plant_id)s
            AND date::timestamptz >= %(start_ts)s
            AND date::timestamptz <= %(end_ts)s
            ORDER BY date
            LIMIT %(limit)s;
        """).format(
            value_col=sql.Identifier(meta["column"]),
            tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_daily")
        )
        cur.execute(query, {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], route_type

    if route_type == "monthly":
        meta = DATASTUDIO_MONTHLY_PATHS[pathname]
        query = sql.SQL("""
            SELECT
                month::timestamptz AS ts,
                {value_col} AS value
            FROM {tbl}
            WHERE customer_id = %(customer_id)s
            AND power_plant_id = %(power_plant_id)s
            AND month::timestamptz >= %(start_ts)s
            AND month::timestamptz <= %(end_ts)s
            ORDER BY month
            LIMIT %(limit)s;
        """).format(
            value_col=sql.Identifier(meta["column"]),
            tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_monthly")
        )
        cur.execute(query, {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], route_type

    if route_type == "consolidado":
        return [], "consolidado_unsupported"

    query = sql.SQL("""
        SELECT
            ts,
            value
        FROM {tbl}
        WHERE customer_id = %(customer_id)s
        AND power_plant_id = %(power_plant_id)s
        AND pathname = %(pathname)s
        AND ts >= %(start_ts)s
        AND ts <= %(end_ts)s
        AND (%(source)s IS NULL OR source = %(source)s)
        ORDER BY ts
        LIMIT %(limit)s;
    """).format(
        tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_timeseries")
    )
    cur.execute(query, {
        "customer_id": customer_id,
        "power_plant_id": power_plant_id,
        "pathname": pathname,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "source": effective_source,
        "limit": limit,
    })
    return cur.fetchall() or [], route_type
# ============================================================
# RELAY REALTIME
# ============================================================

def resolve_latest_relay_device(cur, plant_id: int, ctx: dict):
    cur.execute("""
        SELECT
        d.id AS device_id,
        d.name AS device_name,
        NULL::timestamptz AS last_ts
        FROM public.device d
        JOIN public.device_type dt ON dt.id = d.device_type_id
        JOIN public.power_plant p ON p.id = d.power_plant_id
        WHERE d.power_plant_id = %(plant_id)s
        AND d.is_active = true
        AND LOWER(dt.name) IN ('relay', 'rele', 'relé')
        AND ( %(is_superuser)s = true OR p.customer_id = %(customer_id)s )
        ORDER BY d.id
        LIMIT 1;
    """, {
        "plant_id": int(plant_id),
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"]
    })
    return cur.fetchone()


def handle_get_relay_realtime(cur, plant_id: int, ctx: dict):
    cur.execute("SET LOCAL statement_timeout = '12000ms';")

    if not ensure_plant_access(cur, int(plant_id), ctx):
        return http_response(403, {"error": "sem permissão para esta usina"})

    latest = resolve_latest_relay_device(cur, plant_id, ctx)
    if not latest:
        return http_response(200, {
            "item": None,
            "meta": {
                "event_source": f"{RT_SCHEMA}.stg_relay_event",
                "analog_source": f"{RT_SCHEMA}.stg_relay_analog",
            }
        })

    relay_device_id = int(latest["device_id"])
    relay_device_name = latest.get("device_name")

    cur.execute(sql.SQL("""
        SELECT
        e.timestamp AS last_event_ts,
        e.device_id,
        e.discrete_data_json,
        EXTRACT(EPOCH FROM (now() - e.timestamp))::int AS age_seconds_event,
        (now() - e.timestamp <= interval %(online_window)s) AS is_online_event
        FROM {int_relay_event} e
        JOIN public.power_plant p ON p.id = e.power_plant_id
        WHERE e.power_plant_id = %(plant_id)s
        AND e.device_id = %(device_id)s
        AND e.timestamp >= now() - interval '1 hour'
        AND ( %(is_superuser)s = true OR p.customer_id = %(customer_id)s )
        ORDER BY e.timestamp DESC
        LIMIT 1;
    """).format(int_relay_event=q(RT_SCHEMA, "stg_relay_event")), {
        "online_window": RELAY_ONLINE_WINDOW,
        "plant_id": int(plant_id),
        "device_id": relay_device_id,
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"]
    })
    ev = cur.fetchone()

    cur.execute(sql.SQL("""
        SELECT
        a."timestamp" AS last_analog_ts,
        a.device_id,
        a.active_power_kw,
        a.apparent_power_kva,
        a.reactive_power_kvar,
        a.voltage_ab_v,
        a.voltage_bc_v,
        a.voltage_ca_v,
        a.current_a_a,
        a.current_b_a,
        a.current_c_a,
        EXTRACT(EPOCH FROM (now() - a."timestamp"))::int AS age_seconds_analog,
        (now() - a."timestamp" <= interval %(online_window)s) AS is_online_analog
        FROM {stg_relay_analog} a
        WHERE a.power_plant_id = %(plant_id)s
        AND a.device_id = %(device_id)s
        AND a."timestamp" >= now() - interval '1 hour'
        ORDER BY a."timestamp" DESC
        LIMIT 1;
    """).format(stg_relay_analog=q(RT_SCHEMA, "stg_relay_analog")), {
        "online_window": RELAY_ONLINE_WINDOW,
        "plant_id": int(plant_id),
        "device_id": relay_device_id,
    })
    an = cur.fetchone()

    last_ts = None
    online = False
    age_seconds = None

    if ev and ev.get("last_event_ts"):
        last_ts = ev["last_event_ts"]
        online = bool(ev.get("is_online_event"))
        age_seconds = ev.get("age_seconds_event")

    if an and an.get("last_analog_ts"):
        if (not last_ts) or (an["last_analog_ts"] and an["last_analog_ts"] > last_ts):
            last_ts = an["last_analog_ts"]
            online = bool(an.get("is_online_analog"))
            age_seconds = an.get("age_seconds_analog")

    relay_on = None
    is_valid = None
    communication_fault = None
    event_payload = {}
    if ev:
        raw_json = ev.get("discrete_data_json")
        if isinstance(raw_json, str):
            try:
                event_payload = json.loads(raw_json) or {}
            except Exception:
                event_payload = {}
        elif isinstance(raw_json, dict):
            event_payload = raw_json

        raw_comm_fault = event_payload.get("communication_fault")
        if raw_comm_fault is not None:
            raw_comm_fault = str(raw_comm_fault).strip()
            try:
                communication_fault = int(raw_comm_fault)
            except Exception:
                communication_fault = None
            is_valid = raw_comm_fault == "192"
        else:
            is_valid = None

        raw_relay = event_payload.get("relay_on", event_payload.get("status_relay", event_payload.get("event_value")))
        if raw_relay is not None:
            relay_on = str(raw_relay).strip() in ("1", "true", "True", "on", "ON")

    active_power_kw = None
    if an and an.get("active_power_kw") is not None:
        try:
            active_power_kw = float(an["active_power_kw"])
        except Exception:
            active_power_kw = None

    item = {
        "power_plant_id": int(plant_id),
        "device_id": relay_device_id,
        "device_name": relay_device_name,
        "device_type": "relay",
        "last_update": last_ts,
        "age_seconds": age_seconds,
        "is_online": bool(online),
        "relay_on": relay_on,
        "is_valid": is_valid,
        "communication_fault": communication_fault,
        "event": {
            "timestamp": ev.get("last_event_ts") if ev else None,
            "event_code": event_payload.get("event_code"),
            "event_name": event_payload.get("event_name"),
            "event_type": event_payload.get("event_type"),
            "severity": event_payload.get("severity"),
            "event_value": event_payload.get("event_value"),
            "communication_fault": event_payload.get("communication_fault"),
            "raw": event_payload if ev else None,
        },
        "analog": {
            "timestamp": an.get("last_analog_ts") if an else None,
            "active_power_kw": active_power_kw,
            "apparent_power_kva": float(an["apparent_power_kva"]) if an and an.get("apparent_power_kva") is not None else None,
            "reactive_power_kvar": float(an["reactive_power_kvar"]) if an and an.get("reactive_power_kvar") is not None else None,
            "voltage_ab_v": float(an["voltage_ab_v"]) if an and an.get("voltage_ab_v") is not None else None,
            "voltage_bc_v": float(an["voltage_bc_v"]) if an and an.get("voltage_bc_v") is not None else None,
            "voltage_ca_v": float(an["voltage_ca_v"]) if an and an.get("voltage_ca_v") is not None else None,
            "current_a_a": float(an["current_a_a"]) if an and an.get("current_a_a") is not None else None,
            "current_b_a": float(an["current_b_a"]) if an and an.get("current_b_a") is not None else None,
            "current_c_a": float(an["current_c_a"]) if an and an.get("current_c_a") is not None else None,
        }
    }

    return http_response(200, {
        "item": item,
        "meta": {
            "event_source": f"{RT_SCHEMA}.stg_relay_event",
            "analog_source": f"{RT_SCHEMA}.stg_relay_analog",
        }
    })


# ============================================================
# MULTIMETER REALTIME
# ============================================================

def handle_get_multimeter_realtime(cur, plant_id: int, ctx: dict):
    cur.execute("SET LOCAL statement_timeout = '12000ms';")

    if not ensure_plant_access(cur, int(plant_id), ctx):
        return http_response(403, {"error": "sem permissão para esta usina"})

    cur.execute("""
        SELECT
            d.id AS device_id,
            d.name AS device_name,
            dt.id AS device_type_id,
            LOWER(COALESCE(dt.name, '')) AS device_type_name
        FROM public.device d
        JOIN public.device_type dt
        ON dt.id = d.device_type_id
        JOIN public.power_plant p
        ON p.id = d.power_plant_id
        WHERE d.power_plant_id = %(plant_id)s
        AND d.is_active = true
        AND (
            LOWER(COALESCE(dt.name, '')) IN ('multimeter', 'meter', 'multimedidor', 'medidor')
            OR LOWER(COALESCE(dt.name, '')) LIKE '%%multimedidor%%'
            OR LOWER(COALESCE(dt.name, '')) LIKE '%%meter%%'
            OR LOWER(COALESCE(dt.name, '')) LIKE '%%medidor%%'
            OR dt.id = 3
        )
        AND ( %(is_superuser)s = true OR p.customer_id = %(customer_id)s )
        ORDER BY d.id DESC
        LIMIT 1;
    """, {
        "plant_id": int(plant_id),
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
    })
    dev = cur.fetchone()

    if not dev:
        return http_response(200, {
            "item": None,
            "meta": {
                "source": f"{RT_SCHEMA}.stg_meter_analog"
            }
        })

    device_id = int(dev["device_id"])
    device_name = dev.get("device_name")

    cur.execute(sql.SQL("""
        SELECT
            a."timestamp" AS last_ts,
            a.power_plant_id,
            a.device_id,
            a.active_power_kw,
            a.reactive_power_kvar,
            CASE
                WHEN a.active_power_kw IS NOT NULL OR a.reactive_power_kvar IS NOT NULL THEN
                    SQRT(
                        POWER(COALESCE(a.active_power_kw, 0)::numeric, 2) +
                        POWER(COALESCE(a.reactive_power_kvar, 0)::numeric, 2)
                    )
                ELSE NULL::numeric
            END AS apparent_power_kva,
            a.power_factor,
            a.frequency_hz,
            a.voltage_ab_v,
            a.voltage_bc_v,
            a.voltage_ca_v,
            a.current_a_a,
            a.current_b_a,
            a.current_c_a,
            a.energy_import_kwh,
            a.energy_export_kwh,
            EXTRACT(EPOCH FROM (now() - a."timestamp"))::int AS age_seconds,
            (now() - a."timestamp" <= interval %(online_window)s) AS is_online
        FROM {stg_meter_analog} a
        WHERE a.power_plant_id = %(plant_id)s
        AND a.device_id = %(device_id)s
        AND a."timestamp" >= now() - interval '1 hour'
        ORDER BY a."timestamp" DESC
        LIMIT 1;
    """).format(stg_meter_analog=q(RT_SCHEMA, "stg_meter_analog")), {
        "plant_id": int(plant_id),
        "device_id": device_id,
        "online_window": MULTIMETER_ONLINE_WINDOW,
    })
    row = cur.fetchone()

    def fnum(k):
        if not row:
            return None
        v = row.get(k)
        return float(v) if v is not None else None

    last_update = row.get("last_ts") if row else None
    age_seconds = int(row["age_seconds"]) if row and row.get("age_seconds") is not None else None
    is_online = bool(row.get("is_online")) if row else False

    # Observação de qualidade de dados:
    # alguns equipamentos já enviaram power_factor fora de faixa típica.
    # Não alteramos escala aqui para manter fidelidade do dado bruto.
    power_factor = fnum("power_factor")

    item = {
        "power_plant_id": int(plant_id),
        "device_id": device_id,
        "device_name": device_name or "Multimedidor",
        "device_type": "multimeter",
        "last_update": last_update,
        "age_seconds": age_seconds,
        "is_online": is_online,
        "analog": {
            "timestamp": row.get("last_ts") if row else None,
            "active_power_kw": fnum("active_power_kw"),
            "react_power_kvar": fnum("reactive_power_kvar"),
            "reactive_power_kvar": fnum("reactive_power_kvar"),
            "apparent_power_kva": fnum("apparent_power_kva"),
            "power_factor": power_factor,
            "frequency_hz": fnum("frequency_hz"),
            "voltage_ab_v": fnum("voltage_ab_v"),
            "voltage_bc_v": fnum("voltage_bc_v"),
            "voltage_ca_v": fnum("voltage_ca_v"),
            "volt_uab_line": fnum("voltage_ab_v"),
            "volt_ubc_line": fnum("voltage_bc_v"),
            "volt_uca_line": fnum("voltage_ca_v"),
            "current_a_a": fnum("current_a_a"),
            "current_b_a": fnum("current_b_a"),
            "current_c_a": fnum("current_c_a"),
            "current_a_phase_a": fnum("current_a_a"),
            "current_b_phase_b": fnum("current_b_a"),
            "current_c_phase_c": fnum("current_c_a"),
            "energy_import_kwh": fnum("energy_import_kwh"),
            "energy_export_kwh": fnum("energy_export_kwh"),
            "energy_imp_kwh": fnum("energy_import_kwh"),
            "energy_exp_kwh": fnum("energy_export_kwh"),
            "communication_fault": None,
        }
    }

    return http_response(200, {
        "item": item,
        "meta": {
            "source": f"{RT_SCHEMA}.stg_meter_analog"
        }
    })


def handle_get_trackers_realtime(cur, plant_id: int, ctx: dict):
    cur.execute("SET LOCAL statement_timeout = '12000ms';")

    if not ensure_plant_access(cur, int(plant_id), ctx):
        return http_response(403, {"error": "sem permissão para esta usina"})

    analog_cols = set(get_table_columns(cur, RT_SCHEMA, "stg_tracker_analog"))

    def bool_expr(col_name: str) -> str:
        if col_name in analog_cols:
            return f"COALESCE(l.{col_name}, false)"
        return "false"

    def num_expr(col_name: str) -> str:
        if col_name in analog_cols:
            return f"l.{col_name}::float8"
        return "NULL::float8"

    error_expr = "NULL::float8"
    for c in ("deviation_deg", "tcu_desvio", "angle_error_deg"):
        if c in analog_cols:
            error_expr = f"l.{c}::float8"
            break

    angle_current_col = "angular_position_current_deg" if "angular_position_current_deg" in analog_cols else None
    angle_target_col = "angular_position_target_deg" if "angular_position_target_deg" in analog_cols else None

    query = f"""
        WITH trackers AS (
        SELECT
            d.id AS device_id,
            d.name AS device_name,
            d.name AS tracker_code,
            UPPER(dt.name) AS tracker_type,
            d.latitude::float8 AS latitude,
            d.longitude::float8 AS longitude
        FROM public.device d
        JOIN public.device_type dt ON dt.id = d.device_type_id
        JOIN public.power_plant p ON p.id = d.power_plant_id
        WHERE d.power_plant_id = %(plant_id)s
            AND d.is_active = true
            AND UPPER(dt.name) IN ('TCU','RSU','TRACKER_TCU','TRACKER_RSU','TRACKER')
            AND (%(is_superuser)s = true OR p.customer_id = %(customer_id)s)
        ),
        latest AS (
        SELECT DISTINCT ON (a.device_id)
            a.device_id,
            a."timestamp" AS last_update,
            {num_expr(angle_current_col) if angle_current_col else 'NULL::float8'} AS angle_deg,
            {num_expr(angle_target_col) if angle_target_col else 'NULL::float8'} AS target_angle_deg,
            {error_expr} AS error_value,
            {bool_expr("communication_fault")} AS communication_fault,
            {bool_expr("fault_tcu")} AS fault_tcu,
            {bool_expr("fault_zigbee")} AS fault_zigbee,
            {bool_expr("low_batt")} AS low_batt,
            {bool_expr("tcu_auto")} AS tcu_auto,
            {bool_expr("tcu_manual")} AS tcu_manual,
            {bool_expr("tcu_off")} AS tcu_off,
            {bool_expr("tcu_standbye")} AS tcu_standbye,
            {bool_expr("tcu_fora_limite")} AS tcu_fora_limite,
            {bool_expr("button_emergency")} AS button_emergency,
            (now() - a."timestamp" <= interval %(online_window)s) AS is_online
        FROM {RT_SCHEMA}.stg_tracker_analog a
        WHERE a.power_plant_id = %(plant_id)s
        ORDER BY a.device_id, a."timestamp" DESC
        )
        SELECT
        t.device_id,
        t.device_name,
        t.tracker_code,
        t.tracker_type,
        t.latitude,
        t.longitude,
        l.last_update,
        l.angle_deg,
        l.target_angle_deg,
        l.error_value,
        COALESCE(l.is_online, false) AS is_online,
        COALESCE(l.button_emergency, false) AS button_emergency,
        COALESCE(l.fault_tcu, false) AS fault_tcu,
        COALESCE(l.fault_zigbee, false) AS fault_zigbee,
        COALESCE(l.communication_fault, false) AS communication_fault,
        COALESCE(l.tcu_manual, false) AS tcu_manual,
        COALESCE(l.tcu_off, false) AS tcu_off,
        COALESCE(l.tcu_standbye, false) AS tcu_standbye,
        COALESCE(l.tcu_auto, false) AS tcu_auto
        FROM trackers t
        LEFT JOIN latest l ON l.device_id = t.device_id
        ORDER BY t.device_id ASC;
    """

    cur.execute(query, {
        "plant_id": int(plant_id),
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
        "online_window": TRACKER_ONLINE_WINDOW,
    })
    rows = cur.fetchall() or []

    items = []
    valid_coords = []
    for r in rows:
        ttype = (r.get("tracker_type") or "").upper()
        if ttype not in ("TCU", "RSU"):
            ttype = "RSU" if "RSU" in ttype else "TCU"
        if not r.get("last_update") or not r.get("is_online"):
            state_code = "no_comm"
        elif r.get("button_emergency"):
            state_code = "emergency"
        elif r.get("fault_tcu") or r.get("fault_zigbee") or r.get("communication_fault"):
            state_code = "fault"
        elif r.get("tcu_manual"):
            state_code = "manual"
        elif r.get("tcu_off"):
            state_code = "off"
        elif r.get("tcu_standbye"):
            state_code = "standby"
        elif r.get("tcu_auto"):
            state_code = "auto"
        else:
            state_code = "online" if r.get("is_online") else "unknown"

        item = {
            "tracker_id": r.get("device_id"),
            "tracker_code": r.get("tracker_code") or f"{ttype}{r.get('device_id')}",
            "tracker_type": ttype,
            "device_id": int(r["device_id"]),
            "name": r.get("device_name") or f"Tracker {r['device_id']}",
            "latitude": r.get("latitude"),
            "longitude": r.get("longitude"),
            "is_online": bool(r.get("is_online")),
            "state_code": state_code,
            "angle_deg": r.get("angle_deg"),
            "target_angle_deg": r.get("target_angle_deg"),
            "error_value": r.get("error_value"),
            "last_update": r.get("last_update")
        }
        if item["latitude"] is not None and item["longitude"] is not None:
            valid_coords.append((float(item["latitude"]), float(item["longitude"])))
        items.append(item)

    plant_center = None
    plant_bounds = None
    if valid_coords:
        lats = [p[0] for p in valid_coords]
        lngs = [p[1] for p in valid_coords]
        plant_center = {
            "latitude": sum(lats) / len(lats),
            "longitude": sum(lngs) / len(lngs)
        }
        plant_bounds = {
            "min_lat": min(lats),
            "max_lat": max(lats),
            "min_lng": min(lngs),
            "max_lng": max(lngs)
        }

    return http_response(200, {
        "items": items,
        "plant_center": plant_center,
        "plant_bounds": plant_bounds,
        "meta": {
            "source": f"{RT_SCHEMA}.stg_tracker_analog",
            "coords_source": "public.device"
        }
    })


# ============================================================
# DATA STUDIO TAGS
# ============================================================

def handle_get_datastudio_tags(cur, ctx: dict, params: dict):
    cur.execute("SET LOCAL statement_timeout = '12000ms';")
    load_current_user(cur, ctx)

    plant_id = params.get("plant_id") or params.get("power_plant_id")
    data_kind = params.get("data_kind")
    source = params.get("source")
    context = params.get("context")
    device_type = params.get("device_type")
    q_text = params.get("q")
    limit = params.get("limit", "200")

    try:
        limit = int(limit)
    except Exception:
        limit = 200

    limit = max(1, min(5000, limit))

    allowed_ids = get_allowed_plant_ids(ctx)
    allowed_filter = ""
    if allowed_ids:
        allowed_filter = "AND tc.power_plant_id IN %(allowed_plant_ids)s"

    cur.execute("""
        SELECT
        tc.id,
        tc.customer_id,
        tc.power_plant_id,
        tc.device_type,
        tc.device_id,
        tc.context,
        tc.point_name,
        tc.pathname,
        tc.description,
        tc.source,
        tc.data_kind,
        tc.unit
        FROM app.tag_catalog tc
        WHERE
        (%(is_superuser)s = true OR tc.customer_id = %(customer_id)s)
        AND tc.is_active = true
        AND (%(plant_id)s IS NULL OR tc.power_plant_id = %(plant_id)s::bigint)
        AND (%(data_kind)s IS NULL OR tc.data_kind = %(data_kind)s)
        AND (%(source)s IS NULL OR tc.source = %(source)s)
        AND (%(context)s IS NULL OR tc.context = %(context)s)
        AND (%(device_type)s IS NULL OR tc.device_type = %(device_type)s)
        AND (
            %(q)s IS NULL
            OR tc.pathname ILIKE '%%' || %(q)s || '%%'
            OR COALESCE(tc.description, '') ILIKE '%%' || %(q)s || '%%'
            OR COALESCE(tc.point_name, '') ILIKE '%%' || %(q)s || '%%'
            OR COALESCE(tc.context, '') ILIKE '%%' || %(q)s || '%%'
            OR COALESCE(tc.device_type, '') ILIKE '%%' || %(q)s || '%%'
        )
        """ + allowed_filter + """
        ORDER BY tc.power_plant_id,
        CASE
            WHEN tc.pathname LIKE 'PLANT.%%'   THEN 0
            WHEN tc.pathname LIKE 'WEATHER_%%' THEN 1
            WHEN tc.pathname LIKE 'METER_%%'   THEN 2
            WHEN tc.pathname LIKE 'RELAY_%%'   THEN 3
            ELSE 4
        END,
        tc.context, tc.pathname
        LIMIT %(limit)s;
    """, {
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
        "plant_id": plant_id if plant_id not in (None, "", "null", "undefined") else None,
        "data_kind": data_kind if data_kind not in (None, "", "null", "undefined") else None,
        "source": source if source not in (None, "", "null", "undefined") else None,
        "context": context if context not in (None, "", "null", "undefined") else None,
        "device_type": device_type if device_type not in (None, "", "null", "undefined") else None,
        "q": q_text if q_text not in (None, "", "null", "undefined") else None,
        "limit": limit,
        "allowed_plant_ids": tuple(allowed_ids) if allowed_ids else None,
    })

    rows = cur.fetchall() or []

    return http_response(200, {
        "items": rows,
        "count": len(rows)
    })

def get_tag_catalog_row(cur, ctx: dict, *, tag_id=None, pathname=None, power_plant_id=None):
    if tag_id is not None:
        cur.execute("""
            SELECT
                id,
                customer_id,
                power_plant_id,
                device_type,
                device_id,
                context,
                point_name,
                pathname,
                description,
                source,
                data_kind,
                unit
            FROM app.tag_catalog
            WHERE id = %(tag_id)s
            AND is_active = true
            AND (%(is_superuser)s = true OR customer_id = %(customer_id)s)
            LIMIT 1;
        """, {
            "tag_id": int(tag_id),
            "customer_id": ctx["customer_id"],
            "is_superuser": ctx["is_superuser"],
        })
        return cur.fetchone()

    if pathname and power_plant_id is not None:
        cur.execute("""
            SELECT
                id,
                customer_id,
                power_plant_id,
                device_type,
                device_id,
                context,
                point_name,
                pathname,
                description,
                source,
                data_kind,
                unit
            FROM app.tag_catalog
            WHERE pathname = %(pathname)s
            AND power_plant_id = %(power_plant_id)s
            AND is_active = true
            AND (%(is_superuser)s = true OR customer_id = %(customer_id)s)
            LIMIT 1;
        """, {
            "pathname": pathname,
            "power_plant_id": int(power_plant_id),
            "customer_id": ctx["customer_id"],
            "is_superuser": ctx["is_superuser"],
        })
        return cur.fetchone()

    return None



# ============================================================
# DATA STUDIO SELECTION (POST)
# ============================================================

def cleanup_old_datastudio_selections(cur, *, customer_id: int, keep_limit: int = 80):
    """
    Mantém apenas as `keep_limit` seleções mais recentes do customer_id.
    Remove primeiro os itens (app.user_selection_item) e depois o cabeçalho
    (app.user_selection).
    """
    cur.execute("""
        SELECT id
        FROM app.user_selection
        WHERE customer_id = %(customer_id)s
        AND COALESCE(is_favorite, false) = false
        ORDER BY created_at DESC, id DESC
        OFFSET %(keep_limit)s;
    """, {
        "customer_id": int(customer_id),
        "keep_limit": int(keep_limit),
    })

    rows = cur.fetchall() or []
    old_ids = [int(r["id"]) for r in rows if r.get("id") is not None]

    if not old_ids:
        return {
            "deleted_selection_ids": [],
            "deleted_count": 0
        }

    cur.execute("""
        DELETE FROM app.user_selection_item
        WHERE selection_id = ANY(%(ids)s);
    """, {
        "ids": old_ids
    })

    cur.execute("""
        DELETE FROM app.user_selection
        WHERE id = ANY(%(ids)s);
    """, {
        "ids": old_ids
    })

    return {
        "deleted_selection_ids": old_ids,
        "deleted_count": len(old_ids)
    }


def handle_post_datastudio_selection(cur, conn, ctx: dict, body: dict):
    cur.execute("SET LOCAL statement_timeout = '12000ms';")

    if body is None:
        return http_response(400, {"error": "JSON inválido"})

    selection_name = body.get("selection_name")
    power_plant_id = body.get("power_plant_id")
    start_ts = body.get("start_ts")
    end_ts = body.get("end_ts")
    historico_aggregation_default = body.get("historico_aggregation_default") or "avg"
    consolidado_period_default = body.get("consolidado_period_default") or "daily"
    timezone = body.get("timezone") or "America/Fortaleza"
    items = body.get("items") or []

    if not start_ts or not end_ts:
        return http_response(400, {"error": "start_ts e end_ts são obrigatórios"})

    sdt = parse_time_to_dt(start_ts)
    edt = parse_time_to_dt(end_ts)

    if not sdt or not edt:
        return http_response(400, {"error": "start_ts/end_ts inválidos"})

    if edt < sdt:
        return http_response(400, {"error": "end_ts não pode ser menor que start_ts"})

    if power_plant_id is not None:
        if not str(power_plant_id).isdigit():
            return http_response(400, {"error": "power_plant_id inválido"})
        if not ensure_plant_access(cur, int(power_plant_id), ctx):
            return http_response(403, {"error": "sem permissão para esta usina"})
        power_plant_id = int(power_plant_id)

    if power_plant_id is None:
        return http_response(400, {"error": "power_plant_id é obrigatório"})

    effective_customer_id = resolve_customer_id_for_plant(cur, int(power_plant_id), ctx)
    if effective_customer_id is None:
        return http_response(400, {"error": "não foi possível resolver customer_id efetivo pela usina"})

    if not isinstance(items, list):
        return http_response(400, {"error": "items deve ser uma lista"})

    if len(items) > 50:
        return http_response(400, {"error": "máximo de 50 itens por seleção"})

    cur.execute("""
        INSERT INTO app.user_selection (
            customer_id,
            user_id,
            selection_name,
            power_plant_id,
            start_ts,
            end_ts,
            historico_aggregation_default,
            consolidado_period_default,
            timezone,
            created_at
        )
        VALUES (
            %(customer_id)s,
            %(user_id)s,
            %(selection_name)s,
            %(power_plant_id)s,
            %(start_ts)s,
            %(end_ts)s,
            %(historico_aggregation_default)s,
            %(consolidado_period_default)s,
            %(timezone)s,
            now()
        )
        RETURNING id;
    """, {
        "customer_id": int(effective_customer_id),
        "user_id": None,
        "selection_name": selection_name,
        "power_plant_id": power_plant_id,
        "start_ts": sdt,
        "end_ts": edt,
        "historico_aggregation_default": historico_aggregation_default,
        "consolidado_period_default": consolidado_period_default,
        "timezone": timezone,
    })

    selection_row = cur.fetchone()
    selection_id = int(selection_row["id"])

    for idx, item in enumerate(items, start=1):
        pathname = item.get("pathname")
        tag_id = item.get("tag_id")

        if tag_id is not None:
            if str(tag_id).isdigit():
                tag_id = int(tag_id)
            else:
                rollback_quiet(conn)
                return http_response(400, {"error": f"item {idx}: tag_id inválido"})
        else:
            tag_id = None

        tag_meta = get_tag_catalog_row(
            cur,
            ctx,
            tag_id=tag_id,
            pathname=pathname,
            power_plant_id=power_plant_id
        )

        if not tag_meta:
            rollback_quiet(conn)
            return http_response(400, {
                "error": f"item {idx}: tag não encontrada no catálogo",
                "pathname": pathname,
                "tag_id": tag_id
            })

        pathname = tag_meta["pathname"]
        tag_id = int(tag_meta["id"])

        series_order = item.get("series_order", idx)
        try:
            series_order = int(series_order)
        except Exception:
            rollback_quiet(conn)
            return http_response(400, {"error": f"item {idx}: series_order inválido"})

        label = item.get("label") or tag_meta.get("description") or tag_meta.get("point_name") or pathname
        source_val = item.get("source") or tag_meta.get("source")
        unit_val = item.get("unit") or tag_meta.get("unit")
        data_kind_val = item.get("data_kind") or tag_meta.get("data_kind")

        cur.execute("""
            INSERT INTO app.user_selection_item (
                selection_id,
                tag_id,
                pathname,
                aggregation_override,
                period_override,
                source_override,
                display_type,
                series_order,
                source,
                unit,
                label,
                data_kind,
                created_at
            )
            VALUES (
                %(selection_id)s,
                %(tag_id)s,
                %(pathname)s,
                %(aggregation_override)s,
                %(period_override)s,
                %(source_override)s,
                %(display_type)s,
                %(series_order)s,
                %(source)s,
                %(unit)s,
                %(label)s,
                %(data_kind)s,
                now()
            );
        """, {
            "selection_id": selection_id,
            "tag_id": tag_id,
            "pathname": pathname,
            "aggregation_override": item.get("aggregation_override"),
            "period_override": item.get("period_override"),
            "source_override": item.get("source_override"),
            "display_type": item.get("display_type") or "line",
            "series_order": series_order,
            "source": source_val,
            "unit": unit_val,
            "label": label,
            "data_kind": data_kind_val,
        })

    cleanup_info = cleanup_old_datastudio_selections(
        cur,
        customer_id=int(effective_customer_id),
        keep_limit=80
    )

    conn.commit()

    return http_response(200, {
        "ok": True,
        "selection_id": selection_id,
        "items_count": len(items),
        "cleanup": {
            "keep_limit": 80,
            "deleted_count": cleanup_info["deleted_count"]
        },
        "selection": {
            "selection_name": selection_name,
            "power_plant_id": power_plant_id,
            "start_ts": sdt,
            "end_ts": edt,
            "historico_aggregation_default": historico_aggregation_default,
            "consolidado_period_default": consolidado_period_default,
            "timezone": timezone
        }
    })



# ============================================================
# DATA STUDIO — TOGGLE FAVORITO
# ============================================================
def handle_post_datastudio_favorite(cur, conn, ctx: dict, body: dict):
    cur.execute("SET LOCAL statement_timeout = '8000ms';")

    if body is None:
        return http_response(400, {"error": "JSON inválido"})

    selection_id = body.get("selection_id")
    if not selection_id or not str(selection_id).isdigit():
        return http_response(400, {"error": "selection_id inválido"})
    selection_id = int(selection_id)

    # is_favorite pode vir explícito (true/false) — se ausente, faz toggle
    desired = body.get("is_favorite", None)

    username = ctx.get("username")

    # Confere acesso à seleção (mesmo escopo do GET series)
    cur.execute("""
        SELECT id, customer_id, COALESCE(is_favorite, false) AS is_favorite
        FROM app.user_selection
        WHERE id = %(selection_id)s
        AND (%(is_superuser)s = true OR customer_id = %(customer_id)s)
        LIMIT 1;
    """, {
        "selection_id": selection_id,
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
    })
    row = cur.fetchone()
    if not row:
        return http_response(404, {"error": "selection não encontrada"})

    if desired is None:
        new_fav = not bool(row["is_favorite"])
    else:
        new_fav = bool(desired)

    cur.execute("""
        UPDATE app.user_selection
        SET is_favorite  = %(fav)s,
            favorited_by = CASE WHEN %(fav)s THEN %(username)s ELSE NULL END,
            favorited_at = CASE WHEN %(fav)s THEN now() ELSE NULL END
        WHERE id = %(selection_id)s;
    """, {
        "fav": new_fav,
        "username": username,
        "selection_id": selection_id,
    })
    conn.commit()

    return http_response(200, {
        "ok": True,
        "selection_id": selection_id,
        "is_favorite": new_fav,
    })


# ============================================================
# DATA STUDIO — LISTAR SELEÇÕES (favoritos e recentes)
# ============================================================
def handle_get_datastudio_selections(cur, conn, ctx: dict, params: dict):
    cur.execute("SET LOCAL statement_timeout = '8000ms';")

    only_fav = str(params.get("favorites_only", "false")).lower() == "true"
    username = ctx.get("username")

    fav_filter = ""
    if only_fav:
        fav_filter = """
        AND COALESCE(us.is_favorite, false) = true
        AND (us.favorited_by = %(username)s OR %(username)s IS NULL)
        """

    cur.execute(f"""
        SELECT
            us.id,
            us.selection_name,
            us.power_plant_id,
            pp.name AS power_plant_name,
            us.start_ts,
            us.end_ts,
            us.timezone,
            us.historico_aggregation_default,
            us.consolidado_period_default,
            us.created_at,
            COALESCE(us.is_favorite, false) AS is_favorite,
            us.favorited_by,
            us.favorited_at,
            (SELECT count(*) FROM app.user_selection_item i WHERE i.selection_id = us.id) AS items_count,
            (SELECT string_agg(
                COALESCE(tc.description, i2.label, tc.point_name, i2.pathname),
                ', ' ORDER BY i2.series_order
            )
            FROM app.user_selection_item i2
            LEFT JOIN app.tag_catalog tc ON tc.id = i2.tag_id
            WHERE i2.selection_id = us.id) AS items_labels
        FROM app.user_selection us
        LEFT JOIN public.power_plant pp ON pp.id = us.power_plant_id
        WHERE (%(is_superuser)s = true OR us.customer_id = %(customer_id)s)
        {fav_filter}
        ORDER BY COALESCE(us.is_favorite, false) DESC, us.created_at DESC, us.id DESC
        LIMIT 100;
    """, {
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
        "username": username,
    })

    rows = cur.fetchall() or []
    return http_response(200, {
        "ok": True,
        "count": len(rows),
        "selections": rows,
    })


# ============================================================
# DATA STUDIO SERIES (GET) - ROTEADA POR MART
# ============================================================

def fetch_datastudio_points_from_timeseries(cur, *, customer_id: int, power_plant_id: int,
                                            pathname: str, start_ts, end_ts,
                                            source: str = None, limit: int = 5000):
    cur.execute(sql.SQL("""
        SELECT
            ts,
            value
        FROM {tbl}
        WHERE customer_id = %(customer_id)s
        AND power_plant_id = %(power_plant_id)s
        AND pathname = %(pathname)s
        AND ts >= %(start_ts)s
        AND ts <= %(end_ts)s
        AND (%(source)s IS NULL OR source = %(source)s)
        ORDER BY ts
        LIMIT %(limit)s;
    """).format(
        tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_timeseries")
    ), {
        "customer_id": customer_id,
        "power_plant_id": power_plant_id,
        "pathname": pathname,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "source": source,
        "limit": limit,
    })
    return cur.fetchall() or []


_SUMMABLE_SUFFIXES = frozenset({
    '.daily_energy', '.cumulative_energy',
    '.energy_import', '.energy_export',
    '.accumulated_rain', '.hourly_accumulated_rain',
})

def _is_summable_pathname(pathname: str) -> bool:
    pn = safe_lower(pathname)
    if pn.startswith("plant.") and "alarm" in pn:
        return True
    for suffix in _SUMMABLE_SUFFIXES:
        if pn.endswith(suffix):
            return True
    if ".alarm_" in pn or pn.endswith("_alarm_count"):
        return True
    return False


def datastudio_resolve_hist_aggregation(aggregation: str, pathname: str = ""):
    agg = safe_lower(aggregation)

    if agg in ("sum", "soma"):
        if pathname and not _is_summable_pathname(pathname):
            return "avg_value", "avg(override:non-summable)"
        return "sum_value", "sum"

    if agg in ("max", "maxima", "máxima"):
        return "max_value", "max"

    if agg in ("avg", "media", "média", "none", "raw", "sem_agregacao"):
        return "avg_value", (agg or "avg")

    fallback_from = agg or "none"
    return "avg_value", f"avg(fallback:{fallback_from})"


def _is_string_current(pathname):
    return bool(re.match(r'^INV_\d+\.string_current_\d+$', str(pathname or '')))

def _parse_string_pathname(pathname):
    m = re.match(r'^INV_(\d+)\.string_current_(\d+)$', str(pathname or ''))
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)

def fetch_datastudio_points_historico(cur, *, customer_id: int, power_plant_id: int,
                                    pathname: str, start_ts, end_ts,
                                    aggregation: str = None, limit: int = 5000):
    if _is_string_current(pathname):
        device_id_str, string_idx = _parse_string_pathname(pathname)
        if device_id_str is None:
            return [], "hist_string", str(aggregation or "avg").lower()
        agg_col = {
            "max":      "max_value",
        }.get(str(aggregation or "avg").lower(), "avg_value")
        cur.execute(f"""
            SELECT ts, {agg_col} AS value
            FROM analytics.mart_datastudio_hist_string
            WHERE customer_id    = %(cid)s
            AND power_plant_id = %(pid)s
            AND device_id      = %(did)s
            AND string_index   = %(sidx)s
            AND ts BETWEEN %(start_ts)s AND %(end_ts)s
            ORDER BY ts
        """, {
            "cid":      customer_id,
            "pid":      int(power_plant_id),
            "did":      device_id_str,
            "sidx":     string_idx,
            "start_ts": start_ts,
            "end_ts":   end_ts
        })
        rows = cur.fetchall() or []
        return [
            {"ts": r["ts"].isoformat(), "value": float(r["value"])}
            for r in rows if r["value"] is not None
        ], "hist_string", str(aggregation or "avg").lower()

    hist_table_name, resolved_route = datastudio_choose_hist_table(start_ts, end_ts)
    value_col, aggregation_resolved = datastudio_resolve_hist_aggregation(aggregation, pathname)

    cur.execute(sql.SQL("""
        SELECT
            ts,
            {value_col} AS value
        FROM {tbl}
        WHERE customer_id = %(customer_id)s
        AND power_plant_id = %(power_plant_id)s
        AND pathname = %(pathname)s
        AND ts >= %(start_ts)s
        AND ts <= %(end_ts)s
        ORDER BY ts
        LIMIT %(limit)s;
    """).format(
        value_col=sql.Identifier(value_col),
        tbl=q(ANALYTICS_SCHEMA, hist_table_name)
    ), {
        "customer_id": customer_id,
        "power_plant_id": power_plant_id,
        "pathname": pathname,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit": limit,
    })

    return cur.fetchall() or [], resolved_route, aggregation_resolved


def fetch_datastudio_points_consolidado(cur, *, customer_id: int, power_plant_id: int,
                                        pathname: str, start_ts, end_ts,
                                        period: str = None, limit: int = 5000):
    period_l = safe_lower(period)

    # =========================================================
    # daily consolidado
    # =========================================================
    if period_l in ("daily", "hdaily") and pathname in DATASTUDIO_DAILY_PATHS:
        meta = DATASTUDIO_DAILY_PATHS[pathname]
        cur.execute(sql.SQL("""
            SELECT
                date::timestamptz AS ts,
                {value_col} AS value
            FROM {tbl}
            WHERE customer_id = %(customer_id)s
            AND power_plant_id = %(power_plant_id)s
            AND date::timestamptz >= %(start_ts)s
            AND date::timestamptz <= %(end_ts)s
            ORDER BY date
            LIMIT %(limit)s;
        """).format(
            value_col=sql.Identifier(meta["column"]),
            tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_daily")
        ), {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], "daily"

    # =========================================================
    # weekly consolidado -> agrega em cima de mart_datastudio_daily
    # =========================================================
    if period_l in ("weekly", "hweekly") and pathname in DATASTUDIO_DAILY_PATHS:
        meta = DATASTUDIO_DAILY_PATHS[pathname]
        strategy = datastudio_daily_rollup_strategy(pathname)

        if strategy == "sum":
            agg_expr = sql.SQL("SUM({col})").format(col=sql.Identifier(meta["column"]))
        else:
            agg_expr = sql.SQL("AVG({col})").format(col=sql.Identifier(meta["column"]))

        cur.execute(sql.SQL("""
            SELECT
                date_trunc('week', date::timestamp)::timestamptz AS ts,
                {agg_expr} AS value
            FROM {tbl}
            WHERE customer_id = %(customer_id)s
            AND power_plant_id = %(power_plant_id)s
            AND date::timestamptz >= %(start_ts)s
            AND date::timestamptz <= %(end_ts)s
            GROUP BY 1
            ORDER BY 1
            LIMIT %(limit)s;
        """).format(
            agg_expr=agg_expr,
            tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_daily")
        ), {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], f"weekly({strategy})"

    # =========================================================
    # monthly consolidado
    # =========================================================
    if period_l in ("monthly", "hmonthly") and pathname in DATASTUDIO_MONTHLY_PATHS:
        meta = DATASTUDIO_MONTHLY_PATHS[pathname]
        cur.execute(sql.SQL("""
            SELECT
                month::timestamptz AS ts,
                {value_col} AS value
            FROM {tbl}
            WHERE customer_id = %(customer_id)s
            AND power_plant_id = %(power_plant_id)s
            AND month::timestamptz >= %(start_ts)s
            AND month::timestamptz <= %(end_ts)s
            ORDER BY month
            LIMIT %(limit)s;
        """).format(
            value_col=sql.Identifier(meta["column"]),
            tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_monthly")
        ), {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], "monthly"

    # =========================================================
    # yearly consolidado -> agrega em cima de mart_datastudio_monthly
    # =========================================================
    if period_l in ("yearly", "hyearly") and pathname in DATASTUDIO_MONTHLY_PATHS:
        meta = DATASTUDIO_MONTHLY_PATHS[pathname]
        strategy = datastudio_monthly_rollup_strategy(pathname)

        if strategy == "sum":
            agg_expr = sql.SQL("SUM({col})").format(col=sql.Identifier(meta["column"]))
        else:
            agg_expr = sql.SQL("AVG({col})").format(col=sql.Identifier(meta["column"]))

        cur.execute(sql.SQL("""
            SELECT
                date_trunc('year', month::timestamp)::timestamptz AS ts,
                {agg_expr} AS value
            FROM {tbl}
            WHERE customer_id = %(customer_id)s
            AND power_plant_id = %(power_plant_id)s
            AND month::timestamptz >= %(start_ts)s
            AND month::timestamptz <= %(end_ts)s
            GROUP BY 1
            ORDER BY 1
            LIMIT %(limit)s;
        """).format(
            agg_expr=agg_expr,
            tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_monthly")
        ), {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], f"yearly({strategy})"

    return [], "consolidado_unsupported"


def datastudio_resolved_table_name(resolved_route: str):
    route = safe_lower(resolved_route)

    if route == "hist_15min":
        return f"{ANALYTICS_SCHEMA}.mart_datastudio_hist_15min"
    if route == "hist_hourly":
        return f"{ANALYTICS_SCHEMA}.mart_datastudio_hist_hourly"
    if route == "hist_daily":
        return f"{ANALYTICS_SCHEMA}.mart_datastudio_hist_daily"
    if route == "daily" or route.startswith("weekly("):
        return f"{ANALYTICS_SCHEMA}.mart_datastudio_daily"
    if route == "monthly" or route.startswith("yearly("):
        return f"{ANALYTICS_SCHEMA}.mart_datastudio_monthly"
    if route == "timeseries":
        return f"{ANALYTICS_SCHEMA}.mart_datastudio_timeseries"

    return None


def datastudio_period_for_fixed_route(route_type: str, period: str):
    period_l = safe_lower(period)

    if route_type == "daily" and period_l not in ("daily", "hdaily", "weekly", "hweekly"):
        return "daily"

    if route_type == "monthly" and period_l not in ("monthly", "hmonthly", "yearly", "hyearly"):
        return "monthly"

    return period


def handle_get_datastudio_series(cur, ctx: dict, params: dict):  # noqa: C901
    t0 = time.perf_counter()
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    selection_id = params.get("selection_id")
    if not selection_id or not str(selection_id).isdigit():
        return http_response(400, {"error": "selection_id inválido"})

    selection_id = int(selection_id)
    max_points_per_series = 5000

    cur.execute("""
        SELECT
            id,
            customer_id,
            user_id,
            selection_name,
            power_plant_id,
            start_ts,
            end_ts,
            historico_aggregation_default,
            consolidado_period_default,
            timezone,
            created_at
        FROM app.user_selection
        WHERE id = %(selection_id)s
        AND (%(is_superuser)s = true OR customer_id = %(customer_id)s)
        LIMIT 1;
    """, {
        "selection_id": selection_id,
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
    })

    selection = cur.fetchone()
    if not selection:
        return http_response(404, {"error": "selection não encontrada"})

    cur.execute("""
        SELECT
            usi.id,
            usi.selection_id,
            usi.tag_id,
            usi.pathname,
            usi.aggregation_override,
            usi.period_override,
            usi.source_override,
            usi.display_type,
            usi.series_order,
            usi.source,
            usi.unit,
            usi.label,
            usi.data_kind,
            usi.created_at,
            tc.source AS tag_source,
            tc.device_type,
            tc.device_id,
            tc.context,
            tc.point_name,
            tc.description
        FROM app.user_selection_item usi
        LEFT JOIN app.tag_catalog tc
        ON tc.id = usi.tag_id
        WHERE usi.selection_id = %(selection_id)s
        ORDER BY usi.series_order, usi.id;
    """, {
        "selection_id": selection_id
    })

    items = cur.fetchall() or []

    if not items:
        return http_response(200, {
            "selection_id": selection_id,
            "selection": selection,
            "meta": {
                "timezone": selection.get("timezone"),
                "start_ts": selection.get("start_ts"),
                "end_ts": selection.get("end_ts"),
                "items_count": 0,
                "series_count": 0,
                "max_points_per_series": max_points_per_series,
                "truncated": False,
                "errors_count": 0
            },
            "series": [],
            "errors": []
        })

    series_out = []
    errors_out = []
    selection_truncated = False
    route_hits = {}
    route_debug = []
    total_points = 0

    power_plant_id = selection.get("power_plant_id")
    start_ts = selection.get("start_ts")
    end_ts = selection.get("end_ts")

    override_start = (params.get("start_ts") or "").strip()
    override_end = (params.get("end_ts") or "").strip()
    if override_start:
        parsed = parse_time_to_dt(override_start)
        if parsed:
            start_ts = parsed
    if override_end:
        parsed = parse_time_to_dt(override_end)
        if parsed:
            end_ts = parsed

    historico_default = selection.get("historico_aggregation_default")
    consolidado_default = selection.get("consolidado_period_default")

    if power_plant_id is None:
        return http_response(400, {"error": "selection sem power_plant_id"})

    selection_customer_id = int(selection["customer_id"])

    print(f"[/datastudio/series] START selection_id={selection_id} plant_id={power_plant_id} items={len(items)}")

    for item in items:
        pathname = item.get("pathname")
        if not pathname:
            errors_out.append({
                "pathname": None,
                "label": item.get("label"),
                "error": "item sem pathname"
            })
            continue

        source_effective = item.get("source_override") or item.get("source") or item.get("tag_source") or "historico"
        aggregation_effective = item.get("aggregation_override") or historico_default or "avg"
        period_effective = item.get("period_override") or consolidado_default or "daily"

        resolved_route = "timeseries"
        aggregation_resolved = aggregation_effective

        try:
            route_type = datastudio_resolve_source_type(pathname, source_effective)

            print(
                f"[/datastudio/series] ITEM pathname={pathname} "
                f"source={source_effective} route_type={route_type} "
                f"agg={aggregation_effective} period={period_effective}"
            )

            # Roteamento:
            # - route_type daily/monthly (PLANT.energy_real_kwh_daily, etc) → sempre consolidado,
            #   independente do source declarado (dado só existe em mart_datastudio_daily/monthly)
            # - source=historico para qualquer outro pathname (INV_*, WEATHER_*, PLANT.active_power_kw)
            #   → hist_* tables (15min/hourly/daily conforme range)
            # - source=consolidado explícito → consolidado
            # - fallback → timeseries
            if route_type in ("daily", "monthly"):
                period_for_query = datastudio_period_for_fixed_route(route_type, period_effective)
                rows, resolved_route = fetch_datastudio_points_consolidado(
                    cur,
                    customer_id=selection_customer_id,
                    power_plant_id=int(power_plant_id),
                    pathname=pathname,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    period=period_for_query,
                    limit=max_points_per_series,
                )

            elif route_type == "historico":
                rows, resolved_route, aggregation_resolved = fetch_datastudio_points_historico(
                    cur,
                    customer_id=selection_customer_id,
                    power_plant_id=int(power_plant_id),
                    pathname=pathname,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    aggregation=aggregation_effective,
                    limit=max_points_per_series,
                )

            elif route_type == "consolidado":
                rows, resolved_route = fetch_datastudio_points_consolidado(
                    cur,
                    customer_id=selection_customer_id,
                    power_plant_id=int(power_plant_id),
                    pathname=pathname,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    period=period_effective,
                    limit=max_points_per_series,
                )

            else:
                rows = fetch_datastudio_points_from_timeseries(
                    cur,
                    customer_id=selection_customer_id,
                    power_plant_id=int(power_plant_id),
                    pathname=pathname,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    source=source_effective,
                    limit=max_points_per_series,
                )
                resolved_route = "timeseries"

            resolved_table = datastudio_resolved_table_name(resolved_route)
            route_hits[resolved_route] = route_hits.get(resolved_route, 0) + 1
            route_debug.append({
                "pathname": pathname,
                "effective_source": source_effective,
                "route_type": route_type,
                "resolved_route": resolved_route,
                "resolved_table": resolved_table,
            })

            truncated = len(rows) >= max_points_per_series
            if truncated:
                selection_truncated = True

            points = []
            for row in rows:
                v = row.get("value")
                try:
                    v = float(v) if v is not None else None
                except Exception:
                    v = None
                points.append({
                    "ts": row.get("ts"),
                    "value": v
                })

            total_points += len(points)

            print(
                f"[/datastudio/series] OK pathname={pathname} "
                f"rows={len(points)} route_type={route_type} "
                f"route={resolved_route} table={resolved_table}"
            )

            series_out.append({
                "tag_id": item.get("tag_id"),
                "pathname": pathname,
                "label": item.get("label") or item.get("description") or item.get("point_name") or pathname,
                "source": source_effective,
                "unit": item.get("unit"),
                "data_kind": item.get("data_kind"),
                "display_type": item.get("display_type") or "line",
                "series_order": item.get("series_order"),
                "aggregation": aggregation_effective,
                "aggregation_resolved": aggregation_resolved,
                "period": period_effective,
                "context": item.get("context"),
                "device_type": item.get("device_type"),
                "device_id": item.get("device_id"),
                "description": item.get("description"),
                "points_count": len(points),
                "truncated": truncated,
                "resolved_route": resolved_route,
                "route_type": route_type,
                "resolved_table": resolved_table,
                "points": points
            })

        except Exception as item_err:
            print(f"[/datastudio/series] ITEM ERROR pathname={pathname}: {repr(item_err)}")
            print(traceback.format_exc())

            errors_out.append({
                "pathname": pathname,
                "label": item.get("label") or item.get("description") or item.get("point_name") or pathname,
                "source": source_effective,
                "aggregation": aggregation_effective,
                "period": period_effective,
                "error": repr(item_err)
            })

            continue

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    print(
        f"[/datastudio/series] END selection_id={selection_id} "
        f"series={len(series_out)} errors={len(errors_out)} "
        f"points={total_points} routes={json.dumps(route_hits, ensure_ascii=False)} "
        f"elapsed_ms={elapsed_ms}"
    )

    return http_response(200, {
        "selection_id": selection_id,
        "selection": selection,
        "meta": {
            "timezone": selection.get("timezone"),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "items_count": len(items),
            "series_count": len(series_out),
            "errors_count": len(errors_out),
            "max_points_per_series": max_points_per_series,
            "truncated": selection_truncated,
            "route_debug": route_debug
        },
        "series": series_out,
        "errors": errors_out
    })


# ============================================================
# PLANTS SUMMARY
# ============================================================

def handle_get_plants_summary(cur, ctx: dict):
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    cur.execute(sql.SQL("""
        WITH pivot AS (
        SELECT
            r.power_plant_id,
            p.customer_id,
            r.device_id,
            MAX(CASE WHEN r.point_name = 'working_status' THEN r.point_value END) AS working_status_raw,
            MAX(CASE WHEN r.point_name = 'state_operation' THEN r.point_value END) AS state_operation_raw,
            MAX(r."timestamp") AS last_reading_ts
        FROM {tbl} r
        JOIN public.power_plant p
            ON p.id = r.power_plant_id
        WHERE LOWER(COALESCE(r.device_type_name, '')) = 'inverter'
            AND LOWER(COALESCE(r.reading_source, '')) = 'inverter'
            AND (
            %(is_superuser)s = true
            OR p.customer_id = %(customer_id)s
            )
        GROUP BY
            r.power_plant_id,
            p.customer_id,
            r.device_id
        ),
        normalized AS (
        SELECT
            power_plant_id,
            customer_id,
            device_id,
            last_reading_ts,
            CASE
            WHEN working_status_raw::text ~ '^-?\d+(\.\d+)?$' THEN working_status_raw::text::numeric::int
            ELSE NULL
            END AS working_status,
            CASE
            WHEN state_operation_raw::text ~ '^-?\d+(\.\d+)?$' THEN state_operation_raw::text::numeric::int
            ELSE NULL
            END AS state_operation
        FROM pivot
        ),
        classified AS (
        SELECT
            power_plant_id,
            customer_id,
            device_id,
            last_reading_ts,
            working_status,
            state_operation,
            CASE
            WHEN last_reading_ts IS NULL THEN 'NO_COMM'
            WHEN (now() - last_reading_ts) > (%(online_window)s)::interval THEN 'NO_COMM'
            WHEN state_operation = 2 THEN 'GEN'
            WHEN state_operation = 3 THEN 'OFF'
            WHEN state_operation = 0 THEN 'NO_COMM'
            WHEN working_status = 1 THEN 'GEN'
            WHEN working_status = 8 THEN 'OFF'
            ELSE 'OFF'
            END AS status_group
        FROM normalized
        )
        SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (
            WHERE status_group = 'GEN'
        )::int AS gen,
        COUNT(*) FILTER (
            WHERE status_group = 'NO_COMM'
        )::int AS no_comm,
        COUNT(*) FILTER (
            WHERE status_group = 'OFF'
        )::int AS off
        FROM classified;
    """).format(
        tbl=q(RT_SCHEMA, "mart_latest_reading")
    ), {
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
        "online_window": INVERTER_ONLINE_WINDOW,
    })

    row = cur.fetchone() or {}

    return http_response(200, {
        "gen": int(row.get("gen") or 0),
        "no_comm": int(row.get("no_comm") or 0),
        "off": int(row.get("off") or 0),
        "total": int(row.get("total") or 0),
        "meta": {
            "source": f"{RT_SCHEMA}.mart_latest_reading",
            "online_window": INVERTER_ONLINE_WINDOW
        }
    })


# ============================================================
# RONDA DIÁRIA
# ============================================================

def handle_get_daily_round(cur, plant_id: int, ctx: dict, params: dict):
    print(f"[daily-round] START plant_id={plant_id}")
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    if not ensure_plant_access(cur, int(plant_id), ctx):
        return http_response(403, {"error": "sem permissão para esta usina"})

    print("[daily-round] access OK")
    date_str = (params.get("date") or "").strip()
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return http_response(400, {"error": "date inválido, use YYYY-MM-DD"})
    else:
        cur.execute("SELECT (now() AT TIME ZONE 'America/Fortaleza')::date - 1 AS d")
        target_date = cur.fetchone()["d"]

    tz = "America/Fortaleza"
    day_start = f"{target_date}T00:00:00"
    day_end = f"{target_date + timedelta(days=1)}T00:00:00"

    print(f"[daily-round] date={target_date}, day_start={day_start}, day_end={day_end}")
    # ── 1. Plant summary (fct_power_plant_metrics_daily) ──
    cur.execute(sql.SQL("""
        SELECT
            f.power_plant_id,
            f.power_plant_name,
            f.customer_id,
            f.rated_power_kwp,
            f.rated_power_ac_kw,
            f.generation_daily_kwh,
            f.generation_liquid_meter_kwh,
            f.irradiation_daily_kwh_m2,
            f.pr_daily_pct,
            f.pr_accumulated_pct,
            f.capacity_factor_daily_pct,
            f.generation_accumulated_kwh,
            f.irradiation_accumulated_kwh_m2,
            f.capacity_factor_accumulated_pct
        FROM {fct} f
        WHERE f.power_plant_id = %(plant_id)s
        AND f.date_day = %(target_date)s
    """).format(fct=q(RT_SCHEMA, "fct_power_plant_metrics_daily")), {
        "plant_id": plant_id,
        "target_date": target_date,
    })
    plant_row = cur.fetchone()

    rated_kwp = float(plant_row["rated_power_kwp"]) if plant_row and plant_row.get("rated_power_kwp") else 0
    irradiation = float(plant_row["irradiation_daily_kwh_m2"]) if plant_row and plant_row.get("irradiation_daily_kwh_m2") else 0

    plant_summary = None
    if plant_row:
        plant_summary = {
            "power_plant_id": plant_id,
            "power_plant_name": plant_row["power_plant_name"],
            "date": str(target_date),
            "rated_power_kwp": float(plant_row["rated_power_kwp"] or 0),
            "rated_power_ac_kw": float(plant_row["rated_power_ac_kw"] or 0),
            "generation_kwh": float(plant_row["generation_daily_kwh"] or 0),
            "generation_liquid_meter_kwh": float(plant_row["generation_liquid_meter_kwh"] or 0),
            "irradiation_kwh_m2": float(plant_row["irradiation_daily_kwh_m2"] or 0),
            "pr_daily_pct": float(plant_row["pr_daily_pct"]) if plant_row["pr_daily_pct"] is not None else None,
            "pr_accumulated_pct": float(plant_row["pr_accumulated_pct"]) if plant_row["pr_accumulated_pct"] is not None else None,
            "capacity_factor_daily_pct": float(plant_row["capacity_factor_daily_pct"]) if plant_row["capacity_factor_daily_pct"] is not None else None,
        }

    print(f"[daily-round] 1-plant OK rows={'1' if plant_row else '0'}")
    # ── 2. Weather summary ──
    cur.execute(sql.SQL("""
        SELECT
            round(avg(COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2))::numeric, 1) AS irradiance_avg_wm2,
            round(max(COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2))::numeric, 1) AS irradiance_max_wm2,
            round(avg(irradiance_poa_wm2)::numeric, 1) AS poa_avg_wm2,
            round(max(irradiance_poa_wm2)::numeric, 1) AS poa_max_wm2,
            round(avg(irradiance_ghi_wm2)::numeric, 1) AS ghi_avg_wm2,
            round(max(irradiance_ghi_wm2)::numeric, 1) AS ghi_max_wm2,
            round(max(irradiance_poa_acc)::numeric, 3) AS poa_acc_wh_m2,
            round(max(irradiance_ghi_acc)::numeric, 3) AS ghi_acc_wh_m2,
            round(avg(air_temperature_c)::numeric, 1) AS air_temp_avg_c,
            round(max(air_temperature_c)::numeric, 1) AS air_temp_max_c,
            round(avg(module_temperature_c)::numeric, 1) AS module_temp_avg_c,
            round(max(module_temperature_c)::numeric, 1) AS module_temp_max_c,
            bool_or(rain_signal > 0) AS rain_detected,
            round(avg(wind_speed)::numeric, 1) AS wind_speed_avg,
            round(max(wind_speed)::numeric, 1) AS wind_speed_max
        FROM {ws} w
        WHERE w.power_plant_id = %(plant_id)s
        AND w."timestamp" >= (%(day_start)s AT TIME ZONE %(tz)s)
        AND w."timestamp" <  (%(day_end)s   AT TIME ZONE %(tz)s)
        AND COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2) > 5
    """).format(ws=q(RT_SCHEMA, "stg_weather_station_analog")), {
        "plant_id": plant_id,
        "day_start": day_start,
        "day_end": day_end,
        "tz": tz,
    })
    weather_row = cur.fetchone()
    weather = {}
    if weather_row:
        weather = {k: (float(v) if v is not None else None) for k, v in weather_row.items() if k != "rain_detected"}
        weather["rain_detected"] = bool(weather_row.get("rain_detected"))
        irr_avg = weather.get("irradiance_avg_wm2")
        if irr_avg is not None:
            if irr_avg >= 600:
                weather["irradiance_classification"] = "alta"
            elif irr_avg >= 350:
                weather["irradiance_classification"] = "normal"
            else:
                weather["irradiance_classification"] = "baixa"

    print("[daily-round] 2-weather OK")
    # ── 3. Inverters: avg power, energy, efficiency, temp, gen start/end ──
    cur.execute(sql.SQL("""
        WITH inv_day AS (
            SELECT
                a.device_id,
                d.name AS inverter_name,
                round(avg(a.active_power_kw)::numeric, 2) AS avg_power_kw,
                round(max(a.active_power_kw)::numeric, 2) AS max_power_kw,
                round(avg(a.efficiency_pct)::numeric, 1) AS avg_efficiency_pct,
                round(avg(a.temperature_internal_c)::numeric, 1) AS avg_temp_c,
                round(max(a.temperature_internal_c)::numeric, 1) AS max_temp_c,
                max(a.daily_active_energy_kwh) AS energy_daily_kwh,
                max(a.cumulative_active_energy_kwh) AS energy_cumulative_kwh,
                to_char(min(a."timestamp") FILTER (WHERE a.active_power_kw > 0.1)
                    AT TIME ZONE %(tz)s, 'HH24:MI') AS gen_start_local,
                to_char(max(a."timestamp") FILTER (WHERE a.active_power_kw > 0.1)
                    AT TIME ZONE %(tz)s, 'HH24:MI') AS gen_end_local,
                count(*) AS sample_count,
                count(*) FILTER (WHERE COALESCE(a.state_operation, 0) = 2 OR COALESCE(a.working_status, 0) = 1) AS running_samples,
                count(*) FILTER (WHERE COALESCE(a.state_operation, 0) = 3 OR COALESCE(a.working_status, 0) = 8) AS off_samples
            FROM {stg_inv} a
            JOIN public.device d ON d.id = a.device_id
            WHERE a.power_plant_id = %(plant_id)s
            AND a."timestamp" >= (%(day_start)s AT TIME ZONE %(tz)s)
            AND a."timestamp" <  (%(day_end)s   AT TIME ZONE %(tz)s)
            AND d.is_active = true
            GROUP BY a.device_id, d.name
        ),
        prev_day AS (
            SELECT
                a.device_id,
                round(avg(a.active_power_kw)::numeric, 2) AS avg_power_prev_kw
            FROM {stg_inv} a
            JOIN public.device d ON d.id = a.device_id
            WHERE a.power_plant_id = %(plant_id)s
            AND a."timestamp" >= ((%(day_start)s)::timestamp - interval '1 day') AT TIME ZONE %(tz)s
            AND a."timestamp" <  (%(day_start)s AT TIME ZONE %(tz)s)
            AND d.is_active = true
            GROUP BY a.device_id
        )
        SELECT
            i.*,
            p.avg_power_prev_kw
        FROM inv_day i
        LEFT JOIN prev_day p ON p.device_id = i.device_id
        ORDER BY i.inverter_name
    """).format(stg_inv=q(RT_SCHEMA, "stg_inverter_analog")), {
        "plant_id": plant_id,
        "day_start": day_start,
        "day_end": day_end,
        "tz": tz,
    })
    inv_rows = cur.fetchall() or []

    tz_obj = ZoneInfo(tz)

    def ts_to_local(ts):
        if ts is None:
            return None
        if ts.tzinfo is None:
            from datetime import timezone as _tzmod
            ts = ts.replace(tzinfo=_tzmod.utc)
        return ts.astimezone(tz_obj).strftime("%H:%M")

    def classify_delta(current, previous, threshold_pct=10):
        if current is None or previous is None or previous == 0:
            return "sem_dados"
        delta_pct = round(100.0 * (current - previous) / abs(previous), 1)
        if delta_pct > threshold_pct:
            return "acima"
        elif delta_pct < -threshold_pct:
            return "abaixo"
        return "normal"

    def classify_vs_avg(value, fleet_avg, threshold_pct=10):
        if value is None or fleet_avg is None or fleet_avg == 0:
            return "sem_dados"
        delta_pct = round(100.0 * (value - fleet_avg) / abs(fleet_avg), 1)
        if delta_pct > threshold_pct:
            return "acima"
        elif delta_pct < -threshold_pct:
            return "abaixo"
        return "normal"

    num_inverters = len(inv_rows) if inv_rows else 1
    capacity_per_inverter = rated_kwp / num_inverters if num_inverters > 0 and rated_kwp > 0 else 0

    raw_inverters = []
    gen_start_plant = None
    gen_end_plant = None

    for r in inv_rows:
        energy_kwh = float(r["energy_daily_kwh"]) if r.get("energy_daily_kwh") else None
        avg_power = float(r["avg_power_kw"]) if r.get("avg_power_kw") is not None else None
        avg_power_prev = float(r["avg_power_prev_kw"]) if r.get("avg_power_prev_kw") is not None else None

        pr_inverter = None
        if energy_kwh and capacity_per_inverter > 0 and irradiation > 0:
            pr_inverter = round(min(100, max(0, 100.0 * energy_kwh / (capacity_per_inverter * irradiation))), 2)

        gen_start_str = r.get("gen_start_local")
        gen_end_str = r.get("gen_end_local")

        if gen_start_str:
            if gen_start_plant is None or gen_start_str < gen_start_plant:
                gen_start_plant = gen_start_str
        if gen_end_str:
            if gen_end_plant is None or gen_end_str > gen_end_plant:
                gen_end_plant = gen_end_str

        total_s = r.get("sample_count") or 1
        running_pct = round(100.0 * (r.get("running_samples") or 0) / total_s, 1)
        off_pct = round(100.0 * (r.get("off_samples") or 0) / total_s, 1)

        raw_inverters.append({
            "device_id": r["device_id"],
            "inverter_name": r.get("inverter_name"),
            "avg_power_kw": avg_power,
            "max_power_kw": float(r["max_power_kw"]) if r.get("max_power_kw") is not None else None,
            "avg_power_prev_day_kw": avg_power_prev,
            "power_delta_pct": round(100.0 * (avg_power - avg_power_prev) / abs(avg_power_prev), 1) if avg_power and avg_power_prev and avg_power_prev != 0 else None,
            "power_performance": classify_delta(avg_power, avg_power_prev),
            "energy_daily_kwh": energy_kwh,
            "avg_temp_c": float(r["avg_temp_c"]) if r.get("avg_temp_c") is not None else None,
            "max_temp_c": float(r["max_temp_c"]) if r.get("max_temp_c") is not None else None,
            "pr_inverter_pct": pr_inverter,
            "gen_start_time": gen_start_str,
            "gen_end_time": gen_end_str,
            "running_pct": running_pct,
            "off_pct": off_pct,
        })

    fleet_avg_power = None
    fleet_avg_pr = None
    powers = [i["avg_power_kw"] for i in raw_inverters if i["avg_power_kw"] is not None]
    prs = [i["pr_inverter_pct"] for i in raw_inverters if i["pr_inverter_pct"] is not None]
    if powers:
        fleet_avg_power = round(sum(powers) / len(powers), 2)
    if prs:
        fleet_avg_pr = round(sum(prs) / len(prs), 2)

    inverters = []
    for inv in raw_inverters:
        inv["fleet_avg_power_kw"] = fleet_avg_power
        inv["fleet_avg_pr_pct"] = fleet_avg_pr
        inv["power_vs_fleet"] = classify_vs_avg(inv["avg_power_kw"], fleet_avg_power)
        inv["pr_vs_fleet"] = classify_vs_avg(inv["pr_inverter_pct"], fleet_avg_pr)
        inverters.append(inv)

    if plant_summary:
        plant_summary["gen_start_time"] = gen_start_plant
        plant_summary["gen_end_time"] = gen_end_plant
        plant_summary["fleet_avg_power_kw"] = fleet_avg_power
        plant_summary["fleet_avg_pr_pct"] = fleet_avg_pr

    print(f"[daily-round] 3-inverters OK rows={len(inv_rows)}")
    # ── 4. String box (corrente de cada string vs média do inversor no dia) ──
    string_box = []
    try:
        cur.execute("SAVEPOINT sp_strings")
        cur.execute("SET LOCAL statement_timeout = '8000ms'")
        cur.execute(sql.SQL("""
            SELECT
                s.device_id,
                d.name AS inverter_name,
                s.string_index,
                round(avg(s.string_current)::numeric, 2) AS avg_current,
                round(max(s.string_current)::numeric, 2) AS max_current,
                count(*) AS samples,
                count(*) FILTER (WHERE s.string_current < 0.1) AS zero_samples
            FROM {stg_str} s
            JOIN public.device d ON d.id = s.device_id
            WHERE s.power_plant_id = %(plant_id)s
              AND s."timestamp" >= (%(sun_start)s AT TIME ZONE %(tz)s)
              AND s."timestamp" <  (%(sun_end)s   AT TIME ZONE %(tz)s)
              AND d.is_active = true
            GROUP BY s.device_id, d.name, s.string_index
            ORDER BY s.device_id, s.string_index
        """).format(stg_str=q(RT_SCHEMA, "stg_inverter_string")), {
            "plant_id": plant_id,
            "sun_start": f"{target_date}T06:00:00",
            "sun_end": f"{target_date}T18:00:00",
            "tz": tz,
        })
        str_rows = cur.fetchall() or []

        string_box_by_inv = {}
        for r in str_rows:
            did = r["device_id"]
            total = r["samples"] or 1
            zero_pct = round(100.0 * (r["zero_samples"] or 0) / total, 1)
            avg_c = float(r["avg_current"]) if r.get("avg_current") is not None else 0
            status = "zero" if avg_c < 0.1 else ("intermittent" if zero_pct > 50 else "normal")

            if did not in string_box_by_inv:
                string_box_by_inv[did] = {"device_id": did, "inverter_name": r.get("inverter_name"), "strings": []}
            string_box_by_inv[did]["strings"].append({
                "string_index": r["string_index"],
                "avg_current": avg_c,
                "max_current": float(r["max_current"]) if r.get("max_current") is not None else None,
                "zero_pct": zero_pct,
                "status": status,
            })

        for did, inv_sb in string_box_by_inv.items():
            active_strings = [s for s in inv_sb["strings"] if s["status"] == "normal"]
            inv_sb["total_strings"] = len(inv_sb["strings"])
            inv_sb["active_strings"] = len(active_strings) if active_strings else 0
            inv_sb["zero_strings"] = len([s for s in inv_sb["strings"] if s["status"] == "zero"])

            if active_strings:
                inv_avg = sum(s["avg_current"] for s in active_strings) / len(active_strings)
                inv_sb["avg_inverter_current"] = round(inv_avg, 2)
                for s in inv_sb["strings"]:
                    if s["status"] == "normal" and inv_avg > 0:
                        s["avg_current_ref"] = round(inv_avg, 2)
                        s["variation_pct"] = round(((s["avg_current"] - inv_avg) / inv_avg) * 100.0, 1)
                    else:
                        s["avg_current_ref"] = None
                        s["variation_pct"] = None
            else:
                inv_sb["avg_inverter_current"] = None

            variations = [s.get("variation_pct") for s in inv_sb["strings"] if s.get("variation_pct") is not None]
            worst = min(variations) if variations else None
            inv_sb["health_pct"] = worst

        string_box = list(string_box_by_inv.values())
        cur.execute("RELEASE SAVEPOINT sp_strings")
        print(f"[daily-round] 4-strings OK rows={len(str_rows)}")
    except Exception as e:
        print(f"[daily-round] 4-strings TIMEOUT/ERROR: {e!r}")
        cur.execute("ROLLBACK TO SAVEPOINT sp_strings")
        cur.execute("SET LOCAL statement_timeout = '30000ms'")
    # ── 5. Alarms (events of the day) ──
    cur.execute(sql.SQL("""
        SELECT
            e.event_row_id,
            e.timestamp,
            e.device_id,
            e.device_name,
            e.device_type_name,
            e.code,
            e.description_pt,
            e.severity,
            e.is_active_event,
            e.value
        FROM {alarms} e
        WHERE e.power_plant_id = %(plant_id)s
        AND e."timestamp" >= (%(day_start)s AT TIME ZONE %(tz)s)
        AND e."timestamp" <  (%(day_end)s   AT TIME ZONE %(tz)s)
        ORDER BY e."timestamp" DESC
    """).format(alarms=q(RT_SCHEMA, "int_events_alarms")), {
        "plant_id": plant_id,
        "day_start": day_start,
        "day_end": day_end,
        "tz": tz,
    })
    alarm_rows = cur.fetchall() or []

    alarms = []
    for r in alarm_rows:
        alarms.append({
            "event_row_id": r.get("event_row_id"),
            "timestamp": r.get("timestamp"),
            "device_id": r.get("device_id"),
            "device_name": r.get("device_name"),
            "device_type": r.get("device_type_name"),
            "code": r.get("code"),
            "description": r.get("description_pt"),
            "severity": r.get("severity"),
            "is_active": bool(r.get("is_active_event")),
        })

    # ── 6. Trackers (position, deviation) ──
    trackers = []
    try:
        cur.execute(sql.SQL("""
            WITH trk_day AS (
                SELECT
                    t.device_id,
                    d.name AS tracker_name,
                    round(avg(t.posicao_atual)::numeric, 2) AS avg_position,
                    round(avg(t.posicao_alvo)::numeric, 2) AS avg_target,
                    round(avg(t.diferenca_posicao_abs)::numeric, 2) AS avg_deviation,
                    round(max(t.diferenca_posicao_abs)::numeric, 2) AS max_deviation,
                    count(*) AS samples,
                    count(*) FILTER (WHERE t.diferenca_posicao_abs > 5) AS deviation_samples
                FROM {stg_trk} t
                JOIN public.device d ON d.id = t.device_id
                WHERE t.power_plant_id = %(plant_id)s
                  AND t."timestamp" >= (%(day_start)s AT TIME ZONE %(tz)s)
                  AND t."timestamp" <  (%(day_end)s   AT TIME ZONE %(tz)s)
                  AND d.is_active = true
                GROUP BY t.device_id, d.name
            ),
            trk_latest AS (
                SELECT DISTINCT ON (t.device_id)
                    t.device_id,
                    t.posicao_atual AS current_position,
                    t.posicao_alvo AS current_target,
                    t."timestamp" AS last_ts
                FROM {stg_trk} t
                WHERE t.power_plant_id = %(plant_id)s
                  AND t."timestamp" >= (%(day_start)s AT TIME ZONE %(tz)s)
                  AND t."timestamp" <  (%(day_end)s   AT TIME ZONE %(tz)s)
                ORDER BY t.device_id, t."timestamp" DESC
            )
            SELECT td.*, tl.current_position, tl.current_target, tl.last_ts
            FROM trk_day td
            LEFT JOIN trk_latest tl ON tl.device_id = td.device_id
            ORDER BY td.tracker_name
        """).format(stg_trk=q(RT_SCHEMA, "stg_tracker_analog")), {
            "plant_id": plant_id,
            "day_start": day_start,
            "day_end": day_end,
            "tz": tz,
        })
        trk_rows = cur.fetchall() or []
        for r in trk_rows:
            total_s = r.get("samples") or 1
            dev_pct = round(100.0 * (r.get("deviation_samples") or 0) / total_s, 1)
            trackers.append({
                "device_id": r["device_id"],
                "tracker_name": r.get("tracker_name"),
                "avg_position": float(r["avg_position"]) if r.get("avg_position") is not None else None,
                "avg_target": float(r["avg_target"]) if r.get("avg_target") is not None else None,
                "avg_deviation": float(r["avg_deviation"]) if r.get("avg_deviation") is not None else None,
                "max_deviation": float(r["max_deviation"]) if r.get("max_deviation") is not None else None,
                "deviation_pct": dev_pct,
                "current_position": float(r["current_position"]) if r.get("current_position") is not None else None,
                "current_target": float(r["current_target"]) if r.get("current_target") is not None else None,
                "last_ts": r.get("last_ts"),
            })
        print(f"[daily-round] 6-trackers OK rows={len(trk_rows)}")
    except Exception as e:
        print(f"[daily-round] 6-trackers SKIP (no table or error): {e!r}")

    return http_response(200, {
        "date": str(target_date),
        "power_plant_id": plant_id,
        "plant_summary": plant_summary,
        "weather": weather,
        "inverters": inverters,
        "string_box": string_box if string_box else None,
        "trackers": trackers if trackers else None,
        "alarms": alarms,
        "alarm_count": len(alarms),
        "meta": {
            "sources": [
                f"{RT_SCHEMA}.fct_power_plant_metrics_daily",
                f"{RT_SCHEMA}.stg_weather_station_analog",
                f"{RT_SCHEMA}.stg_inverter_analog",
                f"{RT_SCHEMA}.stg_inverter_string",
                f"{RT_SCHEMA}.stg_tracker_analog",
                f"{RT_SCHEMA}.int_events_alarms",
            ]
        }
    })


# ============================================================
# REPORT — multi-day performance report
# ============================================================

def handle_get_report(cur, plant_id: int, ctx: dict, params: dict):
    print(f"[report] START plant_id={plant_id}")
    cur.execute("SET LOCAL statement_timeout = '45000ms';")

    if not ensure_plant_access(cur, int(plant_id), ctx):
        return http_response(403, {"error": "sem permissão para esta usina"})

    tz = "America/Fortaleza"

    start_str = (params.get("start") or "").strip()
    end_str = (params.get("end") or "").strip()
    today_row = None
    cur.execute("SELECT (now() AT TIME ZONE %s)::date AS d", (tz,))
    today_row = cur.fetchone()
    today = today_row["d"]

    if start_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        except ValueError:
            return http_response(400, {"error": "start inválido, use YYYY-MM-DD"})
    else:
        start_date = today - timedelta(days=7)

    if end_str:
        try:
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        except ValueError:
            return http_response(400, {"error": "end inválido, use YYYY-MM-DD"})
    else:
        end_date = today - timedelta(days=1)

    if (end_date - start_date).days > 30:
        return http_response(400, {"error": "período máximo: 30 dias"})
    if end_date < start_date:
        return http_response(400, {"error": "end deve ser >= start"})

    num_days = (end_date - start_date).days + 1
    print(f"[report] period={start_date} ~ {end_date} ({num_days} days)")

    # ── 1. Daily KPIs from fct_power_plant_metrics_daily ──
    cur.execute(sql.SQL("""
        SELECT
            f.date_day,
            f.power_plant_name,
            f.rated_power_kwp,
            f.rated_power_ac_kw,
            f.generation_daily_kwh,
            f.generation_liquid_meter_kwh,
            f.irradiation_daily_kwh_m2,
            f.pr_daily_pct,
            f.pr_accumulated_pct,
            f.capacity_factor_daily_pct,
            f.generation_accumulated_kwh,
            f.irradiation_accumulated_kwh_m2
        FROM {fct} f
        WHERE f.power_plant_id = %(plant_id)s
          AND f.date_day >= %(start)s
          AND f.date_day <= %(end)s
        ORDER BY f.date_day
    """).format(fct=q(RT_SCHEMA, "fct_power_plant_metrics_daily")), {
        "plant_id": plant_id, "start": start_date, "end": end_date,
    })
    fct_rows = cur.fetchall() or []

    plant_name = fct_rows[0]["power_plant_name"] if fct_rows else ""
    rated_kwp = float(fct_rows[0]["rated_power_kwp"]) if fct_rows and fct_rows[0].get("rated_power_kwp") else 0

    daily_trend = []
    total_gen = 0.0
    pr_vals = []
    fc_vals = []
    irr_vals = []
    for r in fct_rows:
        gen = float(r["generation_daily_kwh"] or 0)
        pr = float(r["pr_daily_pct"]) if r.get("pr_daily_pct") is not None else None
        fc = float(r["capacity_factor_daily_pct"]) if r.get("capacity_factor_daily_pct") is not None else None
        irr = float(r["irradiation_daily_kwh_m2"] or 0)
        total_gen += gen
        if pr is not None:
            pr_vals.append(pr)
        if fc is not None:
            fc_vals.append(fc)
        if irr > 0:
            irr_vals.append(irr)
        daily_trend.append({
            "date": str(r["date_day"]),
            "generation_kwh": round(gen, 1),
            "pr_pct": round(pr, 1) if pr is not None else None,
            "capacity_factor_pct": round(fc, 1) if fc is not None else None,
            "irradiation_kwh_m2": round(irr, 3) if irr else None,
        })

    avg_pr = round(sum(pr_vals) / len(pr_vals), 1) if pr_vals else None
    avg_fc = round(sum(fc_vals) / len(fc_vals), 1) if fc_vals else None

    summary = {
        "total_generation_kwh": round(total_gen, 1),
        "avg_pr_pct": avg_pr,
        "avg_capacity_factor_pct": avg_fc,
        "operating_days": len(fct_rows),
    }

    print(f"[report] 1-daily OK rows={len(fct_rows)}")

    # ── 2. Monthly comparison (current month vs previous month) ──
    cur_month_start = end_date.replace(day=1)
    prev_month_end = cur_month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    cur.execute(sql.SQL("""
        SELECT
            to_char(date_day, 'YYYY-MM') AS month,
            round(sum(generation_daily_kwh)::numeric, 1) AS total_gen,
            round(avg(pr_daily_pct)::numeric, 1) AS avg_pr,
            round(avg(capacity_factor_daily_pct)::numeric, 1) AS avg_fc,
            round(avg(irradiation_daily_kwh_m2)::numeric, 3) AS avg_irr,
            count(*) AS days
        FROM {fct} f
        WHERE f.power_plant_id = %(plant_id)s
          AND f.date_day >= %(prev_start)s
          AND f.date_day <= %(cur_end)s
        GROUP BY to_char(date_day, 'YYYY-MM')
        ORDER BY month
    """).format(fct=q(RT_SCHEMA, "fct_power_plant_metrics_daily")), {
        "plant_id": plant_id,
        "prev_start": prev_month_start,
        "cur_end": end_date,
    })
    month_rows = {r["month"]: r for r in (cur.fetchall() or [])}

    cur_m_key = end_date.strftime("%Y-%m")
    prev_m_key = prev_month_start.strftime("%Y-%m")
    cur_m = month_rows.get(cur_m_key, {})
    prev_m = month_rows.get(prev_m_key, {})

    def _safe_float(val):
        return float(val) if val is not None else None

    def _delta_pct(cur_val, prev_val):
        if cur_val is None or prev_val is None or prev_val == 0:
            return None
        return round(100.0 * (cur_val - prev_val) / abs(prev_val), 1)

    monthly_comparison = {
        "current_month": cur_m_key,
        "current_generation_kwh": _safe_float(cur_m.get("total_gen")),
        "current_pr_pct": _safe_float(cur_m.get("avg_pr")),
        "current_fc_pct": _safe_float(cur_m.get("avg_fc")),
        "current_irradiance_wm2": _safe_float(cur_m.get("avg_irr")),
        "current_days": int(cur_m.get("days", 0)),
        "previous_month": prev_m_key,
        "previous_generation_kwh": _safe_float(prev_m.get("total_gen")),
        "previous_pr_pct": _safe_float(prev_m.get("avg_pr")),
        "previous_fc_pct": _safe_float(prev_m.get("avg_fc")),
        "previous_irradiance_wm2": _safe_float(prev_m.get("avg_irr")),
        "previous_days": int(prev_m.get("days", 0)),
    }
    monthly_comparison["delta_generation_pct"] = _delta_pct(
        monthly_comparison["current_generation_kwh"],
        monthly_comparison["previous_generation_kwh"])
    monthly_comparison["delta_pr_pct"] = _delta_pct(
        monthly_comparison["current_pr_pct"],
        monthly_comparison["previous_pr_pct"])
    monthly_comparison["delta_fc_pct"] = _delta_pct(
        monthly_comparison["current_fc_pct"],
        monthly_comparison["previous_fc_pct"])
    monthly_comparison["delta_irradiance_pct"] = _delta_pct(
        monthly_comparison["current_irradiance_wm2"],
        monthly_comparison["previous_irradiance_wm2"])

    print("[report] 2-monthly OK")

    # ── 3. Weather summary for the period ──
    cur.execute(sql.SQL("""
        SELECT
            round(avg(COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2))::numeric, 1) AS avg_irradiance_wm2,
            round(max(COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2))::numeric, 1) AS max_irradiance_wm2,
            round(avg(air_temperature_c)::numeric, 1) AS avg_temp_c,
            round(max(air_temperature_c)::numeric, 1) AS max_temp_c,
            round(avg(wind_speed)::numeric, 1) AS avg_wind_speed
        FROM {ws}
        WHERE power_plant_id = %(plant_id)s
          AND "timestamp" >= (%(start)s::text || 'T06:00:00')::timestamp AT TIME ZONE %(tz)s
          AND "timestamp" <  ((%(end)s + 1)::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
          AND COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2) > 5
    """).format(ws=q(RT_SCHEMA, "stg_weather_station_analog")), {
        "plant_id": plant_id, "start": start_date, "end": end_date, "tz": tz,
    })
    w_row = cur.fetchone() or {}

    cur.execute(sql.SQL("""
        SELECT
            date_trunc('day', "timestamp" AT TIME ZONE %(tz)s)::date AS d,
            round(avg(COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2))::numeric, 1) AS avg_irradiance,
            bool_or(rain_signal > 0) AS rain
        FROM {ws}
        WHERE power_plant_id = %(plant_id)s
          AND "timestamp" >= (%(start)s::text || 'T06:00:00')::timestamp AT TIME ZONE %(tz)s
          AND "timestamp" <  ((%(end)s + 1)::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
          AND COALESCE(irradiance_poa_wm2, irradiance_ghi_wm2) > 5
        GROUP BY d ORDER BY d
    """).format(ws=q(RT_SCHEMA, "stg_weather_station_analog")), {
        "plant_id": plant_id, "start": start_date, "end": end_date, "tz": tz,
    })
    w_daily = cur.fetchall() or []
    rain_days = sum(1 for r in w_daily if r.get("rain"))
    daily_irradiance = [float(r["avg_irradiance"]) if r.get("avg_irradiance") else 0 for r in w_daily]

    weather = {
        "avg_irradiance_wm2": _safe_float(w_row.get("avg_irradiance_wm2")),
        "max_irradiance_wm2": _safe_float(w_row.get("max_irradiance_wm2")),
        "avg_temp_c": _safe_float(w_row.get("avg_temp_c")),
        "max_temp_c": _safe_float(w_row.get("max_temp_c")),
        "avg_wind_speed": _safe_float(w_row.get("avg_wind_speed")),
        "rain_days": rain_days,
        "total_days": len(w_daily),
        "daily_irradiance": daily_irradiance,
    }

    summary["avg_irradiance_wm2"] = weather["avg_irradiance_wm2"]

    print(f"[report] 3-weather OK days={len(w_daily)}")

    # ── 4. Inverters: period averages + daily energy for sparklines ──
    cur.execute(sql.SQL("""
        WITH inv_period AS (
            SELECT
                a.device_id,
                d.name AS inverter_name,
                round(avg(a.active_power_kw)::numeric, 2) AS avg_power_kw,
                round(max(a.active_power_kw)::numeric, 2) AS max_power_kw,
                round(avg(a.temperature_internal_c)::numeric, 1) AS avg_temp_c,
                count(*) AS total_samples,
                count(*) FILTER (WHERE COALESCE(a.state_operation, 0) = 2
                    OR COALESCE(a.working_status, 0) = 1) AS running_samples
            FROM {stg_inv} a
            JOIN public.device d ON d.id = a.device_id
            WHERE a.power_plant_id = %(plant_id)s
              AND a."timestamp" >= (%(start)s::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
              AND a."timestamp" <  ((%(end)s + 1)::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
              AND d.is_active = true
            GROUP BY a.device_id, d.name
        ),
        inv_daily AS (
            SELECT
                a.device_id,
                date_trunc('day', a."timestamp" AT TIME ZONE %(tz)s)::date AS d,
                max(a.daily_active_energy_kwh) AS energy_kwh
            FROM {stg_inv} a
            JOIN public.device d ON d.id = a.device_id
            WHERE a.power_plant_id = %(plant_id)s
              AND a."timestamp" >= (%(start)s::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
              AND a."timestamp" <  ((%(end)s + 1)::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
              AND d.is_active = true
            GROUP BY a.device_id, d
        )
        SELECT
            p.*,
            COALESCE(
                (SELECT json_agg(json_build_object('date', dd.d, 'energy_kwh', round(dd.energy_kwh::numeric, 1))
                    ORDER BY dd.d)
                 FROM inv_daily dd WHERE dd.device_id = p.device_id),
                '[]'::json
            ) AS daily_energy
        FROM inv_period p
        ORDER BY p.inverter_name
    """).format(stg_inv=q(RT_SCHEMA, "stg_inverter_analog")), {
        "plant_id": plant_id, "start": start_date, "end": end_date, "tz": tz,
    })
    inv_rows = cur.fetchall() or []

    num_inverters = len(inv_rows) or 1
    cap_per_inv = rated_kwp / num_inverters if rated_kwp > 0 else 0

    raw_inverters = []
    for r in inv_rows:
        daily_e = r.get("daily_energy") or []
        if isinstance(daily_e, str):
            import json as _json
            daily_e = _json.loads(daily_e)
        energies = [float(de.get("energy_kwh") or 0) for de in daily_e]
        total_energy = sum(energies)

        avg_irr_period = sum(irr_vals) / len(irr_vals) if irr_vals else 0
        pr_inv = None
        if total_energy > 0 and cap_per_inv > 0 and avg_irr_period > 0:
            pr_inv = round(min(100, max(0, 100.0 * total_energy / (cap_per_inv * avg_irr_period * num_days))), 1)

        ts = r.get("total_samples") or 1
        avail_pct = round(100.0 * (r.get("running_samples") or 0) / ts, 1)

        raw_inverters.append({
            "device_id": r["device_id"],
            "inverter_name": r.get("inverter_name"),
            "avg_power_kw": float(r["avg_power_kw"]) if r.get("avg_power_kw") is not None else None,
            "max_power_kw": float(r["max_power_kw"]) if r.get("max_power_kw") is not None else None,
            "total_energy_kwh": round(total_energy, 1),
            "avg_pr_pct": pr_inv,
            "avg_temp_c": float(r["avg_temp_c"]) if r.get("avg_temp_c") is not None else None,
            "availability_pct": avail_pct,
            "daily_energy": energies,
        })

    fleet_prs = [i["avg_pr_pct"] for i in raw_inverters if i["avg_pr_pct"] is not None]
    fleet_powers = [i["avg_power_kw"] for i in raw_inverters if i["avg_power_kw"] is not None]
    fleet_avg_pr = round(sum(fleet_prs) / len(fleet_prs), 1) if fleet_prs else None
    fleet_avg_power = round(sum(fleet_powers) / len(fleet_powers), 2) if fleet_powers else None

    def _classify(value, avg, threshold=10):
        if value is None or avg is None or avg == 0:
            return "sem_dados"
        d = 100.0 * (value - avg) / abs(avg)
        if d > threshold:
            return "acima"
        elif d < -threshold:
            return "abaixo"
        return "normal"

    inverters = []
    for inv in raw_inverters:
        inv["vs_fleet"] = _classify(inv["avg_pr_pct"], fleet_avg_pr)
        inv["fleet_avg_pr_pct"] = fleet_avg_pr
        inv["fleet_avg_power_kw"] = fleet_avg_power
        inverters.append(inv)

    print(f"[report] 4-inverters OK rows={len(inv_rows)}")

    # ── 5. String box heatmap (per-day, per-string, 6h-18h) ──
    string_box_heatmap = []
    try:
        cur.execute("SAVEPOINT sp_report_strings")
        cur.execute("SET LOCAL statement_timeout = '12000ms'")
        cur.execute(sql.SQL("""
            SELECT
                s.device_id,
                d.name AS inverter_name,
                s.string_index,
                date_trunc('day', s."timestamp" AT TIME ZONE %(tz)s)::date AS d,
                round(avg(s.string_current)::numeric, 2) AS avg_current,
                count(*) AS samples,
                count(*) FILTER (WHERE s.string_current < 0.1) AS zero_samples
            FROM {stg_str} s
            JOIN public.device d ON d.id = s.device_id
            WHERE s.power_plant_id = %(plant_id)s
              AND s."timestamp" >= (%(sun_start)s AT TIME ZONE %(tz)s)
              AND s."timestamp" <  (%(sun_end)s AT TIME ZONE %(tz)s)
              AND d.is_active = true
            GROUP BY s.device_id, d.name, s.string_index, date_trunc('day', s."timestamp" AT TIME ZONE %(tz)s)
            ORDER BY s.device_id, s.string_index, d
        """).format(stg_str=q(RT_SCHEMA, "stg_inverter_string")), {
            "plant_id": plant_id,
            "sun_start": f"{start_date}T06:00:00",
            "sun_end": f"{end_date + timedelta(days=1)}T18:00:00",
            "tz": tz,
        })
        str_rows = cur.fetchall() or []

        inv_str_map = {}
        for r in str_rows:
            did = r["device_id"]
            si = r["string_index"]
            day = str(r["d"])
            avg_c = float(r["avg_current"]) if r.get("avg_current") is not None else 0

            if did not in inv_str_map:
                inv_str_map[did] = {"device_id": did, "inverter_name": r.get("inverter_name"), "strings_map": {}}
            if si not in inv_str_map[did]["strings_map"]:
                inv_str_map[did]["strings_map"][si] = []
            inv_str_map[did]["strings_map"][si].append({
                "date": day, "avg_current": avg_c,
            })

        for did, inv_data in inv_str_map.items():
            all_daily_avgs = {}
            for si, days_list in inv_data["strings_map"].items():
                for dd in days_list:
                    d_key = dd["date"]
                    if d_key not in all_daily_avgs:
                        all_daily_avgs[d_key] = []
                    if dd["avg_current"] >= 0.1:
                        all_daily_avgs[d_key].append(dd["avg_current"])

            daily_inv_avg = {}
            for d_key, vals in all_daily_avgs.items():
                daily_inv_avg[d_key] = sum(vals) / len(vals) if vals else 0

            strings_out = []
            for si in sorted(inv_data["strings_map"].keys()):
                daily_items = []
                for dd in inv_data["strings_map"][si]:
                    inv_avg = daily_inv_avg.get(dd["date"], 0)
                    var_pct = None
                    status = "zero"
                    if dd["avg_current"] >= 0.1 and inv_avg > 0:
                        var_pct = round(((dd["avg_current"] - inv_avg) / inv_avg) * 100.0, 1)
                        status = "normal" if var_pct >= -5 else ("warning" if var_pct >= -15 else "critical")
                    elif dd["avg_current"] >= 0.1:
                        status = "normal"
                    daily_items.append({
                        "date": dd["date"],
                        "avg_current": round(dd["avg_current"], 2),
                        "variation_pct": var_pct,
                        "status": status,
                    })
                strings_out.append({"string_index": si, "daily": daily_items})

            avg_inv_current = None
            all_avgs = [v for v in daily_inv_avg.values() if v > 0]
            if all_avgs:
                avg_inv_current = round(sum(all_avgs) / len(all_avgs), 2)

            string_box_heatmap.append({
                "device_id": did,
                "inverter_name": inv_data["inverter_name"],
                "avg_inverter_current": avg_inv_current,
                "strings": strings_out,
            })

        cur.execute("RELEASE SAVEPOINT sp_report_strings")
        print(f"[report] 5-strings OK rows={len(str_rows)}")
    except Exception as e:
        print(f"[report] 5-strings TIMEOUT/ERROR: {e!r}")
        cur.execute("ROLLBACK TO SAVEPOINT sp_report_strings")
        cur.execute("SET LOCAL statement_timeout = '45000ms'")

    # ── 6. Alarms summary grouped by device ──
    cur.execute(sql.SQL("""
        SELECT
            e.device_name,
            e.device_type_name AS device_type,
            count(*) AS total_count,
            count(*) FILTER (WHERE e.severity IN ('high', 'critical')) AS critical_count,
            count(*) FILTER (WHERE e.severity = 'medium') AS medium_count,
            count(*) FILTER (WHERE e.severity = 'low') AS low_count,
            mode() WITHIN GROUP (ORDER BY e.description_pt) AS top_alarm
        FROM {alarms} e
        WHERE e.power_plant_id = %(plant_id)s
          AND e."timestamp" >= (%(start)s::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
          AND e."timestamp" <  ((%(end)s + 1)::text || 'T00:00:00')::timestamp AT TIME ZONE %(tz)s
        GROUP BY e.device_name, e.device_type_name
        ORDER BY critical_count DESC, total_count DESC
    """).format(alarms=q(RT_SCHEMA, "int_events_alarms")), {
        "plant_id": plant_id, "start": start_date, "end": end_date, "tz": tz,
    })
    alarm_rows = cur.fetchall() or []
    alarms_summary = []
    total_alarms = 0
    for r in alarm_rows:
        alarms_summary.append({
            "device_name": r.get("device_name"),
            "device_type": r.get("device_type"),
            "critical_count": int(r.get("critical_count") or 0),
            "medium_count": int(r.get("medium_count") or 0),
            "low_count": int(r.get("low_count") or 0),
            "total_count": int(r.get("total_count") or 0),
            "top_alarm": r.get("top_alarm"),
        })
        total_alarms += int(r.get("total_count") or 0)

    print(f"[report] 6-alarms OK devices={len(alarm_rows)}")

    # ── 6b. Trackers (period averages + daily positions) ──
    report_trackers = []
    try:
        cur.execute(sql.SQL("""
            WITH trk_period AS (
                SELECT
                    t.device_id,
                    d.name AS tracker_name,
                    round(avg(t.posicao_atual)::numeric, 2) AS avg_position,
                    round(avg(t.posicao_alvo)::numeric, 2) AS avg_target,
                    round(avg(t.diferenca_posicao_abs)::numeric, 2) AS avg_deviation,
                    round(max(t.diferenca_posicao_abs)::numeric, 2) AS max_deviation,
                    count(*) AS samples,
                    count(*) FILTER (WHERE t.diferenca_posicao_abs > 5) AS deviation_samples
                FROM {stg_trk} t
                JOIN public.device d ON d.id = t.device_id
                WHERE t.power_plant_id = %(plant_id)s
                  AND t."timestamp" >= (%(start)s::text || 'T06:00:00')::timestamp AT TIME ZONE %(tz)s
                  AND t."timestamp" <  ((%(end)s + 1)::text || 'T18:00:00')::timestamp AT TIME ZONE %(tz)s
                  AND d.is_active = true
                GROUP BY t.device_id, d.name
            ),
            trk_daily AS (
                SELECT
                    t.device_id,
                    date_trunc('day', t."timestamp" AT TIME ZONE %(tz)s)::date AS d,
                    round(avg(t.diferenca_posicao_abs)::numeric, 2) AS avg_dev
                FROM {stg_trk} t
                JOIN public.device d ON d.id = t.device_id
                WHERE t.power_plant_id = %(plant_id)s
                  AND t."timestamp" >= (%(start)s::text || 'T06:00:00')::timestamp AT TIME ZONE %(tz)s
                  AND t."timestamp" <  ((%(end)s + 1)::text || 'T18:00:00')::timestamp AT TIME ZONE %(tz)s
                  AND d.is_active = true
                GROUP BY t.device_id, d
            )
            SELECT
                p.*,
                COALESCE(
                    (SELECT json_agg(json_build_object('date', dd.d, 'avg_deviation', dd.avg_dev) ORDER BY dd.d)
                     FROM trk_daily dd WHERE dd.device_id = p.device_id),
                    '[]'::json
                ) AS daily_deviation
            FROM trk_period p
            ORDER BY p.tracker_name
        """).format(stg_trk=q(RT_SCHEMA, "stg_tracker_analog")), {
            "plant_id": plant_id, "start": start_date, "end": end_date, "tz": tz,
        })
        for r in (cur.fetchall() or []):
            total_s = r.get("samples") or 1
            dev_pct = round(100.0 * (r.get("deviation_samples") or 0) / total_s, 1)
            daily_dev = r.get("daily_deviation") or []
            if isinstance(daily_dev, str):
                import json as _json2
                daily_dev = _json2.loads(daily_dev)
            report_trackers.append({
                "device_id": r["device_id"],
                "tracker_name": r.get("tracker_name"),
                "avg_position": float(r["avg_position"]) if r.get("avg_position") is not None else None,
                "avg_target": float(r["avg_target"]) if r.get("avg_target") is not None else None,
                "avg_deviation": float(r["avg_deviation"]) if r.get("avg_deviation") is not None else None,
                "max_deviation": float(r["max_deviation"]) if r.get("max_deviation") is not None else None,
                "deviation_pct": dev_pct,
                "daily_deviation": [float(d.get("avg_deviation") or 0) for d in daily_dev],
            })
        print(f"[report] 6b-trackers OK rows={len(report_trackers)}")
    except Exception as e:
        print(f"[report] 6b-trackers SKIP: {e!r}")

    # ── 7. Auto-generated diagnostic text ──
    diag = []
    diag.append({
        "type": "info",
        "text": f"A usina {plant_name} operou no período de {start_date.strftime('%d/%m')} a {end_date.strftime('%d/%m/%Y')} com geração total de {total_gen:,.1f} kWh"
               + (f" e PR médio de {avg_pr}%." if avg_pr is not None else "."),
    })

    for inv in inverters:
        if inv["vs_fleet"] == "abaixo":
            inv_alarms = sum(a["critical_count"] for a in alarms_summary if a["device_name"] == inv["inverter_name"])
            txt = f"O inversor {inv['inverter_name']} apresentou performance abaixo da média (PR {inv['avg_pr_pct']}%"
            if fleet_avg_pr:
                delta = round(((inv["avg_pr_pct"] or 0) - fleet_avg_pr) / fleet_avg_pr * 100, 1) if inv["avg_pr_pct"] else 0
                txt += f", {delta:+.1f}% vs média da frota"
            txt += ")"
            if inv_alarms > 0:
                txt += f" com {inv_alarms} alarme(s) crítico(s)"
            txt += ". Recomenda-se inspeção."
            diag.append({"type": "warning", "text": txt})

    for sb in string_box_heatmap:
        for s in sb.get("strings", []):
            zero_days = sum(1 for d in s.get("daily", []) if d.get("status") == "zero")
            if zero_days >= 2:
                diag.append({
                    "type": "warning",
                    "text": f"String S{s['string_index']} do {sb['inverter_name']} apresentou corrente zerada em {zero_days} dia(s) do período. Possível falha de conexão.",
                })

    ok_invs = [i for i in inverters if i["vs_fleet"] in ("normal", "acima")]
    if ok_invs and len(ok_invs) < len(inverters):
        diag.append({"type": "ok", "text": "Os demais inversores operaram dentro dos parâmetros normais."})
    elif ok_invs and len(ok_invs) == len(inverters):
        diag.append({"type": "ok", "text": "Todos os inversores operaram dentro dos parâmetros normais."})

    if weather.get("avg_irradiance_wm2") and weather["avg_irradiance_wm2"] >= 400:
        diag.append({"type": "ok", "text": f"Irradiação média do período ({weather['avg_irradiance_wm2']} W/m²) dentro do esperado para a região."})

    if total_alarms == 0:
        diag.append({"type": "ok", "text": "Nenhum alarme registrado no período."})

    print(f"[report] 7-diagnostic OK items={len(diag)}")

    return http_response(200, {
        "period": {
            "start": str(start_date),
            "end": str(end_date),
            "days": num_days,
            "power_plant_name": plant_name,
            "power_plant_id": plant_id,
        },
        "summary": summary,
        "monthly_comparison": monthly_comparison,
        "daily_trend": daily_trend,
        "inverters": inverters,
        "string_box_heatmap": string_box_heatmap if string_box_heatmap else None,
        "trackers": report_trackers if report_trackers else None,
        "weather": weather,
        "diagnostic_text": diag,
    })


# ============================================================
# LAMBDA HANDLER
# ============================================================

def handle_get_datastudio_export(cur, ctx: dict, params: dict):
    t0 = time.perf_counter()
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    selection_id = params.get("selection_id")
    if not selection_id or not str(selection_id).isdigit():
        return http_response(400, {"error": "selection_id inválido"})

    selection_id = int(selection_id)
    max_points_per_series = 50000

    cur.execute("""
        SELECT
            id,
            customer_id,
            user_id,
            selection_name,
            power_plant_id,
            start_ts,
            end_ts,
            historico_aggregation_default,
            consolidado_period_default,
            timezone,
            created_at
        FROM app.user_selection
        WHERE id = %(selection_id)s
        AND (%(is_superuser)s = true OR customer_id = %(customer_id)s)
        LIMIT 1;
    """, {
        "selection_id": selection_id,
        "customer_id": ctx["customer_id"],
        "is_superuser": ctx["is_superuser"],
    })

    selection = cur.fetchone()
    if not selection:
        return http_response(404, {"error": "selection não encontrada"})

    cur.execute("""
        SELECT
            usi.id,
            usi.selection_id,
            usi.tag_id,
            usi.pathname,
            usi.aggregation_override,
            usi.period_override,
            usi.source_override,
            usi.display_type,
            usi.series_order,
            usi.source,
            usi.unit,
            usi.label,
            usi.data_kind,
            usi.created_at,
            tc.source AS tag_source,
            tc.device_type,
            tc.device_id,
            tc.context,
            tc.point_name,
            tc.description
        FROM app.user_selection_item usi
        LEFT JOIN app.tag_catalog tc
        ON tc.id = usi.tag_id
        WHERE usi.selection_id = %(selection_id)s
        ORDER BY usi.series_order, usi.id;
    """, {
        "selection_id": selection_id
    })

    items = cur.fetchall() or []

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    writer.writerow([
        "selection_id",
        "selection_name",
        "power_plant_id",
        "pathname",
        "label",
        "context",
        "device_type",
        "device_id",
        "source",
        "period",
        "aggregation",
        "aggregation_resolved",
        "resolved_route",
        "unit",
        "data_kind",
        "ts",
        "value"
    ])

    if not items:
        filename = f"datastudio_export_{selection_id}.csv"
        return file_response(200, output.getvalue(), filename)

    power_plant_id = selection.get("power_plant_id")
    start_ts = selection.get("start_ts")
    end_ts = selection.get("end_ts")
    historico_default = selection.get("historico_aggregation_default")
    consolidado_default = selection.get("consolidado_period_default")

    tz_name = selection.get("timezone") or "America/Fortaleza"
    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = ZoneInfo("America/Fortaleza")

    if power_plant_id is None:
        return http_response(400, {"error": "selection sem power_plant_id"})

    route_hits = {}
    rows_written = 0

    for item in items:
        pathname = item.get("pathname")
        if not pathname:
            continue

        source_effective = item.get("source_override") or item.get("source") or item.get("tag_source") or "historico"
        aggregation_effective = item.get("aggregation_override") or historico_default or "avg"
        period_effective = item.get("period_override") or consolidado_default or "daily"

        resolved_route = "timeseries"
        aggregation_resolved = aggregation_effective

        route_type = datastudio_resolve_source_type(pathname, source_effective)

        if route_type in ("daily", "monthly"):
            period_for_query = datastudio_period_for_fixed_route(route_type, period_effective)
            rows, resolved_route = fetch_datastudio_points_consolidado(
                cur,
                customer_id=int(selection["customer_id"]),
                power_plant_id=int(power_plant_id),
                pathname=pathname,
                start_ts=start_ts,
                end_ts=end_ts,
                period=period_for_query,
                limit=max_points_per_series,
            )
        elif route_type == "historico":
            rows, resolved_route, aggregation_resolved = fetch_datastudio_points_historico(
                cur,
                customer_id=int(selection["customer_id"]),
                power_plant_id=int(power_plant_id),
                pathname=pathname,
                start_ts=start_ts,
                end_ts=end_ts,
                aggregation=aggregation_effective,
                limit=max_points_per_series,
            )
        elif route_type == "consolidado":
            rows, resolved_route = fetch_datastudio_points_consolidado(
                cur,
                customer_id=int(selection["customer_id"]),
                power_plant_id=int(power_plant_id),
                pathname=pathname,
                start_ts=start_ts,
                end_ts=end_ts,
                period=period_effective,
                limit=max_points_per_series,
            )
        else:
            rows = fetch_datastudio_points_from_timeseries(
                cur,
                customer_id=int(selection["customer_id"]),
                power_plant_id=int(power_plant_id),
                pathname=pathname,
                start_ts=start_ts,
                end_ts=end_ts,
                source=source_effective,
                limit=max_points_per_series,
            )
            resolved_route = "timeseries"

        route_hits[resolved_route] = route_hits.get(resolved_route, 0) + 1

        for row in rows:
            v = row.get("value")
            try:
                v = float(v) if v is not None else None
            except Exception:
                v = None

            ts_raw = row.get("ts")
            if isinstance(ts_raw, datetime):
                if ts_raw.tzinfo is None:
                    ts_raw = ts_raw.replace(tzinfo=_tz.utc)
                ts_local = ts_raw.astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")
            elif ts_raw is not None:
                try:
                    dt = datetime.fromisoformat(str(ts_raw))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=_tz.utc)
                    ts_local = dt.astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts_local = str(ts_raw)
            else:
                ts_local = ""

            rows_written += 1
            writer.writerow([
                selection_id,
                selection.get("selection_name"),
                power_plant_id,
                pathname,
                item.get("label") or item.get("description") or item.get("point_name") or pathname,
                item.get("context"),
                item.get("device_type"),
                item.get("device_id"),
                source_effective,
                period_effective,
                aggregation_effective,
                aggregation_resolved,
                resolved_route,
                item.get("unit"),
                item.get("data_kind"),
                ts_local,
                v
            ])

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    print(f"[/datastudio/export] selection_id={selection_id} rows={rows_written} routes={json.dumps(route_hits, ensure_ascii=False)} elapsed_ms={elapsed_ms}")

    csv_content = output.getvalue()
    filename = f"datastudio_export_{selection_id}.csv"
    return file_response(200, csv_content, filename)


def _build_openapi_spec():
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "AIOTI SCADA API",
            "version": "1.0.0",
            "description": "API do dashboard SCADA solar — endpoints curados + acesso a dados raw de ingestão.",
        },
        "paths": {
            "/plants/summary": {
                "get": {
                    "summary": "Resumo do portfólio de usinas",
                    "tags": ["Portfólio"],
                    "description": "Retorna métricas agregadas de todas as usinas do cliente (potência, energia, PR, alarmes).",
                    "responses": {"200": {"description": "Lista de usinas com métricas"}},
                }
            },
            "/plants": {
                "get": {
                    "summary": "Listar usinas",
                    "tags": ["Portfólio"],
                    "description": "Retorna lista detalhada de usinas ativas com capacidade, geração, status.",
                    "responses": {"200": {"description": "Array de usinas"}},
                }
            },
            "/plants/{plant_id}/realtime": {
                "get": {
                    "summary": "Dados realtime de uma usina",
                    "tags": ["Usina"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                        {"name": "view", "in": "query", "schema": {"type": "string", "enum": ["daily-round", "report"]}},
                    ],
                    "responses": {"200": {"description": "Dados realtime (potência, weather, alarmes)"}},
                }
            },
            "/plants/{plant_id}/inverters/realtime": {
                "get": {
                    "summary": "Estado realtime dos inversores",
                    "tags": ["Usina"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Lista de inversores com potência, eficiência, temp, status"}},
                }
            },
            "/plants/{plant_id}/relay/realtime": {
                "get": {
                    "summary": "Dados realtime do relé",
                    "tags": ["Usina"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Dados do relé de proteção"}},
                }
            },
            "/plants/{plant_id}/multimeter/realtime": {
                "get": {
                    "summary": "Dados realtime do multimedidor",
                    "tags": ["Usina"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Medições elétricas do medidor"}},
                }
            },
            "/plants/{plant_id}/trackers/realtime": {
                "get": {
                    "summary": "Dados realtime dos trackers",
                    "tags": ["Usina"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Posição e estado dos trackers"}},
                }
            },
            "/plants/{plant_id}/energy/daily": {
                "get": {
                    "summary": "Curva de potência do dia (intraday)",
                    "tags": ["Energia"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Labels, potência ativa, irradiância, curva esperada"}},
                }
            },
            "/plants/{plant_id}/energy/monthly": {
                "get": {
                    "summary": "Geração diária do mês",
                    "tags": ["Energia"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                        {"name": "year", "in": "query", "schema": {"type": "integer"}},
                        {"name": "month", "in": "query", "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Geração real vs esperada por dia"}},
                }
            },
            "/plants/{plant_id}/daily-round": {
                "get": {
                    "summary": "Ronda diária da usina",
                    "tags": ["Usina"],
                    "parameters": [
                        {"name": "plant_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Snapshot completo para ronda"}},
                }
            },
            "/events": {
                "get": {
                    "summary": "Eventos e alarmes (global)",
                    "tags": ["Eventos"],
                    "parameters": [
                        {"name": "start_time", "in": "query", "required": True, "schema": {"type": "string", "format": "date-time"}},
                        {"name": "end_time", "in": "query", "required": True, "schema": {"type": "string", "format": "date-time"}},
                        {"name": "plant_id", "in": "query", "schema": {"type": "integer"}},
                        {"name": "device_id", "in": "query", "schema": {"type": "integer"}},
                        {"name": "severity", "in": "query", "schema": {"type": "string", "enum": ["low", "medium", "high"]}},
                        {"name": "source", "in": "query", "schema": {"type": "string", "enum": ["inverter", "relay", "weather"]}},
                        {"name": "status", "in": "query", "schema": {"type": "string", "enum": ["active", "inactive"]}},
                        {"name": "page", "in": "query", "schema": {"type": "integer", "default": 1}},
                        {"name": "page_size", "in": "query", "schema": {"type": "integer", "default": 50}},
                    ],
                    "responses": {"200": {"description": "Lista paginada de eventos"}},
                }
            },
            "/datastudio/tags": {
                "get": {
                    "summary": "Catálogo de tags do Data Studio",
                    "tags": ["Data Studio"],
                    "responses": {"200": {"description": "Lista de tags disponíveis para consulta"}},
                }
            },
            "/datastudio/series": {
                "get": {
                    "summary": "Séries temporais do Data Studio",
                    "tags": ["Data Studio"],
                    "parameters": [
                        {"name": "tag_ids", "in": "query", "schema": {"type": "string"}, "description": "IDs separados por vírgula"},
                        {"name": "start", "in": "query", "schema": {"type": "string", "format": "date-time"}},
                        {"name": "end", "in": "query", "schema": {"type": "string", "format": "date-time"}},
                    ],
                    "responses": {"200": {"description": "Dados de séries temporais"}},
                }
            },
            "/datastudio/export": {
                "get": {
                    "summary": "Exportar dados do Data Studio (CSV)",
                    "tags": ["Data Studio"],
                    "responses": {"200": {"description": "Arquivo CSV"}},
                }
            },
            "/raw/tables": {
                "get": {
                    "summary": "Listar tabelas raw de ingestão",
                    "tags": ["Raw / Ingestão"],
                    "description": "Retorna a lista de tabelas raw disponíveis no banco (raw_inverter, raw_relay, etc.).",
                    "responses": {"200": {"description": "Lista de tabelas com descrição"}},
                }
            },
            "/raw/query": {
                "get": {
                    "summary": "Consultar dados brutos de uma tabela raw",
                    "tags": ["Raw / Ingestão"],
                    "description": "Retorna registros brutos da tabela de ingestão SCADA (JSON original do dispositivo IoT).",
                    "parameters": [
                        {"name": "table", "in": "query", "required": True, "schema": {"type": "string", "enum": [
                            "raw_inverter", "raw_relay", "raw_meter", "raw_weather_station",
                            "raw_tracker", "raw_transformer", "raw_nobreak", "raw_logger",
                        ]}},
                        {"name": "plant_id", "in": "query", "schema": {"type": "integer"}, "description": "Filtrar por usina"},
                        {"name": "device_id", "in": "query", "schema": {"type": "integer"}, "description": "Filtrar por dispositivo"},
                        {"name": "start", "in": "query", "schema": {"type": "string", "format": "date-time"}, "description": "Timestamp início (ISO 8601)"},
                        {"name": "end", "in": "query", "schema": {"type": "string", "format": "date-time"}, "description": "Timestamp fim (ISO 8601)"},
                        {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 100, "maximum": 500}},
                        {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}},
                    ],
                    "responses": {"200": {"description": "Registros com timestamp, power_plant_id, device_id, json_data"}},
                }
            },
            "/raw/id-legend": {
                "get": {
                    "summary": "Legenda dos IDs de eventos/alarmes para uma tabela raw",
                    "tags": ["Raw / Ingestão"],
                    "description": "Retorna a descrição de cada ID (ID1-ID55) do catálogo de alarmes para o tipo de dispositivo associado à tabela.",
                    "parameters": [
                        {"name": "table", "in": "query", "required": True, "schema": {"type": "string", "enum": [
                            "raw_inverter", "raw_relay", "raw_meter", "raw_weather_station",
                            "raw_tracker", "raw_transformer", "raw_nobreak", "raw_logger",
                        ]}},
                    ],
                    "responses": {"200": {"description": "Lista de IDs com code, description, type, severity"}},
                }
            },
        },
    }


def _tk_presign_image(s3_key):
    if not s3_key:
        return None
    bucket = os.getenv("OS_S3_BUCKET", "").strip()
    if not bucket:
        return None
    try:
        s3c = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
        return s3c.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": s3_key}, ExpiresIn=3600)
    except:
        return None


def _tk_enrich_rows(rows):
    for r in rows:
        if r.get("image_url"):
            r["image_url"] = _tk_presign_image(r["image_url"])
    return rows


def _lambda_handler_impl(event, context):
    path = normalize_path(event)
    method = get_method(event)
    params = event.get("queryStringParameters") or {}
    path_params = event.get("pathParameters") or {}

    ids = extract_ids_from_path(path)
    ctx = get_user_context(event)

    plant_id = (path_params or {}).get("plant_id") or ids.get("plant_id")
    inverter_id_fallback = (path_params or {}).get("inverter_id") or ids.get("inverter_id")
    device_id_fallback = (path_params or {}).get("device_id") or ids.get("device_id")
    string_index_fallback = (path_params or {}).get("string_index") or ids.get("string_index")

    # ------------------------
    # CORS preflight
    # ------------------------
    if method == "OPTIONS":
        return http_response(200, {"ok": True})

    # ------------------------
    # POST /auth/login
    # ------------------------
    if method == "POST" and is_path(path, "/auth/login"):
        body = parse_json_body(event)
        if body is None:
            return http_response(400, {"ok": False, "error": "JSON inválido"})

        username = body.get("username")
        password = body.get("password")
        if not username or not password:
            return http_response(200, {"ok": False})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                SELECT
                    id,
                    username,
                    password_hash,
                    customer_id,
                    is_superuser,
                    is_active,
                    role_key,
                    permissions
                FROM public.app_user
                WHERE username = %s
            """, (username,))
            user = cur.fetchone()

            if not user or not user["is_active"]:
                return http_response(200, {"ok": False})

            if hash_password(password) != user["password_hash"]:
                return http_response(200, {"ok": False})

            return http_response(200, {
                "ok": True,
                "user": {
                    "id": user["id"],
                    "username": user["username"],
                    "customer_id": user["customer_id"],
                    "is_superuser": user["is_superuser"],
                    "role_key": user.get("role_key"),
                    "permissions": normalize_permissions(user.get("permissions")) or {}
                }
            })
        finally:
            cur.close()
            end_request(conn)

    # ------------------------
    # GET /push/vapid-key — chave pública Web Push (público, sem auth)
    # ------------------------
    if method == "GET" and is_path(path, "/push/vapid-key"):
        return http_response(200, {"public_key": os.getenv("VAPID_PUBLIC_KEY", "")})

    # ------------------------
    # AUTH REQUIRED
    # ------------------------
    if not ctx["customer_id"] and not ctx["is_superuser"]:
        return http_response(401, {"error": "customer_id ausente"})

    # ========================================================
    # GET /datastudio/tags
    # ========================================================
    if method == "GET" and is_path(path, "/datastudio/tags"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_datastudio_tags(cur, ctx, params)
        except Exception as e:
            print("[/datastudio/tags] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # POST /datastudio/selection
    # ========================================================
    if method == "POST" and is_path(path, "/datastudio/selection"):
        body = parse_json_body(event)
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_post_datastudio_selection(cur, conn, ctx, body)
        except Exception as e:
            rollback_quiet(conn)
            print("[/datastudio/selection] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # POST /datastudio/favorite
    # ========================================================
    if method == "POST" and is_path(path, "/datastudio/favorite"):
        body = parse_json_body(event)
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_post_datastudio_favorite(cur, conn, ctx, body)
        except Exception as e:
            rollback_quiet(conn)
            print("[/datastudio/favorite] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /datastudio/selections
    # ========================================================
    if method == "GET" and is_path(path, "/datastudio/selections"):
        params = event.get("queryStringParameters") or {}
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_datastudio_selections(cur, conn, ctx, params)
        except Exception as e:
            rollback_quiet(conn)
            print("[/datastudio/selections] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /datastudio/series
    # ========================================================
    if method == "GET" and is_path(path, "/datastudio/series"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_datastudio_series(cur, ctx, params)
        except Exception as e:
            print("[/datastudio/series] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /datastudio/export
    # ========================================================
    if method == "GET" and is_path(path, "/datastudio/export"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_datastudio_export(cur, ctx, params)
        except Exception as e:
            print("[/datastudio/export] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/summary
    # ========================================================
    if method == "GET" and is_path(path, "/plants/summary"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_plants_summary(cur, ctx)
        except Exception as e:
            print("[/plants/summary] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/relay/realtime
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/relay/realtime"):
        for _attempt in range(2):
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                return handle_get_relay_realtime(cur, int(plant_id), ctx)
            except Exception as e:
                if _attempt == 0 and is_connection_error(e):
                    print(f"[/relay/realtime] conn error, retrying: {e!r}")
                    cur.close()
                    invalidate_conn()
                    continue
                print("[/relay/realtime] ERROR:", repr(e))
                print(traceback.format_exc())
                return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
            finally:
                cur.close()
                end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/multimeter/realtime
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/multimeter/realtime"):
        for _attempt in range(2):
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                return handle_get_multimeter_realtime(cur, int(plant_id), ctx)
            except Exception as e:
                if _attempt == 0 and is_connection_error(e):
                    print(f"[/multimeter/realtime] conn error, retrying: {e!r}")
                    cur.close()
                    invalidate_conn()
                    continue
                print("[/multimeter/realtime] ERROR:", repr(e))
                print(traceback.format_exc())
                return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
            finally:
                cur.close()
                end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/trackers/realtime
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/trackers/realtime"):
        for _attempt in range(2):
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                return handle_get_trackers_realtime(cur, int(plant_id), ctx)
            except Exception as e:
                if _attempt == 0 and is_connection_error(e):
                    print(f"[/trackers/realtime] conn error, retrying: {e!r}")
                    cur.close()
                    invalidate_conn()
                    continue
                print("[/trackers/realtime] ERROR:", repr(e))
                print(traceback.format_exc())
                return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
            finally:
                cur.close()
                end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/inverters/realtime
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/inverters/realtime"):
        for _attempt in range(2):
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("SET LOCAL statement_timeout = '30000ms';")

                if not ensure_plant_access(cur, int(plant_id), ctx):
                    return http_response(403, {"error": "sem permissão para esta usina"})

                effective_customer_id = resolve_customer_id_for_plant(cur, int(plant_id), ctx)
                if effective_customer_id is None:
                    return http_response(400, {"error": "não foi possível resolver customer_id efetivo pela usina"})

                cur.execute(sql.SQL("""
                    WITH inv_devices AS (
                    SELECT
                        d.id AS device_id,
                        d.power_plant_id,
                        COALESCE(NULLIF(BTRIM(d.display_name), ''), d.name) AS inverter_name,
                        d.cabin_id,
                        c.code AS cabin_code,
                        c.name AS cabin_name,
                        c.display_order AS cabin_display_order
                    FROM public.device d
                    JOIN public.device_type dt ON dt.id = d.device_type_id
                    LEFT JOIN public.cabin c ON c.id = d.cabin_id
                    WHERE d.power_plant_id = %(plant_id)s
                        AND d.is_active = true
                        AND LOWER(dt.name) = 'inverter'
                    ),
                    pivot AS (
                    SELECT DISTINCT ON (s.device_id)
                        s.device_id,
                        s.active_power_kw AS power_kw,
                        s.frequency_hz AS freq_hz,
                        s.power_factor,
                        s.efficiency_pct,
                        s.power_input_kw,
                        s.working_status,
                        s.state_operation,
                        s.string_voltage_v,
                        s.current_phase_a_a,
                        s.current_phase_b_a,
                        s.current_phase_c_a,
                        s.line_voltage_ab_v,
                        s.line_voltage_bc_v,
                        s.line_voltage_ca_v,
                        s.apparent_power_kva,
                        s.power_reactive_kvar,
                        s.temperature_internal_c AS temp_c,
                        s.daily_active_energy_kwh,
                        s.resistance_insulation_mohm,
                        s.cumulative_active_energy_kwh,
                        s.power_dc_kw,
                        s."timestamp" AS last_reading_ts
                    FROM {stg_inverter_analog} s
                    WHERE s.power_plant_id = %(plant_id)s
                        AND s."timestamp" >= now() - interval '1 hour'
                    ORDER BY s.device_id, s."timestamp" DESC
                    )
                    SELECT
                    p.customer_id,
                    iv.power_plant_id,
                    p.name AS power_plant_name,
                    iv.device_id,
                    iv.inverter_name,
                    pv.power_kw,
                    pv.efficiency_pct,
                    pv.temp_c,
                    pv.freq_hz,
                    NULL::numeric AS pr,
                    pv.last_reading_ts,
                    pv.apparent_power_kva,
                    pv.power_factor,
                    pv.power_reactive_kvar,
                    pv.power_input_kw,
                    pv.daily_active_energy_kwh,
                    pv.cumulative_active_energy_kwh,
                    pv.current_phase_a_a,
                    pv.current_phase_b_a,
                    pv.current_phase_c_a,
                    pv.line_voltage_ab_v,
                    pv.line_voltage_bc_v,
                    pv.line_voltage_ca_v,
                    pv.string_voltage_v,
                    pv.power_dc_kw,
                    pv.resistance_insulation_mohm,
                    iv.cabin_id,
                    iv.cabin_code,
                    iv.cabin_name,
                    iv.cabin_display_order,
                    pv.working_status,
                    NULL::text AS inverter_status,
                    pv.state_operation,
                    NULL::int AS communication_fault_code,
                    (pv.last_reading_ts IS NOT NULL) AS is_communication_ok,
                    COALESCE(pv.state_operation, pv.working_status) AS status_code_raw,
                    CASE
                        WHEN pv.last_reading_ts IS NULL THEN 'NO_COMM'
                        WHEN pv.state_operation = 2 THEN 'RUNNING'
                        WHEN pv.state_operation = 3 THEN 'OFF'
                        WHEN pv.working_status = 1 THEN 'RUNNING'
                        WHEN pv.working_status = 8 THEN 'OFF'
                        ELSE 'UNKNOWN'
                    END AS status
                    FROM inv_devices iv
                    LEFT JOIN pivot pv ON pv.device_id = iv.device_id
                    JOIN public.power_plant p
                    ON p.id = iv.power_plant_id
                    WHERE (%(is_superuser)s = true OR p.customer_id = %(customer_id)s)
                    ORDER BY iv.device_id;
                """).format(
                    stg_inverter_analog=q(RT_SCHEMA, "stg_inverter_analog")
                ), {
                    "customer_id": int(effective_customer_id),
                    "is_superuser": ctx["is_superuser"],
                    "plant_id": int(plant_id),
                })

                rows = cur.fetchall() or []

                cur.execute(sql.SQL("""
                    SELECT rated_power_kwp, irradiation_daily_kwh_m2
                    FROM {fct}
                    WHERE power_plant_id = %(plant_id)s
                      AND date_day = (now() AT TIME ZONE 'America/Fortaleza')::date
                """).format(fct=q(RT_SCHEMA, "fct_power_plant_metrics_daily")), {
                    "plant_id": int(plant_id),
                })
                fct_row = cur.fetchone()
                _pr_rated = float(fct_row["rated_power_kwp"]) if fct_row and fct_row.get("rated_power_kwp") else 0
                _pr_irrad = float(fct_row["irradiation_daily_kwh_m2"]) if fct_row and fct_row.get("irradiation_daily_kwh_m2") else 0
                _pr_n_inv = len(rows) or 1
                _pr_cap_per_inv = _pr_rated / _pr_n_inv if _pr_rated > 0 else 0

                def fnum(r, k):
                    v = r.get(k)
                    if v is None:
                        return None
                    try:
                        return float(v)
                    except Exception:
                        return None

                def inum(r, k):
                    v = r.get(k)
                    if v is None:
                        return None
                    try:
                        return int(float(v))
                    except Exception:
                        return None

                def calc_pr_inv(energy_kwh):
                    if energy_kwh and _pr_cap_per_inv > 0 and _pr_irrad > 0:
                        return round(min(100, max(0, 100.0 * energy_kwh / (_pr_cap_per_inv * _pr_irrad))), 1)
                    return None

                items = []
                for r in rows:
                    device_id = r.get("device_id")
                    inverter_id = int(device_id) if device_id is not None else None

                    status_code = inum(r, "status_code_raw")
                    if status_code is None:
                        status_code = 4

                    energy_kwh = fnum(r, "daily_active_energy_kwh")

                    item = {
                        "inverter_id": inverter_id,
                        "inverter_name": r.get("inverter_name"),
                        "power_kw": fnum(r, "power_kw") or 0.0,
                        "efficiency_pct": fnum(r, "efficiency_pct") or 0.0,
                        "temp_c": fnum(r, "temp_c"),
                        "freq_hz": fnum(r, "freq_hz"),
                        "pr": calc_pr_inv(energy_kwh),
                        "status_code": status_code,
                        "status": r.get("status") or "UNKNOWN",
                        "last_reading_ts": r.get("last_reading_ts"),

                        "apparent_power_kva": fnum(r, "apparent_power_kva"),
                        "power_factor": fnum(r, "power_factor"),
                        "power_reactive_kvar": fnum(r, "power_reactive_kvar"),
                        "power_input_kw": fnum(r, "power_input_kw"),
                        "daily_active_energy_kwh": fnum(r, "daily_active_energy_kwh"),
                        "cumulative_active_energy_kwh": fnum(r, "cumulative_active_energy_kwh"),
                        "current_phase_a_a": fnum(r, "current_phase_a_a"),
                        "current_phase_b_a": fnum(r, "current_phase_b_a"),
                        "current_phase_c_a": fnum(r, "current_phase_c_a"),
                        "line_voltage_ab_v": fnum(r, "line_voltage_ab_v"),
                        "line_voltage_bc_v": fnum(r, "line_voltage_bc_v"),
                        "line_voltage_ca_v": fnum(r, "line_voltage_ca_v"),
                        "string_voltage_v": fnum(r, "string_voltage_v"),
                        "power_dc_kw": fnum(r, "power_dc_kw"),
                        "resistance_insulation_mohm": fnum(r, "resistance_insulation_mohm"),

                        "working_status": inum(r, "working_status"),
                        "inverter_status": r.get("inverter_status"),
                        "state_operation": inum(r, "state_operation"),
                        "communication_fault_code": r.get("communication_fault_code"),
                        "is_communication_ok": r.get("is_communication_ok"),

                        "cabin_id": int(r["cabin_id"]) if r.get("cabin_id") is not None else None,
                        "cabin_code": r.get("cabin_code"),
                        "cabin_name": r.get("cabin_name"),
                        "cabin_display_order": int(r["cabin_display_order"]) if r.get("cabin_display_order") is not None else None,
                        "section_name": r.get("cabin_name"),
                    }

                    items.append(item)

                return http_response(200, {
                    "power_plant_id": int(plant_id),
                    "customer_id": int(effective_customer_id),
                    "items": items,
                    "meta": {
                        "source": f"{RT_SCHEMA}.stg_inverter_analog"
                    }
                })

            except Exception as e:
                if _attempt == 0 and is_connection_error(e):
                    print(f"[/inverters/realtime] conn error, retrying: {e!r}")
                    cur.close()
                    invalidate_conn()
                    continue
                print("[/inverters/realtime] ERROR:", repr(e))
                print(traceback.format_exc())
                return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
            finally:
                cur.close()
                end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/inverters/{inverter_id}/strings/realtime
    # ========================================================
    if method == "GET" and plant_id and path_contains(path, "/inverters/") and is_path(path, "/strings/realtime"):
        inverter_id = inverter_id_fallback
        if not inverter_id or not str(inverter_id).isdigit():
            return http_response(400, {"error": "inverter_id inválido"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SET LOCAL statement_timeout = '12000ms';")

            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            effective_customer_id = resolve_customer_id_for_plant(cur, int(plant_id), ctx)
            if effective_customer_id is None:
                return http_response(400, {"error": "não foi possível resolver customer_id efetivo pela usina"})

            max_s = 40 if int(effective_customer_id) == 3 else 30

            cur.execute(sql.SQL("""
                WITH all_strings AS (
                SELECT generate_series(1, %(max_strings)s) AS string_index
                ),
                cfg AS (
                SELECT string_index, enabled
                FROM public.inverter_string_config
                WHERE customer_id = %(customer_id)s
                    AND plant_id = %(plant_id)s
                    AND inverter_id = %(inverter_id)s
                ),
                last_per_string AS (
                SELECT DISTINCT ON (s.string_index)
                    s.string_index,
                    s.string_current,
                    s.timestamp AS last_ts
                FROM {int_inverter_string} s
                WHERE s.power_plant_id = %(plant_id)s
                    AND s.device_id = %(inverter_id)s
                    AND s.timestamp >= now() - interval '7 days'
                ORDER BY s.string_index, s.timestamp DESC
                )
                SELECT
                a.string_index,
                COALESCE(cfg.enabled, true) AS enabled,
                (l.string_index IS NOT NULL) AS has_data,
                l.string_current AS current_a,
                l.last_ts,
                EXTRACT(EPOCH FROM (now() - l.last_ts))::int AS age_seconds,
                (now() - l.last_ts <= interval %(online_window)s) AS is_online
                FROM all_strings a
                LEFT JOIN cfg ON cfg.string_index = a.string_index
                LEFT JOIN last_per_string l ON l.string_index = a.string_index
                ORDER BY a.string_index;
            """).format(int_inverter_string=q(RT_SCHEMA, "stg_inverter_string")), {
                "customer_id": effective_customer_id,
                "plant_id": int(plant_id),
                "inverter_id": int(inverter_id),
                "online_window": STRING_ONLINE_WINDOW,
                "max_strings": max_s,
            })

            rows = cur.fetchall() or []
            strings_out = [{
                "string_index": int(r.get("string_index")),
                "enabled": bool(r.get("enabled")),
                "has_data": bool(r.get("has_data")),
                "current_a": float(r.get("current_a")) if r.get("current_a") is not None else None,
                "last_ts": r.get("last_ts"),
                "age_seconds": int(r.get("age_seconds")) if r.get("age_seconds") is not None else None,
                "is_online": bool(r.get("is_online")) if r.get("is_online") is not None else False,
            } for r in rows]

            return http_response(200, {
                "power_plant_id": int(plant_id),
                "inverter_id": int(inverter_id),
                "max_strings": max_s,
                "customer_id": int(effective_customer_id),
                "strings": strings_out,
                "items": strings_out,
                "meta": {
                    "source": f"{RT_SCHEMA}.stg_inverter_string"
                }
            })
        except Exception as e:
            print("[GET strings/realtime] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/realtime
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/realtime") and not path_contains(path, "/inverters/"):
        if params.get("view") == "daily-round":
            for _attempt in range(2):
                conn = get_conn()
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                try:
                    return handle_get_daily_round(cur, int(plant_id), ctx, params)
                except Exception as e:
                    if _attempt == 0 and is_connection_error(e):
                        print(f"[/realtime?view=daily-round] conn error, retrying: {e!r}")
                        cur.close()
                        invalidate_conn()
                        continue
                    print("[/realtime?view=daily-round] ERROR:", repr(e))
                    print(traceback.format_exc())
                    return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
                finally:
                    cur.close()
                    end_request(conn)
        if params.get("view") == "report":
            for _attempt in range(2):
                conn = get_conn()
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                try:
                    return handle_get_report(cur, int(plant_id), ctx, params)
                except Exception as e:
                    if _attempt == 0 and is_connection_error(e):
                        print(f"[/realtime?view=report] conn error, retrying: {e!r}")
                        cur.close()
                        invalidate_conn()
                        continue
                    print("[/realtime?view=report] ERROR:", repr(e))
                    print(traceback.format_exc())
                    return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
                finally:
                    cur.close()
                    end_request(conn)
        for _attempt in range(2):
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("SET LOCAL statement_timeout = '12000ms';")
                load_current_user(cur, ctx)

                if not ensure_plant_access(cur, int(plant_id), ctx):
                    return http_response(403, {"error": "sem permissão para esta usina"})

                cur.execute(sql.SQL("""
                    WITH weather AS (
                    SELECT DISTINCT ON (w.power_plant_id)
                        w.power_plant_id,
                        w.irradiance_ghi_wm2,
                        w.irradiance_poa_wm2,
                        w.air_temperature_c,
                        w.module_temperature_c,
                        w.rain_signal,
                        w."timestamp" AS weather_last_update
                    FROM {stg_weather_station_analog} w
                    WHERE w.power_plant_id = %(plant_id)s
                    ORDER BY w.power_plant_id, w."timestamp" DESC
                    ),
                    alarms AS (
                    SELECT
                        power_plant_id,
                        MAX(
                            CASE
                                WHEN severity = 'high' THEN 3
                                WHEN severity = 'medium' THEN 2
                                WHEN severity = 'low' THEN 1
                                ELSE 0
                            END
                        ) AS alarm_level
                    FROM {int_events_alarms}
                    WHERE power_plant_id = %(plant_id)s
                        AND COALESCE(is_active_event, false) = true
                    GROUP BY power_plant_id
                    )
                    SELECT
                        p.power_plant_id,
                        COALESCE(NULLIF(BTRIM(pp.display_name), ''), p.power_plant_name) AS power_plant_name,
                        pp.capacity_ac,
                        COALESCE(pp.capacity_dc, p.rated_power_kwp) AS rated_power_kw,
                        COALESCE(p.active_power_inverter_kw, 0) AS active_power_kw,
                        p.active_power_meter_kw,
                        p.active_power_kw AS active_power_total_kw,
                        p.daily_energy_kwh AS energy_today_kwh,
                        p.inverter_availability_pct,
                        p.relay_availability_pct,
                        p.pr_daily_pct AS performance_ratio,
                        p.irradiance_wm2 AS irradiance_ghi_wm2,
                        p.red_alarm_count AS critical_alarms,
                        p.plant_status_color AS plant_status,
                        p.updated_at AS last_update,
                        NULL::int AS inverter_total,
                        NULL::int AS inverter_generating,
                        NULL::int AS inverter_no_comm,
                        NULL::int AS inverter_off,
                        w.irradiance_ghi_wm2 AS weather_ghi,
                        w.irradiance_poa_wm2 AS weather_poa,
                        w.air_temperature_c AS weather_air_temp,
                        w.module_temperature_c AS weather_module_temp,
                        w.rain_signal AS weather_rain,
                        w.weather_last_update,
                        CASE
                            WHEN a.alarm_level = 3 THEN 'high'
                            WHEN a.alarm_level = 2 THEN 'medium'
                            WHEN a.alarm_level = 1 THEN 'low'
                            ELSE null
                        END AS alarm_severity
                    FROM {mart_portfolio_overview} p
                    LEFT JOIN public.power_plant pp ON pp.id = p.power_plant_id
                    LEFT JOIN weather w ON w.power_plant_id = p.power_plant_id
                    LEFT JOIN alarms a ON a.power_plant_id = p.power_plant_id
                    WHERE p.power_plant_id = %(plant_id)s
                    AND (
                        %(is_superuser)s = true
                        OR p.customer_id = %(customer_id)s
                    ){plant_filter}
                """).format(
                    mart_portfolio_overview=q(RT_SCHEMA, "mart_portfolio_overview"),
                    stg_weather_station_analog=q(RT_SCHEMA, "stg_weather_station_analog"),
                    int_events_alarms=q(RT_SCHEMA, "int_events_alarms"),
                    plant_filter=plant_filter_sql(ctx, "p")
                ), {
                    "plant_id": int(plant_id),
                    "customer_id": ctx["customer_id"],
                    "is_superuser": ctx["is_superuser"]
                })

                row = cur.fetchone()
                if not row:
                    return http_response(404, {"error": "usina não encontrada"})

                row["weather"] = {
                    "irradiance_ghi_wm2": row.pop("weather_ghi"),
                    "irradiance_poa_wm2": row.pop("weather_poa"),
                    "air_temperature_c": row.pop("weather_air_temp"),
                    "module_temperature_c": row.pop("weather_module_temp"),
                    "rain_signal": row.pop("weather_rain"),
                    "last_update": row.pop("weather_last_update"),
                }

                return http_response(200, row)
            except Exception as e:
                if _attempt == 0 and is_connection_error(e):
                    print(f"[/realtime] conn error, retrying: {e!r}")
                    cur.close()
                    invalidate_conn()
                    continue
                print("[/realtime] ERROR:", repr(e))
                print(traceback.format_exc())
                return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
            finally:
                cur.close()
                end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/daily-round
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/daily-round"):
        for _attempt in range(2):
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                return handle_get_daily_round(cur, int(plant_id), ctx, params)
            except Exception as e:
                if _attempt == 0 and is_connection_error(e):
                    print(f"[/daily-round] conn error, retrying: {e!r}")
                    cur.close()
                    invalidate_conn()
                    continue
                print("[/daily-round] ERROR:", repr(e))
                print(traceback.format_exc())
                return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
            finally:
                cur.close()
                end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/energy/daily
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/energy/daily"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            cur.execute(sql.SQL("""
                WITH bounds AS (
                    SELECT date_trunc('day', now() AT TIME ZONE 'America/Fortaleza') AS day_start_local
                )
                SELECT
                    to_char((ts AT TIME ZONE 'America/Fortaleza')::timestamp, 'HH24:MI') AS label,
                    COALESCE(active_power_kw, 0)::numeric AS active_power_kw,
                    COALESCE(irradiance_poa_wm2, 0)::numeric AS irradiance_poa_wm2
                FROM {mart_power_intraday}
                WHERE power_plant_id = %(plant_id)s
                AND ts >= ((SELECT day_start_local FROM bounds) AT TIME ZONE 'America/Fortaleza')
                AND ts <  (((SELECT day_start_local FROM bounds) + interval '1 day') AT TIME ZONE 'America/Fortaleza')
                ORDER BY ts
            """).format(mart_power_intraday=q(RT_SCHEMA, "mart_power_intraday")), {
                "plant_id": int(plant_id)
            })

            rows = cur.fetchall() or []
            labels = [r["label"] for r in rows]
            active_power = [float(r["active_power_kw"]) for r in rows]
            irradiance = [float(r["irradiance_poa_wm2"]) for r in rows]

            expected_power = []
            has_expected = False
            expected_error = None

            try:
                effective_customer_id = resolve_customer_id_for_plant(cur, int(plant_id), ctx)

                if int(plant_id) == 13 and int(effective_customer_id or 0) == 2 and labels:
                    cur.execute("SELECT (now() AT TIME ZONE 'America/Fortaleza')::date AS local_date")
                    local_row = cur.fetchone() or {}
                    local_date = local_row.get("local_date") or datetime.utcnow().date()

                    expected_day_kwh = get_pvsyst_expected_day_kwh(cur, int(plant_id), local_date)

                    if expected_day_kwh is not None and float(expected_day_kwh) > 0:
                        step_minutes = infer_step_minutes(labels)
                        full_day_labels = build_full_day_labels(step_minutes, start_hour=5, end_hour=18)
                        full_day_curve = expected_kwh_to_power_curve(
                            expected_day_kwh=expected_day_kwh,
                            labels_hhmm=full_day_labels,
                            step_minutes=step_minutes
                        )
                        curve_by_label = dict(zip(full_day_labels, full_day_curve))
                        expected_power = [curve_by_label.get(lbl) for lbl in labels]
                        has_expected = True
                    else:
                        expected_power = [None for _ in labels]
                else:
                    expected_power = [None for _ in labels]
            except Exception as e:
                expected_error = str(e)
                expected_power = [None for _ in labels]
                print("[/energy/daily][expected] ERROR:", repr(e))
                print(traceback.format_exc())

            return http_response(200, {
                "labels": labels,
                "activePower": active_power,
                "irradiance": irradiance,
                "expectedPower": expected_power,
                "meta": {
                    "source": f"{RT_SCHEMA}.mart_power_intraday",
                    "expected_source": "public.pvsyst_expected_daily",
                    "has_expected": has_expected,
                    "expected_mode": "expected_day_kwh_distributed_full_day",
                    "expected_error": expected_error
                }
            })
        except Exception as e:
            print("[/energy/daily] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/energy/monthly
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/energy/monthly"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            qparams = event.get("queryStringParameters") or {}
            year = qparams.get("year")
            month = qparams.get("month")

            if year and str(year).isdigit() and month and str(month).isdigit():
                y = int(year)
                m = int(month)
                if m < 1 or m > 12:
                    return http_response(400, {"error": "month deve ser 1..12"})
            else:
                cur.execute("SELECT EXTRACT(YEAR FROM (now() AT TIME ZONE 'America/Fortaleza'))::int AS y, EXTRACT(MONTH FROM (now() AT TIME ZONE 'America/Fortaleza'))::int AS m")
                ym = cur.fetchone() or {}
                y = int(ym.get("y") or datetime.utcnow().year)
                m = int(ym.get("m") or datetime.utcnow().month)

            month_start = datetime(y, m, 1).date()
            if m == 12:
                month_end = datetime(y + 1, 1, 1).date() - timedelta(days=1)
            else:
                month_end = datetime(y, m + 1, 1).date() - timedelta(days=1)

            rows, has_expected = get_monthly_real_and_expected(cur, int(plant_id), month_start, month_end)
            payload = build_monthly_expected_payload(rows)
            payload["meta"] = {
                "source": f"{RT_SCHEMA}.fct_power_plant_metrics_daily",
                "expected_source": "public.pvsyst_expected_daily",
                "has_expected": has_expected
            }

            return http_response(200, payload)
        except Exception as e:
            print("[/energy/monthly] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # POST /plants  (criar nova usina)
    # ========================================================
    if method == "POST" and is_path(path, "/plants"):
        if not ctx.get("is_superuser") and not can_edit_plant(ctx):
            return http_response(403, {"error": "Permissão negada"})

        body = parse_json_body(event)
        if not isinstance(body, dict):
            return http_response(400, {"error": "JSON inválido"})

        plant_name = str(body.get("plant_name") or "").strip()
        if not plant_name:
            return http_response(400, {"error": "plant_name é obrigatório"})

        customer_id_body = body.get("customer_id")
        if ctx.get("is_superuser") and customer_id_body:
            try:
                target_customer_id = int(customer_id_body)
            except (ValueError, TypeError):
                return http_response(400, {"error": "customer_id inválido"})
        else:
            target_customer_id = ctx["customer_id"]

        capacity_dc = None
        raw_dc = body.get("capacity_dc")
        if raw_dc not in (None, ""):
            try:
                capacity_dc = float(raw_dc)
                if capacity_dc < 0:
                    return http_response(400, {"error": "capacity_dc deve ser >= 0"})
            except (ValueError, TypeError):
                return http_response(400, {"error": "capacity_dc deve ser numérico"})

        capacity_ac = None
        raw_ac = body.get("capacity_ac")
        if raw_ac not in (None, ""):
            try:
                capacity_ac = float(raw_ac)
            except (ValueError, TypeError):
                pass

        location = str(body.get("location") or "").strip() or None
        timezone = str(body.get("timezone") or "America/Fortaleza").strip()

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error:
                return auth_error

            cur.execute("""
                INSERT INTO public.power_plant
                    (name, display_name, customer_id, capacity_dc, capacity_ac, location, timezone, is_active, created_at, updated_at)
                VALUES
                    (%(name)s, %(display_name)s, %(customer_id)s,
                    %(capacity_dc)s, %(capacity_ac)s, %(location)s, %(timezone)s,
                    true, NOW(), NOW())
                RETURNING id AS plant_id, name, display_name, customer_id, capacity_dc, capacity_ac, location, is_active;
            """, {
                "name": plant_name,
                "display_name": plant_name,
                "customer_id": target_customer_id,
                "capacity_dc": capacity_dc,
                "capacity_ac": capacity_ac,
                "location": location,
                "timezone": timezone,
            })
            row = cur.fetchone()
            conn.commit()
            return http_response(201, {
                "ok": True,
                "plant_id": int(row["plant_id"]),
                "plant_name": row.get("display_name") or row.get("name"),
                "customer_id": int(row["customer_id"]),
                "capacity_dc": float(row["capacity_dc"]) if row.get("capacity_dc") is not None else None,
            })
        except Exception as e:
            conn.rollback()
            print("[POST /plants] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants
    # ========================================================
    if method == "GET" and is_path(path, "/plants"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            load_current_user(cur, ctx)
            cur.execute(sql.SQL("""
                SELECT
                    p.customer_id,
                    p.customer_name,
                    p.customer_rated_power_kwp,
                    p.power_plant_id,
                    pp.name AS original_name,
                    pp.display_name,
                    COALESCE(NULLIF(BTRIM(pp.display_name), ''), pp.name, p.power_plant_name) AS power_plant_name,
                    pp.capacity_ac,
                    pp.capacity_dc,
                    pp.location,
                    pp.is_active,
                    COALESCE(pp.capacity_dc, p.rated_power_kwp) AS rated_power_kwp,
                    p.rated_power_ac_kw,
                    p.active_power_inverter_kw,
                    p.active_power_meter_kw,
                    p.active_power_kw,
                    CASE
                        WHEN p.rated_power_ac_kw > 0
                        THEN ROUND(100.0 * COALESCE(p.active_power_kw, 0) / p.rated_power_ac_kw, 2)
                        ELSE NULL
                    END AS capacity_utilization_pct,
                    p.daily_energy_kwh,
                    p.generation_liquid_meter_kwh,
                    p.generation_accumulated_kwh,
                    p.irradiance_wm2,
                    p.irradiation_accumulated_kwh_m2,
                    p.pr_daily_pct,
                    p.pr_accumulated_pct,
                    p.capacity_factor_daily_pct,
                    p.capacity_factor_pct,
                    p.inverter_availability_pct,
                    p.relay_availability_pct,
                    p.plant_status_color,
                    p.comm_status,
                    p.inverter_reporting,
                    p.inverter_stale,
                    p.red_alarm_count,
                    p.yellow_alarm_count,
                    p.active_alarm_count,
                    p.updated_at
                FROM {mart_portfolio_overview} p
                JOIN public.power_plant pp
                ON pp.id = p.power_plant_id
                WHERE pp.is_active = true
                AND (
                    %(is_superuser)s = true
                    OR p.customer_id = %(customer_id)s
                ){plant_filter}
                ORDER BY p.power_plant_id
            """).format(
                mart_portfolio_overview=q(RT_SCHEMA, "mart_portfolio_overview"),
                plant_filter=plant_filter_sql(ctx, "p")
            ), {
                "customer_id": ctx["customer_id"],
                "is_superuser": ctx["is_superuser"],
            })

            rows = cur.fetchall() or []
            items = []
            for r in rows:
                red_alarm_count = int(r["red_alarm_count"]) if r.get("red_alarm_count") is not None else 0
                yellow_alarm_count = int(r["yellow_alarm_count"]) if r.get("yellow_alarm_count") is not None else 0
                items.append({
                    "customer_id": r.get("customer_id"),
                    "customer_name": r.get("customer_name"),
                    "customer_rated_power_kwp": float(r["customer_rated_power_kwp"]) if r.get("customer_rated_power_kwp") is not None else None,
                    "power_plant_id": r.get("power_plant_id"),
                    "original_name": r.get("original_name"),
                    "display_name": r.get("display_name"),
                    "power_plant_name": r.get("power_plant_name"),
                    "capacity_ac": float(r["capacity_ac"]) if r.get("capacity_ac") is not None else None,
                    "capacity_dc": float(r["capacity_dc"]) if r.get("capacity_dc") is not None else None,
                    "location": r.get("location"),
                    "is_active": bool(r["is_active"]) if r.get("is_active") is not None else None,
                    "rated_power_kw": float(r["rated_power_kwp"]) if r.get("rated_power_kwp") is not None else None,
                    "rated_power_kwp": float(r["rated_power_kwp"]) if r.get("rated_power_kwp") is not None else None,
                    "rated_power_ac_kw": float(r["rated_power_ac_kw"]) if r.get("rated_power_ac_kw") is not None else None,
                    "active_power_inverter_kw": float(r["active_power_inverter_kw"]) if r.get("active_power_inverter_kw") is not None else None,
                    "active_power_meter_kw": float(r["active_power_meter_kw"]) if r.get("active_power_meter_kw") is not None else None,
                    "active_power_kw": float(r["active_power_inverter_kw"]) if r.get("active_power_inverter_kw") is not None else 0.0,
                    "active_power_total_kw": float(r["active_power_kw"]) if r.get("active_power_kw") is not None else None,
                    "capacity_utilization_pct": float(r["capacity_utilization_pct"]) if r.get("capacity_utilization_pct") is not None else None,
                    "energy_today_kwh": float(r["daily_energy_kwh"]) if r.get("daily_energy_kwh") is not None else None,
                    "daily_energy_kwh": float(r["daily_energy_kwh"]) if r.get("daily_energy_kwh") is not None else None,
                    "generation_liquid_meter_kwh": float(r["generation_liquid_meter_kwh"]) if r.get("generation_liquid_meter_kwh") is not None else None,
                    "generation_accumulated_kwh": float(r["generation_accumulated_kwh"]) if r.get("generation_accumulated_kwh") is not None else None,
                    "irradiance_wm2": float(r["irradiance_wm2"]) if r.get("irradiance_wm2") is not None else None,
                    "irradiation_accumulated_kwh_m2": float(r["irradiation_accumulated_kwh_m2"]) if r.get("irradiation_accumulated_kwh_m2") is not None else None,
                    "pr_daily_pct": float(r["pr_daily_pct"]) if r.get("pr_daily_pct") is not None else None,
                    "pr_accumulated_pct": float(r["pr_accumulated_pct"]) if r.get("pr_accumulated_pct") is not None else None,
                    "performance_ratio": float(r["pr_daily_pct"]) if r.get("pr_daily_pct") is not None else None,
                    "capacity_factor_daily_pct": float(r["capacity_factor_daily_pct"]) if r.get("capacity_factor_daily_pct") is not None else None,
                    "capacity_factor_pct": float(r["capacity_factor_pct"]) if r.get("capacity_factor_pct") is not None else None,
                    "inverter_availability_pct": float(r["inverter_availability_pct"]) if r.get("inverter_availability_pct") is not None else None,
                    "relay_availability_pct": float(r["relay_availability_pct"]) if r.get("relay_availability_pct") is not None else None,
                    "plant_status": r.get("plant_status_color"),
                    "plant_status_color": r.get("plant_status_color"),
                    "comm_status": r.get("comm_status"),
                    "inverter_reporting": int(r["inverter_reporting"]) if r.get("inverter_reporting") is not None else 0,
                    "inverter_stale": int(r["inverter_stale"]) if r.get("inverter_stale") is not None else 0,
                    "red_alarm_count": red_alarm_count,
                    "yellow_alarm_count": yellow_alarm_count,
                    "active_alarm_count": int(r["active_alarm_count"]) if r.get("active_alarm_count") is not None else 0,
                    "critical_alarms": red_alarm_count,
                    "alarm_severity": "high" if red_alarm_count > 0 else ("medium" if yellow_alarm_count > 0 else None),
                    "updated_at": r.get("updated_at"),
                    "last_update": r.get("updated_at"),
                })

            return http_response(200, items)
        except Exception as e:
            print("[/plants] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /events (GLOBAL)
    # ========================================================
    if method == "GET" and is_path(path, "/events") and not plant_id:
        try:
            page = int(params.get("page") or 1)
        except Exception:
            page = 1
        page = max(1, page)

        try:
            page_size = int(params.get("page_size") or 50)
        except Exception:
            page_size = 50
        page_size = max(1, min(500, page_size))

        limit = page_size
        offset = (page - 1) * page_size

        start_time_raw = (params.get("start_time") or "").strip()
        end_time_raw = (params.get("end_time") or "").strip()
        if not start_time_raw or not end_time_raw:
            return http_response(400, {"error": "start_time e end_time são obrigatórios"})

        sdt = parse_time_to_dt(start_time_raw)
        edt = parse_time_to_dt(end_time_raw)

        if not sdt or not edt:
            return http_response(400, {
                "error": "start_time/end_time inválidos. Aceita ISO (com Z) ou 'YYYY-MM-DD HH:MM:SS'.",
                "examples": [
                    "2026-02-15T03:00:00.000Z",
                    "2026-02-15T03:00:00-03:00",
                    "2026-02-15 03:00:00"
                ]
            })

        if edt < sdt:
            return http_response(400, {"error": "end_time não pode ser menor que start_time"})

        max_days = 31
        if (edt - sdt).days > max_days:
            return http_response(400, {"error": f"range muito grande. Máximo {max_days} dias"})

        include_total = safe_lower(params.get("include_total")) in ("1", "true", "yes")

        mode_q = safe_lower(params.get("mode"))
        if mode_q in ("", "normal", "history"):
            mode_q = "normal"

        try:
            rounds = int(params.get("rounds") or 5)
        except Exception:
            rounds = 5
        rounds = max(1, min(20, rounds))

        plant_id_q = params.get("plant_id")
        device_id_q = params.get("device_id")
        severity_q = safe_lower(params.get("severity"))
        source_q = safe_lower(params.get("source"))
        event_type_q = safe_lower(params.get("event_type"))
        status_q = safe_lower(params.get("status"))
        q_raw = (params.get("q") or "").strip()[:80]

        _ev_conn = get_conn()
        _ev_cur = _ev_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            load_current_user(_ev_cur, ctx)
        except Exception:
            pass
        finally:
            _ev_cur.close()
            end_request(_ev_conn)

        where = []
        sql_params = {
            "customer_id": ctx["customer_id"],
            "is_superuser": ctx["is_superuser"],
            "start_dt": sdt,
            "end_dt": edt,
            "limit": limit,
            "offset": offset,
            "rounds": rounds,
        }

        where.append("( %(is_superuser)s = true OR p.customer_id = %(customer_id)s )")
        where.append("""
            e."timestamp" >= %(start_dt)s
            AND e."timestamp" <= %(end_dt)s
        """)

        allowed_ids = get_allowed_plant_ids(ctx)
        if allowed_ids:
            where.append("e.power_plant_id IN %(allowed_plant_ids)s")
            sql_params["allowed_plant_ids"] = tuple(allowed_ids)

        if plant_id_q and str(plant_id_q).isdigit():
            where.append("e.power_plant_id = %(plant_id)s")
            sql_params["plant_id"] = int(plant_id_q)

        if device_id_q and str(device_id_q).isdigit():
            where.append("e.device_id = %(device_id)s")
            sql_params["device_id"] = int(device_id_q)

        if severity_q in ("low", "medium", "high"):
            where.append("e.severity = %(severity)s")
            sql_params["severity"] = severity_q

        if source_q in ("inverter", "relay", "weather"):
            where.append("LOWER(COALESCE(e.event_source, '')) = %(source)s")
            sql_params["source"] = source_q

        if event_type_q in ("event", "alarm", "status"):
            where.append("LOWER(COALESCE(e.type_en, e.type_pt, '')) = %(event_type)s")
            sql_params["event_type"] = event_type_q

        if status_q in ("active", "inactive"):
            if status_q == "active":
                where.append("COALESCE(e.is_active_event, false) = true")
            else:
                where.append("COALESCE(e.is_active_event, false) = false")

        if q_raw:
            where.append("""
                (
                COALESCE(e.description_pt::text, '') ILIKE %(q)s
                OR COALESCE(e.device_name::text, '') ILIKE %(q)s
                OR COALESCE(e.power_plant_name::text, '') ILIKE %(q)s
                OR COALESCE(e.device_type_name::text, '') ILIKE %(q)s
                OR COALESCE(e.event_source::text, '') ILIKE %(q)s
                OR COALESCE(e.type_en::text, e.type_pt::text, '') ILIKE %(q)s
                OR COALESCE(e.description_pt::text, '') ILIKE %(q)s
                )
            """)
            sql_params["q"] = f"%{q_raw}%"

        base_sql = sql.SQL("""
            FROM {mart_events_ui} e
            JOIN public.power_plant p ON p.id = e.power_plant_id
            WHERE {where_clause}
        """).format(
            mart_events_ui=q(RT_SCHEMA, "int_events_alarms"),
            where_clause=sql.SQL(" AND ").join([sql.SQL(w) for w in where])
        )

        data_sql_normal = sql.SQL("""
            SELECT
            e."timestamp" AS event_ts,
            p.customer_id,
            e.power_plant_id,
            e.power_plant_name,
            e.device_type_name AS device_type,
            e.device_id,
            e.device_name,
            LOWER(COALESCE(e.type_en, e.type_pt)) AS event_type,
            e.severity,
            e.code AS event_code,
            e.description_pt AS event_name,
            e.value AS event_value,
            LOWER(COALESCE(e.event_source, '')) AS source,
            CASE WHEN COALESCE(e.is_active_event, false) = true THEN 'active' ELSE 'inactive' END AS status
            {base_sql}
            ORDER BY e."timestamp" DESC, e.event_source ASC, e.device_id ASC
            LIMIT %(limit)s
            OFFSET %(offset)s
        """).format(base_sql=base_sql)

        count_sql_normal = sql.SQL("""
            SELECT COUNT(*) AS total
            {base_sql}
        """).format(base_sql=base_sql)

        data_sql_latest = sql.SQL("""
            WITH filtered AS (
            SELECT
                e."timestamp" AS event_ts,
                p.customer_id,
                e.power_plant_id,
                e.power_plant_name,
                e.device_type_name AS device_type,
                e.device_id,
                e.device_name,
                LOWER(COALESCE(e.type_en, e.type_pt)) AS event_type,
                e.severity,
                e.code AS event_code,
                e.description_pt AS event_name,
                e.value AS event_value,
                LOWER(COALESCE(e.event_source, '')) AS source,
                CASE WHEN COALESCE(e.is_active_event, false) = true THEN 'active' ELSE 'inactive' END AS status
            {base_sql}
            ),
            latest AS (
            SELECT DISTINCT ON (source, device_id)
                *
            FROM filtered
            ORDER BY
                source,
                device_id,
                event_ts DESC,
                CASE
                WHEN severity = 'high' THEN 3
                WHEN severity = 'medium' THEN 2
                WHEN severity = 'low' THEN 1
                ELSE 0
                END DESC,
                CASE
                WHEN event_type = 'alarm' THEN 3
                WHEN event_type = 'event' THEN 2
                WHEN event_type = 'status' THEN 1
                ELSE 0
                END DESC,
                event_code ASC
            )
            SELECT *
            FROM latest
            ORDER BY event_ts DESC, source ASC, device_id ASC
            LIMIT %(limit)s
            OFFSET %(offset)s
        """).format(base_sql=base_sql)

        count_sql_latest = sql.SQL("""
            SELECT COUNT(DISTINCT (LOWER(COALESCE(e.event_source, '')), e.device_id)) AS total
            {base_sql}
        """).format(base_sql=base_sql)

        data_sql_rounds = sql.SQL("""
            WITH filtered AS (
            SELECT
                e."timestamp" AS event_ts,
                date_trunc('second', e."timestamp") AS ts_bucket,
                p.customer_id,
                e.power_plant_id,
                e.power_plant_name,
                e.device_type_name AS device_type,
                e.device_id,
                e.device_name,
                LOWER(COALESCE(e.type_en, e.type_pt)) AS event_type,
                e.severity,
                e.code AS event_code,
                e.description_pt AS event_name,
                e.value AS event_value,
                LOWER(COALESCE(e.event_source, '')) AS source,
                CASE WHEN COALESCE(e.is_active_event, false) = true THEN 'active' ELSE 'inactive' END AS status
            {base_sql}
            ),
            buckets AS (
            SELECT DISTINCT ts_bucket
            FROM filtered
            ORDER BY ts_bucket DESC
            LIMIT %(rounds)s
            ),
            picked AS (
            SELECT DISTINCT ON (f.ts_bucket, f.source, f.device_id)
                f.*
            FROM filtered f
            JOIN buckets b ON b.ts_bucket = f.ts_bucket
            ORDER BY
                f.ts_bucket DESC,
                f.source ASC,
                f.device_id ASC,
                f.event_ts DESC,
                CASE
                WHEN f.severity = 'high' THEN 3
                WHEN f.severity = 'medium' THEN 2
                WHEN f.severity = 'low' THEN 1
                ELSE 0
                END DESC,
                CASE
                WHEN f.event_type = 'alarm' THEN 3
                WHEN f.event_type = 'event' THEN 2
                WHEN f.event_type = 'status' THEN 1
                ELSE 0
                END DESC,
                f.event_code ASC
            )
            SELECT
            event_ts,
            customer_id,
            power_plant_id,
            power_plant_name,
            device_type,
            device_id,
            device_name,
            event_type,
            severity,
            event_code,
            event_name,
            event_value,
            source,
            status
            FROM picked
            ORDER BY ts_bucket DESC, source ASC, device_id ASC
            LIMIT %(limit)s
            OFFSET %(offset)s
        """).format(base_sql=base_sql)

        count_sql_rounds = sql.SQL("""
            WITH filtered AS (
            SELECT
                date_trunc('second', e."timestamp") AS ts_bucket,
                LOWER(COALESCE(e.event_source, '')) AS source,
                e.device_id
            {base_sql}
            ),
            buckets AS (
            SELECT DISTINCT ts_bucket
            FROM filtered
            ORDER BY ts_bucket DESC
            LIMIT %(rounds)s
            )
            SELECT COUNT(DISTINCT (f.ts_bucket, f.source, f.device_id)) AS total
            FROM filtered f
            JOIN buckets b ON b.ts_bucket = f.ts_bucket
        """).format(base_sql=base_sql)

        if mode_q == "latest_per_device":
            data_sql = data_sql_latest
            count_sql = count_sql_latest
            mode_out = "latest_per_device"
        elif mode_q == "round_robin":
            data_sql = data_sql_rounds
            count_sql = count_sql_rounds
            mode_out = f"round_robin({rounds})"
        else:
            data_sql = data_sql_normal
            count_sql = count_sql_normal
            mode_out = "normal"

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SET LOCAL statement_timeout = '30000ms';")

            total = None
            total_pages = None

            if include_total:
                cur.execute(count_sql, sql_params)
                total = int(cur.fetchone()["total"])
                total_pages = (total + page_size - 1) // page_size

            cur.execute(data_sql, sql_params)
            rows = cur.fetchall() or []

            return http_response(200, {
                "items": rows,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": total_pages
                },
                "mode": mode_out,
                "meta": {
                    "source": f"{RT_SCHEMA}.int_events_alarms"
                }
            })
        except Exception as e:
            print("[/events] ERROR:", repr(e))
            print(traceback.format_exc())
            safe_params_log = dict(sql_params)
            safe_params_log["start_dt"] = str(sdt)
            safe_params_log["end_dt"] = str(edt)
            print("[/events] PARAMS:", json.dumps(safe_params_log, default=str))
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/events (alarmes)
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/events"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql.SQL("""
                SELECT
                    event_row_id,
                    event_source,
                    "timestamp",
                    power_plant_id,
                    power_plant_name,
                    device_id,
                    device_name,
                    device_type_name,
                    code AS event_code,
                    value AS event_value,
                    raw_key,
                    raw_value,
                    description_pt AS event_name,
                    severity,
                    point_name,
                    equipment_name,
                    is_active_event
                FROM {mart_alarm_state}
                WHERE power_plant_id = %(plant_id)s
                AND (
                    %(is_superuser)s = true
                    OR EXISTS (
                    SELECT 1
                    FROM public.power_plant p
                    WHERE p.id = %(plant_id)s
                        AND p.customer_id = %(customer_id)s
                    )
                )
                ORDER BY "timestamp" DESC
                LIMIT 50;
            """).format(mart_alarm_state=q(RT_SCHEMA, "int_events_alarms")), {
                "plant_id": int(plant_id),
                "customer_id": ctx["customer_id"],
                "is_superuser": ctx["is_superuser"]
            })
            return http_response(200, {
                "items": cur.fetchall() or [],
                "meta": {
                    "source": f"{RT_SCHEMA}.int_events_alarms"
                }
            })
        except Exception as e:
            print("[/plants/{id}/events] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/alarms/active
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/alarms/active"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql.SQL("""
                SELECT
                    event_row_id,
                    event_source,
                    "timestamp",
                    power_plant_id,
                    power_plant_name,
                    device_id,
                    device_name,
                    device_type_name,
                    code AS event_code,
                    value AS event_value,
                    raw_key,
                    raw_value,
                    description_pt AS event_name,
                    severity,
                    point_name,
                    equipment_name,
                    is_active_event
                FROM {mart_alarm_state}
                WHERE power_plant_id = %(plant_id)s
                AND (
                    %(is_superuser)s = true
                    OR EXISTS (
                    SELECT 1
                    FROM public.power_plant p
                    WHERE p.id = %(plant_id)s
                        AND p.customer_id = %(customer_id)s
                    )
                )
                AND COALESCE(is_active_event, false) = true
                ORDER BY "timestamp" DESC
                LIMIT 200;
            """).format(mart_alarm_state=q(RT_SCHEMA, "int_events_alarms")), {
                "plant_id": int(plant_id),
                "customer_id": ctx["customer_id"],
                "is_superuser": ctx["is_superuser"]
            })
            return http_response(200, {
                "items": cur.fetchall() or [],
                "meta": {
                    "source": f"{RT_SCHEMA}.int_events_alarms"
                }
            })
        except Exception as e:
            print("[/plants/{id}/alarms/active] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # POST /plants/{plant_id}/devices/{device_id}/command
    # ========================================================
    if method == "POST" and plant_id and device_id_fallback and path_contains(path, "/devices/") and is_path(path, "/command"):
        body = parse_json_body(event)
        if body is None:
            return http_response(400, {"error": "JSON inválido"})

        action = safe_lower(body.get("action"))
        username = (body.get("username") or "").strip()
        password = body.get("password")
        requested_by = (body.get("requested_by") or username or "operador").strip()
        power_value = body.get("value")  # kW — usado somente quando action == "set_power"

        if action not in ("on", "off", "reset", "set_power"):
            return http_response(400, {"error": "action inválida. Use: on, off, reset ou set_power"})
        if action == "set_power":
            try:
                power_value = float(power_value)
                if power_value < 0:
                    raise ValueError
            except (TypeError, ValueError):
                return http_response(400, {"error": "value obrigatório e deve ser número >= 0 para set_power"})
        if not username or not password:
            return http_response(400, {"error": "username e password são obrigatórios"})
        if not str(device_id_fallback).isdigit():
            return http_response(400, {"error": "device_id inválido"})

        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            user = validate_operational_user(cur, username, password, {"is_superuser": True})
            if not user:
                return http_response(401, {"error": "credenciais operacionais inválidas"})

            command_ctx = {
                "customer_id": user.get("customer_id"),
                "is_superuser": bool(user.get("is_superuser")),
                "user_id": user.get("id"),
                "username": user.get("username"),
                "role_key": user.get("role_key"),
                "permissions": normalize_permissions(user.get("permissions")),
            }

            if not can_send_command(command_ctx):
                return http_response(403, {
                    "error": "usuario sem permissao para comando remoto",
                    "required_permission": "remote_command",
                    "role_key": command_ctx.get("role_key"),
                })

            if not ensure_plant_access(cur, int(plant_id), command_ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            target = resolve_device_command_target(cur, int(plant_id), int(device_id_fallback), command_ctx)
            if not target:
                return http_response(404, {"error": "device não encontrado para esta usina"})

            customer_id = resolve_customer_id_for_plant(cur, int(plant_id), command_ctx)
            if customer_id is None:
                return http_response(400, {"error": "não foi possível resolver customer_id"})

            device_index = infer_device_index(target.get("device_name"), default_index=1)
            mqtt_topic = build_device_command_topic(
                power_plant_name=target.get("power_plant_name") or f"plant{plant_id}",
                device_type=target.get("device_type") or "device",
                device_index=device_index,
            )
            # Insere PENDING primeiro para obter command_id — usado no payload MQTT
            # para o CLP correlacionar o feedback de retorno
            command_id = insert_device_command_audit(
                cur,
                customer_id=customer_id,
                target=target,
                action=action,
                mqtt_topic=mqtt_topic,
                command_payload={},  # placeholder; atualizado abaixo com command_id
                requested_by=requested_by,
                requested_username=username,
                value_kw=power_value if action == "set_power" else None,
            )
            commit_quiet(conn)

            # Monta payload final com command_id embutido
            payload = build_device_command_payload(
                action=action,
                target=target,
                requested_by=requested_by,
                value=power_value if action == "set_power" else None,
                command_id=command_id,
            )

            # Atualiza command_payload com o payload final
            cur.execute(
                "UPDATE public.device_command SET command_payload = %(p)s::jsonb WHERE id = %(id)s",
                {"p": json.dumps(payload, ensure_ascii=False), "id": command_id},
            )
            update_device_command_audit(cur, command_id, status="SENT", set_started=True)
            commit_quiet(conn)

            try:
                pub = publish_device_command(mqtt_topic=mqtt_topic, payload=payload)
                update_device_command_audit(
                    cur,
                    command_id,
                    status="SUCCESS",
                    response_payload=pub,
                    set_finished=True,
                )
                commit_quiet(conn)

                # Estado derivado para o front atualizar a bolinha imediatamente
                device_state = {
                    "state": {"on": "on", "off": "off"}.get(action),  # None para reset/set_power
                    "power_setpoint_kw": power_value if action == "set_power" else None,
                    "last_command_action": action,
                }

                return http_response(200, {
                    "ok": True,
                    "command_id": command_id,
                    "status": "SUCCESS",
                    "action": action,
                    "mqtt_topic": mqtt_topic,
                    "payload": payload,
                    "device_state": device_state,
                })
            except Exception as pub_err:
                update_device_command_audit(
                    cur,
                    command_id,
                    status="FAILED",
                    status_message=str(pub_err),
                    response_payload={"ok": False, "error": str(pub_err)},
                    set_finished=True,
                )
                commit_quiet(conn)
                return http_response(500, {
                    "ok": False,
                    "command_id": command_id,
                    "status": "FAILED",
                    "mqtt_topic": mqtt_topic,
                    "payload": payload,
                    "error": "falha ao publicar comando MQTT",
                    "detail": str(pub_err),
                })
        except Exception as e:
            rollback_quiet(conn)
            print("[/plants/{id}/devices/{id}/command] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            if cur:
                cur.close()
            if conn:
                end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/commands/{command_id}/status
    # ========================================================
    if method == "GET" and plant_id and path_contains(path, "/commands/") and is_path(path, "/status"):
        m_cmd = re.search(r"/commands/(\d+)/status", path)
        if not m_cmd:
            return http_response(400, {"error": "command_id inválido na URL"})
        cmd_id = int(m_cmd.group(1))
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            cur.execute("""
                SELECT
                    id            AS command_id,
                    action,
                    status,
                    status_message,
                    mqtt_topic,
                    response_payload,
                    created_at,
                    started_at,
                    finished_at,
                    device_id,
                    device_type
                FROM public.device_command
                WHERE id              = %(command_id)s
                AND power_plant_id  = %(plant_id)s
                AND ( %(is_superuser)s = true OR customer_id = %(customer_id)s )
                LIMIT 1;
            """, {
                "command_id":   cmd_id,
                "plant_id":     int(plant_id),
                "customer_id":  ctx.get("customer_id"),
                "is_superuser": ctx.get("is_superuser", False),
            })
            row = cur.fetchone()
            if not row:
                return http_response(404, {"error": "comando não encontrado"})

            status = row["status"] or "UNKNOWN"
            # clp_ok: None = ainda aguardando, True = CLP confirmou sucesso, False = CLP reportou falha
            clp_ok = None
            clp_message = row.get("status_message")
            if status == "CLP_OK":
                clp_ok = True
            elif status == "CLP_FAILED":
                clp_ok = False

            return http_response(200, {
                "command_id":   int(row["command_id"]),
                "action":       row.get("action"),
                "status":       status,
                "clp_ok":       clp_ok,
                "clp_message":  clp_message,
                "device_id":    int(row["device_id"]) if row.get("device_id") is not None else None,
                "device_type":  row.get("device_type"),
                "mqtt_topic":   row.get("mqtt_topic"),
                "created_at":   str(row["created_at"]) if row.get("created_at") else None,
                "started_at":   str(row["started_at"]) if row.get("started_at") else None,
                "finished_at":  str(row["finished_at"]) if row.get("finished_at") else None,
            })
        except Exception as e:
            print("[GET /commands/{id}/status] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/devices/options
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/devices/options"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            cur.execute("""
                SELECT
                d.id AS device_id,
                d.name AS original_name,
                d.display_name,
                COALESCE(NULLIF(BTRIM(d.display_name), ''), d.name) AS device_name,
                COALESCE(NULLIF(BTRIM(d.display_name), ''), d.name) AS label,
                dt.name AS device_type,
                d.device_type_id,
                d.is_active
                FROM public.device d
                LEFT JOIN public.device_type dt
                ON dt.id = d.device_type_id
                WHERE d.power_plant_id = %(plant_id)s
                AND d.is_active = true
                ORDER BY d.device_type_id, d.id;
            """, {"plant_id": int(plant_id)})
            rows = cur.fetchall() or []

            items = [{
                "device_id": int(r["device_id"]),
                "original_name": r.get("original_name"),
                "display_name": r.get("display_name"),
                "device_name": r.get("device_name"),
                "device_type": r.get("device_type"),
                "label": r.get("label"),
                "device_type_id": r.get("device_type_id"),
                "is_active": bool(r["is_active"]) if r.get("is_active") is not None else None,
            } for r in rows]

            return http_response(200, {
                "items": items
            })
        except Exception as e:
            print("[GET /devices/options] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/devices/catalog
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/devices/catalog"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            cur.execute("""
                SELECT
                    d.id AS device_id,
                    LOWER(COALESCE(dt.name, '')) AS device_type_name
                FROM public.device d
                JOIN public.device_type dt ON dt.id = d.device_type_id
                WHERE d.power_plant_id = %(plant_id)s
                AND d.is_active = true
                ORDER BY d.id;
            """, {"plant_id": int(plant_id)})
            rows = cur.fetchall() or []

            has_relay = False
            has_transformer = False
            has_multimeter = False
            relay_device_id = None
            transformer_device_id = None
            multimeter_device_id = None

            for r in rows:
                dt = r.get("device_type_name", "")
                did = int(r["device_id"])
                if any(x in dt for x in ("relay", "relé", "rele", "proteção", "protecao")):
                    if not has_relay:
                        relay_device_id = did
                    has_relay = True
                elif any(x in dt for x in ("transform", "trafo")):
                    if not has_transformer:
                        transformer_device_id = did
                    has_transformer = True
                elif any(x in dt for x in ("multimeter", "multimedidor", "medidor", "meter")):
                    if not has_multimeter:
                        multimeter_device_id = did
                    has_multimeter = True

            # Breakers (disjuntores) cadastrados para esta planta
            breakers_list = []
            try:
                cur.execute("""
                    SELECT b.id, b.level, b.name, b.cabin_id, b.device_id
                    FROM public.breaker b
                    WHERE b.power_plant_id = %(plant_id)s
                    AND b.is_active = true
                    ORDER BY
                    CASE b.level WHEN 'djmt' THEN 1 WHEN 'djbt' THEN 2 WHEN 'djinv' THEN 3 END,
                    b.cabin_id NULLS FIRST,
                    b.device_id NULLS FIRST;
                """, {"plant_id": int(plant_id)})
                for br in (cur.fetchall() or []):
                    breakers_list.append({
                        "id": int(br["id"]),
                        "level": br["level"],
                        "name": br.get("name"),
                        "cabin_id": int(br["cabin_id"]) if br.get("cabin_id") is not None else None,
                        "device_id": int(br["device_id"]) if br.get("device_id") is not None else None,
                    })
            except Exception as e:
                print("[GET /devices/catalog] breaker query warning:", repr(e))

            return http_response(200, {
                "has_relay": has_relay,
                "has_transformer": has_transformer,
                "has_multimeter": has_multimeter,
                "relay_device_id": relay_device_id,
                "transformer_device_id": transformer_device_id,
                "multimeter_device_id": multimeter_device_id,
                "breakers": breakers_list,
            })
        except Exception as e:
            print("[GET /devices/catalog] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # DELETE /plants/{plant_id}/devices/{device_id}  (soft delete)
    if (method == "DELETE" and plant_id and device_id_fallback
            and path_contains(path, "/devices/")
            and not path_contains(path, "/name")
            and not path_contains(path, "/command")
            and not path_contains(path, "/strings")):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error: return auth_error
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            if not can_edit_device(ctx):
                return http_response(403, {"error": "sem permissao para remover dispositivo"})
            cur.execute("""
                UPDATE public.device
                SET is_active = false, updated_at = NOW()
                WHERE id = %(did)s AND power_plant_id = %(pid)s
                RETURNING id;
            """, {"did": int(device_id_fallback), "pid": int(plant_id)})
            row = cur.fetchone()
            if not row:
                return http_response(404, {"error": "Dispositivo nao encontrado"})
            conn.commit()
            return http_response(200, {"ok": True, "device_id": int(row["id"])})
        except Exception as e:
            conn.rollback()
            print("[DELETE /devices] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # PATCH /plants/{plant_id}/devices/{device_id}/name
    # ========================================================
    if method == "PATCH" and plant_id and device_id_fallback and path_contains(path, "/devices/") and is_path(path, "/name"):
        if not str(plant_id).isdigit():
            return http_response(400, {"error": "plant_id invalido"})
        if not str(device_id_fallback).isdigit():
            return http_response(400, {"error": "device_id invalido"})

        body = parse_json_body(event)
        if body is None:
            return http_response(400, {"error": "JSON invalido"})
        if not isinstance(body, dict):
            return http_response(400, {"error": "body deve ser um objeto JSON"})

        new_name = str(body.get("display_name", "")).strip()
        if not new_name:
            return http_response(400, {"error": "display_name nao pode ser vazio"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error:
                return auth_error

            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao para esta usina"})

            if not can_edit_device(ctx):
                return http_response(403, {"error": "sem permissão para renomear dispositivo"})

            cur.execute("""
                UPDATE public.device
                SET
                    display_name = %(display_name)s,
                    updated_at = NOW()
                WHERE id = %(device_id)s
                AND power_plant_id = %(plant_id)s
                RETURNING
                    id AS device_id,
                    display_name,
                    COALESCE(NULLIF(BTRIM(display_name), ''), name) AS device_name,
                    COALESCE(NULLIF(BTRIM(display_name), ''), name) AS label;
            """, {
                "display_name": new_name,
                "device_id": int(device_id_fallback),
                "plant_id": int(plant_id),
            })
            row = cur.fetchone()
            if not row:
                return http_response(404, {"error": "Device nao encontrado"})

            conn.commit()
            return http_response(200, {
                "ok": True,
                "device_id": int(row["device_id"]),
                "display_name": row.get("display_name") or new_name,
                "device_name": row.get("device_name") or new_name,
                "label": row.get("label") or new_name,
            })
        except Exception as e:
            print("[PATCH /devices/name] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # PATCH /plants/{plant_id}/name
    # ========================================================
    if method == "PATCH" and plant_id and is_path(path, "/name") and not path_contains(path, "/devices/"):
        if not str(plant_id).isdigit():
            return http_response(400, {"error": "plant_id invalido"})

        body = parse_json_body(event)
        if body is None:
            return http_response(400, {"error": "JSON invalido"})
        if not isinstance(body, dict):
            return http_response(400, {"error": "body deve ser um objeto JSON"})

        new_name = str(body.get("plant_name", "")).strip() or None
        raw_capacity = body.get("capacity_dc")

        # Validar capacity_dc se enviado
        new_capacity = None
        if raw_capacity is not None and raw_capacity != "":
            try:
                new_capacity = float(raw_capacity)
                if new_capacity < 0:
                    return http_response(400, {"error": "capacity_dc deve ser >= 0"})
            except (ValueError, TypeError):
                return http_response(400, {"error": "capacity_dc deve ser numerico"})

        if not new_name and new_capacity is None:
            return http_response(400, {"error": "Informe plant_name e/ou capacity_dc"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error:
                return auth_error

            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao para esta usina"})

            if not can_edit_plant(ctx):
                return http_response(403, {"error": "sem permissão para editar usina"})

            set_clauses = []
            params = {"plant_id": int(plant_id)}

            if new_name:
                set_clauses.append("display_name = %(display_name)s")
                params["display_name"] = new_name

            if new_capacity is not None:
                set_clauses.append("capacity_dc = %(capacity_dc)s")
                params["capacity_dc"] = new_capacity

            set_clauses.append("updated_at = NOW()")

            cur.execute(f"""
                UPDATE public.power_plant
                SET {', '.join(set_clauses)}
                WHERE id = %(plant_id)s
                RETURNING
                    id AS plant_id,
                    display_name,
                    capacity_dc,
                    COALESCE(NULLIF(BTRIM(display_name), ''), name) AS power_plant_name;
            """, params)
            row = cur.fetchone()
            if not row:
                return http_response(404, {"error": "Usina nao encontrada"})

            conn.commit()
            return http_response(200, {
                "ok": True,
                "plant_id": int(row["plant_id"]),
                "display_name": row.get("display_name") or new_name,
                "power_plant_name": row.get("power_plant_name") or new_name,
                "capacity_dc": float(row["capacity_dc"]) if row.get("capacity_dc") is not None else None,
            })
        except Exception as e:
            print("[PATCH /plants/name] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # PATCH /plants/{plant_id}/inverters/{inverter_id}/strings/{string_index}
    # ========================================================
    if method == "PATCH" and plant_id and path_contains(path, "/inverters/") and path_contains(path, "/strings/"):
        inverter_id = inverter_id_fallback
        string_index = string_index_fallback

        if not inverter_id or not str(inverter_id).isdigit():
            return http_response(400, {"error": "inverter_id inválido"})
        if not string_index or not str(string_index).isdigit():
            return http_response(400, {"error": "string_index inválido"})

        string_index = int(string_index)
        if string_index < 1 or string_index > 30:
            return http_response(400, {"error": "string_index deve ser entre 1 e 30"})

        body = parse_json_body(event)
        if body is None:
            return http_response(400, {"error": "JSON inválido"})

        enabled = body.get("enabled")
        if type(enabled) is not bool:
            return http_response(400, {"error": "campo 'enabled' (boolean) é obrigatório"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error:
                return auth_error

            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            if not can_edit_string_config(ctx):
                return http_response(403, {"error": "sem permissao para editar strings"})

            effective_customer_id = resolve_customer_id_for_plant(cur, int(plant_id), ctx)
            if effective_customer_id is None:
                return http_response(400, {"error": "não foi possível resolver customer_id efetivo pela usina"})

            cur.execute("""
                INSERT INTO public.inverter_string_config (
                customer_id, plant_id, inverter_id, string_index, enabled
                )
                VALUES (
                %(customer_id)s, %(plant_id)s, %(inverter_id)s, %(string_index)s, %(enabled)s
                )
                ON CONFLICT (customer_id, plant_id, inverter_id, string_index)
                DO UPDATE SET
                enabled = excluded.enabled,
                updated_at = now()
                RETURNING inverter_id, string_index, enabled, customer_id;
            """, {
                "customer_id": effective_customer_id,
                "plant_id": int(plant_id),
                "inverter_id": int(inverter_id),
                "string_index": int(string_index),
                "enabled": enabled
            })

            row = cur.fetchone()
            conn.commit()

            return http_response(200, {
                "inverter_id": int(row["inverter_id"]),
                "string_index": int(row["string_index"]),
                "enabled": bool(row["enabled"]),
                "customer_id": int(row["customer_id"]),
            })
        except Exception as e:
            print("[PATCH strings] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/inverters/{inverter_id}/strings (config)
    # ========================================================
    if method == "GET" and plant_id and path_contains(path, "/inverters/") and is_path(path, "/strings"):
        inverter_id = inverter_id_fallback
        if not inverter_id or not str(inverter_id).isdigit():
            return http_response(400, {"error": "inverter_id inválido"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            effective_customer_id = resolve_customer_id_for_plant(cur, int(plant_id), ctx)
            if effective_customer_id is None:
                return http_response(400, {"error": "não foi possível resolver customer_id efetivo pela usina"})

            max_s = 40 if int(effective_customer_id) == 3 else 30

            cur.execute(sql.SQL("""
                WITH all_strings AS (
                SELECT generate_series(1, %(max_strings)s) AS string_index
                ),
                cfg AS (
                SELECT string_index, enabled
                FROM public.inverter_string_config
                WHERE customer_id = %(customer_id)s
                    AND plant_id = %(plant_id)s
                    AND inverter_id = %(inverter_id)s
                ),
                data AS (
                SELECT DISTINCT string_index
                FROM {int_inverter_string}
                WHERE power_plant_id = %(plant_id)s
                    AND device_id = %(inverter_id)s
                )
                SELECT
                s.string_index,
                COALESCE(cfg.enabled, true) AS enabled,
                (data.string_index IS NOT NULL) AS has_data
                FROM all_strings s
                LEFT JOIN cfg  ON cfg.string_index  = s.string_index
                LEFT JOIN data ON data.string_index = s.string_index
                ORDER BY s.string_index;
            """).format(int_inverter_string=q(RT_SCHEMA, "stg_inverter_string")), {
                "customer_id": effective_customer_id,
                "plant_id": int(plant_id),
                "inverter_id": int(inverter_id),
                "max_strings": max_s,
            })

            rows = cur.fetchall() or []
            return http_response(200, {
                "inverter_id": int(inverter_id),
                "max_strings": max_s,
                "customer_id": int(effective_customer_id),
                "meta": {
                    "source": f"{RT_SCHEMA}.stg_inverter_string"
                },
                "strings": [
                    {
                        "string_index": int(r["string_index"]),
                        "enabled": bool(r["enabled"]),
                        "has_data": bool(r["has_data"]),
                    } for r in rows
                ]
            })
        except Exception as e:
            print("[GET strings] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/devices/states  (todos os devices da usina)
    # ========================================================
    if method == "GET" and plant_id and is_path(path, "/devices/states"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            cur.execute("""
                SELECT DISTINCT ON (device_id)
                    device_id,
                    device_type,
                    action          AS last_state,
                    value_kw        AS power_setpoint_kw,
                    status,
                    status_message,
                    created_at      AS last_command_at
                FROM public.device_command
                WHERE power_plant_id = %(plant_id)s
                AND status = 'SUCCESS'
                AND action IN ('on', 'off', 'set_power')
                AND ( %(is_superuser)s = true OR customer_id = %(customer_id)s )
                ORDER BY device_id, created_at DESC;
            """, {
                "plant_id":     int(plant_id),
                "customer_id":  ctx.get("customer_id"),
                "is_superuser": ctx.get("is_superuser", False),
            })
            rows = cur.fetchall() or []
            return http_response(200, {
                "items": [{
                    "device_id":           int(r["device_id"]),
                    "device_type":         r.get("device_type"),
                    "state":               r.get("last_state"),
                    "power_setpoint_kw":   float(r["power_setpoint_kw"]) if r.get("power_setpoint_kw") is not None else None,
                    "last_command_action": r.get("last_state"),
                    "last_command_at":     str(r["last_command_at"]) if r.get("last_command_at") else None,
                } for r in rows]
            })
        except Exception as e:
            print("[GET /devices/states] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /plants/{plant_id}/devices/{device_id}/state
    # ========================================================
    if method == "GET" and plant_id and device_id_fallback and path_contains(path, "/devices/") and is_path(path, "/state"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            cur.execute("""
                SELECT DISTINCT ON (device_id)
                    device_id,
                    device_type,
                    action          AS last_state,
                    value_kw        AS power_setpoint_kw,
                    status,
                    status_message,
                    response_payload,
                    created_at      AS last_command_at
                FROM public.device_command
                WHERE power_plant_id = %(plant_id)s
                AND device_id      = %(device_id)s
                AND status = 'SUCCESS'
                AND action IN ('on', 'off', 'set_power')
                AND ( %(is_superuser)s = true OR customer_id = %(customer_id)s )
                ORDER BY device_id, created_at DESC
                LIMIT 1;
            """, {
                "plant_id":     int(plant_id),
                "device_id":    int(device_id_fallback),
                "customer_id":  ctx.get("customer_id"),
                "is_superuser": ctx.get("is_superuser", False),
            })
            row = cur.fetchone()
            if not row:
                return http_response(200, {
                    "device_id": int(device_id_fallback),
                    "state": None,
                    "power_setpoint_kw": None,
                    "last_command_action": None,
                    "last_command_at": None,
                })
            return http_response(200, {
                "device_id":           int(row["device_id"]),
                "device_type":         row.get("device_type"),
                "state":               row.get("last_state"),
                "power_setpoint_kw":   float(row["power_setpoint_kw"]) if row.get("power_setpoint_kw") is not None else None,
                "last_command_action": row.get("last_state"),
                "status_message":      row.get("status_message"),
                "last_command_at":     str(row["last_command_at"]) if row.get("last_command_at") else None,
            })
        except Exception as e:
            print("[GET /devices/{id}/state] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # GET /plants/{plant_id}/device-types
    if method == "GET" and plant_id and is_path(path, "/device-types"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            cur.execute("SELECT id, name FROM public.device_type ORDER BY name;")
            rows = cur.fetchall() or []
            return http_response(200, {"items": [{"id": r["id"], "name": r["name"]} for r in rows]})
        except Exception as e:
            print("[GET /device-types] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # PATCH /plants/{plant_id}/devices/{device_id}/cabin  (vincular inversor a cabine)
    if method == "PATCH" and plant_id and device_id_fallback and path_contains(path, "/devices/") and is_path(path, "/cabin"):
        body = parse_json_body(event)
        cabin_id = body.get("cabin_id") if body else None  # None = desvincular
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error: return auth_error
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            if not can_edit_device(ctx):
                return http_response(403, {"error": "sem permissao para editar dispositivo"})
            cur.execute("""
                UPDATE public.device
                SET cabin_id = %(cabin_id)s, updated_at = NOW()
                WHERE id = %(did)s AND power_plant_id = %(pid)s
                RETURNING id AS device_id, cabin_id;
            """, {"cabin_id": int(cabin_id) if cabin_id else None,
                "did": int(device_id_fallback), "pid": int(plant_id)})
            row = cur.fetchone()
            if not row:
                return http_response(404, {"error": "dispositivo nao encontrado"})
            conn.commit()
            return http_response(200, {"ok": True, "device_id": int(row["device_id"]),
                                    "cabin_id": int(row["cabin_id"]) if row["cabin_id"] else None})
        except Exception as e:
            conn.rollback()
            print("[PATCH /devices/cabin] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # POST /plants/{plant_id}/devices  (adicionar dispositivo)
    if method == "POST" and plant_id and is_path(path, "/devices") and not path_contains(path, "/devices/"):
        body = parse_json_body(event)
        if not body or not isinstance(body, dict):
            return http_response(400, {"error": "JSON invalido"})
        device_type_id = body.get("device_type_id")
        display_name   = str(body.get("display_name") or "").strip()
        name           = str(body.get("name") or display_name).strip()
        if not device_type_id or not name:
            return http_response(400, {"error": "device_type_id e name sao obrigatorios"})
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error: return auth_error
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            if not can_edit_device(ctx):
                return http_response(403, {"error": "sem permissao para adicionar dispositivo"})
            cur.execute("""
                INSERT INTO public.device
                (power_plant_id, device_type_id, name, display_name, is_active, created_at, updated_at)
                VALUES (%(plant_id)s, %(dt_id)s, %(name)s, %(dname)s, true, NOW(), NOW())
                RETURNING id AS device_id, name, display_name;
            """, {"plant_id": int(plant_id), "dt_id": int(device_type_id),
                "name": name, "dname": display_name or name})
            row = cur.fetchone()
            conn.commit()
            return http_response(201, {"ok": True, "device_id": int(row["device_id"]),
                                    "name": row["name"], "display_name": row.get("display_name")})
        except Exception as e:
            conn.rollback()
            print("[POST /devices] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # GET /plants/{plant_id}/cabin-groups  (grupos organizacionais = public.cabin)
    if method == "GET" and plant_id and is_path(path, "/cabin-groups"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            cur.execute("""
                SELECT c.id, c.name, c.code, c.display_order,
                    COUNT(d.id) FILTER (WHERE d.power_plant_id = %(pid)s
                                            AND d.is_active = true) AS inverter_count
                FROM public.cabin c
                LEFT JOIN public.device d ON d.cabin_id = c.id
                                        AND d.power_plant_id = %(pid)s
                                        AND d.is_active = true
                WHERE c.id IN (
                SELECT DISTINCT cabin_id FROM public.device
                WHERE power_plant_id = %(pid)s AND is_active = true
                AND cabin_id IS NOT NULL
                )
                GROUP BY c.id, c.name, c.code, c.display_order
                ORDER BY c.display_order, c.id;
            """, {"pid": int(plant_id)})
            rows = cur.fetchall() or []
            return http_response(200, {"items": [
                {"id": int(r["id"]), "name": r["name"], "code": r.get("code"),
                "display_order": r.get("display_order"), "inverter_count": int(r["inverter_count"] or 0)}
                for r in rows
            ]})
        except Exception as e:
            print("[GET /cabin-groups] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # POST /plants/{plant_id}/cabin-groups  (criar grupo organizacional)
    if method == "POST" and plant_id and is_path(path, "/cabin-groups"):
        body = parse_json_body(event)
        if not body or not isinstance(body, dict):
            return http_response(400, {"error": "JSON invalido"})
        name = str(body.get("name") or "").strip()
        if not name:
            return http_response(400, {"error": "name obrigatorio"})
        code = str(body.get("code") or name).strip()
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error: return auth_error
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            if not can_edit_plant(ctx):
                return http_response(403, {"error": "sem permissao para adicionar cabine"})
            cur.execute("""
                INSERT INTO public.cabin (name, code, display_order, created_at, updated_at)
                VALUES (%(name)s, %(code)s,
                        COALESCE((SELECT MAX(display_order)+1 FROM public.cabin), 1),
                        NOW(), NOW())
                RETURNING id, name, code, display_order;
            """, {"name": name, "code": code})
            row = cur.fetchone()
            conn.commit()
            return http_response(201, {"ok": True, "id": int(row["id"]), "name": row["name"],
                                    "code": row.get("code"), "display_order": row.get("display_order")})
        except Exception as e:
            conn.rollback()
            print("[POST /cabin-groups] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # GET /plants/{plant_id}/cabines
    if method == "GET" and plant_id and is_path(path, "/cabines"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            cur.execute("""
                SELECT d.id AS device_id,
                    COALESCE(NULLIF(BTRIM(d.display_name),''), d.name) AS device_name,
                    dt.name AS device_type
                FROM public.device d
                JOIN public.device_type dt ON dt.id = d.device_type_id
                WHERE d.power_plant_id = %(pid)s
                AND LOWER(dt.name) IN ('relay','rele','relé','cabine','switchboard','cabinet')
                AND d.is_active = true
                ORDER BY d.id;
            """, {"pid": int(plant_id)})
            rows = cur.fetchall() or []
            return http_response(200, {"items": [
                {"device_id": int(r["device_id"]),
                "device_name": r["device_name"],
                "device_type": r["device_type"]} for r in rows
            ]})
        except Exception as e:
            print("[GET /cabines] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # POST /plants/{plant_id}/cabines  (criar cabine)
    if method == "POST" and plant_id and is_path(path, "/cabines"):
        body = parse_json_body(event)
        if not body or not isinstance(body, dict):
            return http_response(400, {"error": "JSON invalido"})
        display_name = str(body.get("display_name") or "").strip()
        if not display_name:
            return http_response(400, {"error": "display_name obrigatorio"})
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            auth_error = require_current_user(cur, ctx)
            if auth_error: return auth_error
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissao"})
            if not can_edit_plant(ctx):
                return http_response(403, {"error": "sem permissao para adicionar cabine"})
            cur.execute("""
                SELECT id FROM public.device_type
                WHERE LOWER(name) IN ('relay','rele','relé','cabine','switchboard','cabinet')
                ORDER BY id LIMIT 1;
            """)
            dt_row = cur.fetchone()
            if not dt_row:
                return http_response(400, {"error": "device_type relay/cabine nao encontrado no banco"})
            cur.execute("""
                INSERT INTO public.device
                (power_plant_id, device_type_id, name, display_name, is_active, created_at, updated_at)
                VALUES (%(pid)s, %(dt_id)s, %(name)s, %(dname)s, true, NOW(), NOW())
                RETURNING id AS device_id;
            """, {"pid": int(plant_id), "dt_id": int(dt_row["id"]),
                "name": display_name, "dname": display_name})
            row = cur.fetchone()
            conn.commit()
            return http_response(201, {"ok": True, "device_id": int(row["device_id"]),
                                    "display_name": display_name})
        except Exception as e:
            conn.rollback()
            print("[POST /cabines] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # POST /users/notif-prefs — salvar preferências de notificação do robô
    if method == "POST" and is_path(path, "/users/notif-prefs"):
        body = parse_json_body(event)
        if not body or not isinstance(body, dict):
            return http_response(400, {"error": "JSON invalido"})
        prefs = body.get("prefs") or {}
        if not isinstance(prefs, dict):
            return http_response(400, {"error": "prnefs deve ser um objeto"})

        username = ctx.get("username")
        if not username:
            return http_response(401, {"error": "usuario ausente"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                UPDATE public.app_user
                SET notif_prefs = %(prefs)s,
                    updated_at = NOW()
                WHERE username = %(username)s
                RETURNING id;
            """, {
                "prefs": json.dumps(prefs),
                "username": username
            })
            row = cur.fetchone()
            if not row:
                return http_response(404, {"error": "usuario nao encontrado"})
            conn.commit()
            return http_response(200, {"ok": True})
        except Exception as e:
            conn.rollback()
            print("[POST /users/notif-prefs] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # GET /users/notif-prefs — carregar preferências salvas
    if method == "GET" and is_path(path, "/users/notif-prefs"):
        username = ctx.get("username")
        if not username:
            return http_response(401, {"error": "usuario ausente"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                SELECT notif_prefs
                FROM public.app_user
                WHERE username = %(username)s;
            """, {"username": username})
            row = cur.fetchone()
            prefs = {}
            if row and row.get("notif_prefs"):
                raw = row["notif_prefs"]
                prefs = raw if isinstance(raw, dict) else json.loads(raw)
            return http_response(200, {"prefs": prefs})
        except Exception as e:
            print("[GET /users/notif-prefs] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ------------------------
    # POST /push/subscribe — registra subscription Web Push do navegador
    # ------------------------
    if method == "POST" and is_path(path, "/push/subscribe"):
        body = parse_json_body(event)
        if not body or not isinstance(body, dict):
            return http_response(400, {"error": "JSON invalido"})

        sub = body.get("subscription") or {}
        endpoint = sub.get("endpoint")
        if not endpoint:
            return http_response(400, {"error": "subscription.endpoint ausente"})

        keys = sub.get("keys") or {}
        p256dh = keys.get("p256dh")
        auth_key = keys.get("auth")
        user_agent = get_header(event, "User-Agent")

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                INSERT INTO public.push_subscription
                    (user_id, username, customer_id, endpoint, p256dh, auth,
                    subscription, user_agent, is_active, created_at, last_seen_at)
                VALUES
                    (%(user_id)s, %(username)s, %(customer_id)s, %(endpoint)s,
                    %(p256dh)s, %(auth)s, %(subscription)s::jsonb, %(user_agent)s,
                    true, now(), now())
                ON CONFLICT (endpoint) DO UPDATE SET
                    user_id      = EXCLUDED.user_id,
                    username     = EXCLUDED.username,
                    customer_id  = EXCLUDED.customer_id,
                    p256dh       = EXCLUDED.p256dh,
                    auth         = EXCLUDED.auth,
                    subscription = EXCLUDED.subscription,
                    user_agent   = EXCLUDED.user_agent,
                    is_active    = true,
                    last_seen_at = now()
                RETURNING id;
            """, {
                "user_id": ctx.get("user_id"),
                "username": ctx.get("username"),
                "customer_id": ctx.get("customer_id"),
                "endpoint": endpoint,
                "p256dh": p256dh,
                "auth": auth_key,
                "subscription": json.dumps(sub),
                "user_agent": user_agent,
            })
            row = cur.fetchone()
            conn.commit()
            return http_response(201, {"ok": True, "id": int(row["id"]) if row else None})
        except Exception as e:
            conn.rollback()
            print("[POST /push/subscribe] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /raw/tables — lista tabelas raw disponíveis
    # ========================================================
    if method == "GET" and is_path(path, "/raw/tables"):
        RAW_TABLES = [
            {"table": "raw_inverter",        "description": "Telemetria bruta de inversores"},
            {"table": "raw_relay",           "description": "Telemetria bruta de relés"},
            {"table": "raw_meter",           "description": "Telemetria bruta de multimedidores"},
            {"table": "raw_weather_station", "description": "Telemetria bruta de estações meteorológicas"},
            {"table": "raw_tracker",         "description": "Telemetria bruta de trackers (TCU)"},
            {"table": "raw_transformer",     "description": "Telemetria bruta de transformadores"},
            {"table": "raw_nobreak",         "description": "Telemetria bruta de nobreaks"},
            {"table": "raw_logger",          "description": "Telemetria bruta de loggers"},
        ]
        return http_response(200, {"tables": RAW_TABLES})

    # ========================================================
    # GET /raw/query?table=raw_inverter&plant_id=13&device_id=5&limit=50&offset=0&start=...&end=...
    # ========================================================
    if method == "GET" and is_path(path, "/raw/query"):
        ALLOWED_RAW = {
            "raw_inverter", "raw_relay", "raw_meter", "raw_weather_station",
            "raw_tracker", "raw_transformer", "raw_nobreak", "raw_logger",
        }
        table_name = safe_lower(params.get("table"))
        if table_name not in ALLOWED_RAW:
            return http_response(400, {
                "error": f"table inválida: {params.get('table')}",
                "allowed": sorted(ALLOWED_RAW),
            })

        try:
            row_limit = max(1, min(500, int(params.get("limit") or 100)))
        except (ValueError, TypeError):
            row_limit = 100
        try:
            row_offset = max(0, int(params.get("offset") or 0))
        except (ValueError, TypeError):
            row_offset = 0

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SET LOCAL statement_timeout = '15000ms';")
            load_current_user(cur, ctx)

            where_clauses = []
            bind = {}

            if not ctx["is_superuser"]:
                where_clauses.append("""
                    r.power_plant_id IN (
                        SELECT id FROM public.power_plant
                        WHERE customer_id = %(customer_id)s AND is_active = true
                    )
                """)
                bind["customer_id"] = ctx["customer_id"]

            plant_id_q = params.get("plant_id")
            if plant_id_q and str(plant_id_q).isdigit():
                where_clauses.append("r.power_plant_id = %(plant_id)s")
                bind["plant_id"] = int(plant_id_q)

            device_id_q = params.get("device_id")
            if device_id_q and str(device_id_q).isdigit():
                where_clauses.append("r.device_id = %(device_id)s")
                bind["device_id"] = int(device_id_q)

            start_q = (params.get("start") or "").strip()
            if start_q:
                sdt = parse_time_to_dt(start_q)
                if sdt:
                    where_clauses.append('r."timestamp" >= %(start_dt)s')
                    bind["start_dt"] = sdt
            else:
                where_clauses.append("r.\"timestamp\" >= now() - interval '24 hours'")

            end_q = (params.get("end") or "").strip()
            if end_q:
                edt = parse_time_to_dt(end_q)
                if edt:
                    where_clauses.append('r."timestamp" <= %(end_dt)s')
                    bind["end_dt"] = edt

            where_sql = " AND ".join(where_clauses) if where_clauses else "true"
            bind["row_limit"] = row_limit
            bind["row_offset"] = row_offset

            data_query = sql.SQL("""
                SELECT
                    r."timestamp",
                    r.power_plant_id,
                    r.device_id,
                    r.json_data
                FROM {tbl} r
                WHERE {where}
                ORDER BY r."timestamp" DESC
                LIMIT %(row_limit)s OFFSET %(row_offset)s
            """).format(
                tbl=sql.Identifier("public", table_name),
                where=sql.SQL(where_sql),
            )
            cur.execute(data_query, bind)
            rows = cur.fetchall() or []

            items = []
            for r in rows:
                jd = r.get("json_data")
                if isinstance(jd, str):
                    try:
                        jd = json.loads(jd)
                    except Exception:
                        pass
                items.append({
                    "timestamp": r.get("timestamp"),
                    "power_plant_id": r.get("power_plant_id"),
                    "device_id": r.get("device_id"),
                    "json_data": jd,
                })

            has_more = len(items) == row_limit
            total_estimate = row_offset + len(items) + (1 if has_more else 0)

            return http_response(200, {
                "table": table_name,
                "items": items,
                "total": total_estimate,
                "limit": row_limit,
                "offset": row_offset,
                "has_more": has_more,
                "meta": {"source": f"public.{table_name}"},
            })
        except Exception as e:
            print("[/raw/query] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # TICKETS / SUPORTE
    # ========================================================

    # POST /tickets — criar ticket
    if method == "POST" and is_path(path, "/tickets"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            b = parse_json_body(event) or {}
            title = (b.get("title") or "").strip()
            desc = (b.get("description") or "").strip()
            tk_plant_id = b.get("plant_id") or None
            priority = b.get("priority") or "medium"
            image_url = (b.get("image_url") or "").strip() or None
            if not title:
                return http_response(400, {"error": "title é obrigatório"})
            if priority not in ("low", "medium", "high"):
                priority = "medium"
            cur.execute("""
                INSERT INTO app.tickets (customer_id, username, plant_id, title, description, priority, image_url)
                VALUES (%(cid)s, %(user)s, %(pid)s, %(title)s, %(desc)s, %(prio)s, %(img)s)
                RETURNING id, customer_id, username, plant_id, title, description, priority, status, image_url, created_at;
            """, {
                "cid": ctx["customer_id"], "user": ctx["username"], "pid": tk_plant_id,
                "title": title, "desc": desc, "prio": priority, "img": image_url
            })
            row = cur.fetchone()
            conn.commit()
            return http_response(201, row)
        except Exception as e:
            conn.rollback()
            print("[POST /tickets] ERROR:", repr(e), traceback.format_exc())
            return http_response(500, {"error": str(e)})
        finally:
            cur.close()
            end_request(conn)

    # GET /tickets — listar tickets (admin vê todos, cliente vê os dele)
    if method == "GET" and is_path(path, "/tickets"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            status_filter = (params.get("status") or "").strip()
            lim = max(1, min(100, int(params.get("limit") or 50)))
            off = max(0, int(params.get("offset") or 0))

            where_parts = ["1=1"]
            qvals = {}
            if not ctx["is_superuser"]:
                where_parts.append("t.customer_id = %(cid)s")
                qvals["cid"] = ctx["customer_id"]
            if status_filter and status_filter != "all":
                where_parts.append("t.status = %(st)s")
                qvals["st"] = status_filter

            w = " AND ".join(where_parts)
            qvals["lim"] = lim
            qvals["off"] = off
            cur.execute(f"""
                SELECT t.*, pp.name AS plant_name,
                       (SELECT COUNT(*) FROM app.ticket_comments c WHERE c.ticket_id = t.id) AS comment_count
                FROM app.tickets t
                LEFT JOIN public.power_plant pp ON pp.id = t.plant_id
                WHERE {w}
                ORDER BY
                  CASE t.status WHEN 'open' THEN 0 WHEN 'in_progress' THEN 1 ELSE 2 END,
                  t.updated_at DESC
                LIMIT %(lim)s OFFSET %(off)s;
            """, qvals)
            rows = cur.fetchall() or []
            _tk_enrich_rows(rows)

            count_vals = {k: v for k, v in qvals.items() if k not in ("lim", "off")}
            cur.execute(f"SELECT COUNT(*) FROM app.tickets t WHERE {w}", count_vals)
            total = cur.fetchone()["count"]

            return http_response(200, {"items": rows, "total": total})
        except Exception as e:
            print("[GET /tickets] ERROR:", repr(e), traceback.format_exc())
            return http_response(500, {"error": str(e)})
        finally:
            cur.close()
            end_request(conn)

    # GET /tickets/{id} — detalhe do ticket + comentários
    if method == "GET" and re.match(r"^/tickets/\d+$", path):
        ticket_id = int(path.split("/")[-1])
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                SELECT t.*, pp.name AS plant_name
                FROM app.tickets t
                LEFT JOIN public.power_plant pp ON pp.id = t.plant_id
                WHERE t.id = %(tid)s;
            """, {"tid": ticket_id})
            ticket = cur.fetchone()
            if not ticket:
                return http_response(404, {"error": "Ticket não encontrado"})
            if not ctx["is_superuser"] and str(ticket["customer_id"]) != str(ctx["customer_id"]):
                return http_response(403, {"error": "Acesso negado"})

            cur.execute("""
                SELECT * FROM app.ticket_comments
                WHERE ticket_id = %(tid)s
                ORDER BY created_at ASC;
            """, {"tid": ticket_id})
            comments = cur.fetchall() or []
            _tk_enrich_rows([ticket])
            _tk_enrich_rows(comments)

            return http_response(200, {"ticket": ticket, "comments": comments})
        except Exception as e:
            print("[GET /tickets/{id}] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # PUT /tickets/{id} — atualizar status (admin)
    if method == "PUT" and re.match(r"^/tickets/\d+$", path):
        ticket_id = int(path.split("/")[-1])
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT customer_id FROM app.tickets WHERE id = %(tid)s;", {"tid": ticket_id})
            tk_row = cur.fetchone()
            if not tk_row:
                return http_response(404, {"error": "Ticket não encontrado"})
            if not ctx["is_superuser"] and str(tk_row["customer_id"]) != str(ctx["customer_id"]):
                return http_response(403, {"error": "Acesso negado"})

            b = parse_json_body(event) or {}
            new_status = (b.get("status") or "").strip()
            if new_status and new_status not in ("open", "in_progress", "resolved"):
                return http_response(400, {"error": "status inválido"})

            sets = ["updated_at = NOW()"]
            vals = {"tid": ticket_id}
            if new_status:
                sets.append("status = %(st)s")
                vals["st"] = new_status

            cur.execute(f"""
                UPDATE app.tickets SET {', '.join(sets)}
                WHERE id = %(tid)s RETURNING *;
            """, vals)
            row = cur.fetchone()
            conn.commit()
            return http_response(200, row)
        except Exception as e:
            conn.rollback()
            print("[PUT /tickets/{id}] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # POST /tickets/{id}/comments — adicionar comentário
    if method == "POST" and re.match(r"^/tickets/\d+/comments$", path):
        parts = path.split("/")
        ticket_id = int(parts[2])
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT customer_id FROM app.tickets WHERE id = %(tid)s;", {"tid": ticket_id})
            tk_row = cur.fetchone()
            if not tk_row:
                return http_response(404, {"error": "Ticket não encontrado"})
            if not ctx["is_superuser"] and str(tk_row["customer_id"]) != str(ctx["customer_id"]):
                return http_response(403, {"error": "Acesso negado"})

            b = parse_json_body(event) or {}
            text = (b.get("text") or "").strip()
            image_url = (b.get("image_url") or "").strip() or None
            if not text and not image_url:
                return http_response(400, {"error": "text ou image_url é obrigatório"})

            is_admin = ctx["is_superuser"]
            author = ctx["username"] or "Usuário"

            cur.execute("""
                INSERT INTO app.ticket_comments (ticket_id, author, text, image_url, is_admin)
                VALUES (%(tid)s, %(author)s, %(text)s, %(img)s, %(adm)s)
                RETURNING *;
            """, {"tid": ticket_id, "author": author, "text": text, "img": image_url, "adm": is_admin})
            comment = cur.fetchone()

            cur.execute("UPDATE app.tickets SET updated_at = NOW() WHERE id = %(tid)s;", {"tid": ticket_id})
            conn.commit()
            return http_response(201, comment)
        except Exception as e:
            conn.rollback()
            print("[POST /tickets/{id}/comments] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # POST /tickets/upload — gerar presigned URL para upload de imagem no S3 (mesmo bucket da OS)
    if method == "POST" and is_path(path, "/tickets/upload"):
        try:
            b = parse_json_body(event) or {}
            filename = (b.get("filename") or "").strip()
            content_type = (b.get("content_type") or "image/png").strip()
            if not filename:
                return http_response(400, {"error": "filename é obrigatório"})

            bucket = os.getenv("OS_S3_BUCKET", "").strip()
            if not bucket:
                return http_response(500, {"error": "OS_S3_BUCKET não configurado"})

            safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
            s3_key = f"tickets/{ctx['customer_id']}/{int(time.time())}_{safe_name}"

            s3_client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
            presigned_put = s3_client.generate_presigned_url(
                "put_object",
                Params={"Bucket": bucket, "Key": s3_key, "ContentType": content_type},
                ExpiresIn=300
            )
            presigned_get = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": s3_key},
                ExpiresIn=3600
            )
            return http_response(200, {"upload_url": presigned_put, "public_url": presigned_get, "s3_key": s3_key})
        except Exception as e:
            print("[POST /tickets/upload] ERROR:", repr(e))
            return http_response(500, {"error": "Erro ao gerar URL de upload"})

    # ========================================================
    # GET /platform/updates?limit=20
    # ========================================================
    if method == "GET" and is_path(path, "/platform/updates"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            lim = max(1, min(50, int(params.get("limit") or 20)))
            cur.execute("""
                SELECT id, title, description, created_at
                FROM app.platform_update
                WHERE is_active = true
                ORDER BY id DESC
                LIMIT %(lim)s;
            """, {"lim": lim})
            rows = cur.fetchall() or []
            return http_response(200, {"items": rows})
        except Exception as e:
            if "does not exist" in str(e):
                return http_response(200, {"items": []})
            print("[/platform/updates] ERROR:", repr(e))
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /raw/id-legend?table=raw_inverter
    # ========================================================
    if method == "GET" and is_path(path, "/raw/id-legend"):
        TABLE_TO_TYPE_HINT = {
            "raw_inverter": "inverter",
            "raw_relay": "relay",
            "raw_meter": "multimeter",
            "raw_weather_station": "weather_station",
            "raw_tracker": "tracker",
            "raw_transformer": "transformer",
            "raw_nobreak": "nobreak",
            "raw_logger": "logger",
        }
        tbl = safe_lower(params.get("table"))
        type_hint = TABLE_TO_TYPE_HINT.get(tbl)
        if not type_hint:
            return http_response(400, {"error": "table inválida", "allowed": sorted(TABLE_TO_TYPE_HINT.keys())})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                SELECT c.code,
                       c.description_pt,
                       c.description_en,
                       c.type_en,
                       c.severity,
                       c.value,
                       c.status_description_pt,
                       c.status_description_en
                FROM public.tb_event_alarm_catalog c
                JOIN public.device_type dt ON dt.id = c.device_type_id
                WHERE LOWER(dt.name) LIKE %(tp)s
                  AND c.code ~ '^ID[0-9]+$'
                  AND c.is_active = true
                ORDER BY (regexp_replace(c.code, '[^0-9]', '', 'g'))::int, c.value
            """, {"tp": f"%{type_hint}%"})
            rows = cur.fetchall() or []

            legend = {}
            for r in rows:
                code = r["code"]
                desc = (r.get("description_pt") or r.get("description_en") or code).strip()
                if code not in legend or r.get("value") == 1:
                    legend[code] = {
                        "code": code,
                        "description": desc,
                        "type": r.get("type_en"),
                        "severity": r.get("severity"),
                    }

            items = sorted(legend.values(), key=lambda x: int(x["code"].replace("ID", "")) if x["code"].replace("ID", "").isdigit() else 0)
            return http_response(200, {"table": tbl, "device_type": type_hint, "items": items})
        except Exception as e:
            print("[/raw/id-legend] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error"})
        finally:
            cur.close()
            end_request(conn)

    # ========================================================
    # GET /openapi.json — OpenAPI 3.0 spec
    # ========================================================
    if method == "GET" and is_path(path, "/openapi.json"):
        spec = _build_openapi_spec()
        return http_response(200, spec)

    return http_response(404, {"error": "rota não encontrada", "path": path, "method": method})


def lambda_handler(event, context):
    """
    Wrapper de segurança: garante que CORS headers são sempre devolvidos,
    mesmo em crashes não tratados (evita 502/504 sem CORS do API Gateway).
    """
    try:
        return _lambda_handler_impl(event, context)
    except Exception as e:
        print("[lambda_handler] ERRO NÃO TRATADO:", repr(e))
        print(traceback.format_exc())
        return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
