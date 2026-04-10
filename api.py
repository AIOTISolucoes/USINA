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
from datetime import datetime, timedelta
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
# Para produção:
#   ANALYTICS_SCHEMA=analytics
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
    return psycopg2.connect(**DB_CONFIG)


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


def end_request(conn):
    rollback_quiet(conn)


# ============================================================
# CORS
# ============================================================

def build_cors_headers():
    allow_auth = str(os.environ.get("CORS_ALLOW_AUTH", "true")).lower() == "true"
    allow_headers = "Content-Type,X-Customer-Id,X-Is-Superuser"
    if allow_auth:
        allow_headers += ",Authorization"

    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": allow_headers,
        "Access-Control-Allow-Methods": "GET,POST,PATCH,OPTIONS",
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
    return {
        "customer_id": int(customer_id) if customer_id and str(customer_id).isdigit() else None,
        "is_superuser": parse_bool(is_superuser, default=False),
    }


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
        SELECT id, username, password_hash, customer_id, is_superuser, is_active
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


def build_device_command_payload(*, action: str, target: dict, requested_by: str):
    return {
        "action": action,
        "device_id": int(target["device_id"]),
        "device_type": target.get("device_type"),
        "power_plant_id": int(target["power_plant_id"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "requested_by": requested_by,
    }


def insert_device_command_audit(cur, *, customer_id, target: dict, action: str, mqtt_topic: str, command_payload: dict, requested_by: str, requested_username: str):
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

    iot = boto3.client("iot")
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
            retries={
                "max_attempts": 3,
                "mode": "standard"
            }
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

INVERTER_ONLINE_WINDOW = os.getenv("INVERTER_ONLINE_WINDOW", "15 minutes")
STRING_ONLINE_WINDOW = os.getenv("STRING_ONLINE_WINDOW", "15 minutes")
RELAY_ONLINE_WINDOW = os.getenv("RELAY_ONLINE_WINDOW", "15 minutes")
MULTIMETER_ONLINE_WINDOW = os.getenv("MULTIMETER_ONLINE_WINDOW", "15 minutes")
TRACKER_ONLINE_WINDOW = os.getenv("TRACKER_ONLINE_WINDOW", "15 minutes")


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
}

DATASTUDIO_MONTHLY_PATHS = {
    "PLANT.energy_kwh_monthly": {
        "column": "energy_kwh",
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
    ):
        return "sum"

    return "avg"


def datastudio_consolidado_15min_value_column(pathname: str):
    """
    Para o '5min consolidado' vamos usar a tabela de 15min.
    Como são grandezas instantâneas de planta, usamos avg_value.
    """
    return "avg_value"


def datastudio_resolve_source_type(pathname: str):
    if pathname in DATASTUDIO_INTRADAY_PATHS:
        return "intraday"
    if pathname in DATASTUDIO_DAILY_PATHS:
        return "daily"
    if pathname in DATASTUDIO_MONTHLY_PATHS:
        return "monthly"
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
        return "mart_datastudio_hist_15min", "hist_15min"
    if days <= 15:
        return "mart_datastudio_hist_hourly", "hist_hourly"
    return "mart_datastudio_hist_daily", "hist_daily"


def datastudio_hist_agg_column(aggregation: str):
    agg = safe_lower(aggregation)

    if agg in ("sum", "soma"):
        return "sum_value"

    if agg in ("max", "maxima", "máxima"):
        return "max_value"

    # por enquanto:
    # avg / media / sem_agregacao / raw -> avg_value
    return "avg_value"


def fetch_datastudio_points(cur, *, customer_id: int, power_plant_id: int, pathname: str,
                            start_ts, end_ts, effective_source: str,
                            effective_aggregation: str = None,
                            limit: int = 5000):
    route_type = datastudio_resolve_source_type(pathname)

    if route_type == "intraday":
        hist_table_name, resolved_hist_route = datastudio_choose_hist_table(start_ts, end_ts)
        value_col = datastudio_hist_agg_column(effective_aggregation)

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
    cur.execute(sql.SQL("""
        WITH candidate_event AS (
          SELECT
            e.device_id,
            MAX(e.timestamp) AS last_ts
          FROM {int_relay_event} e
          JOIN public.power_plant p ON p.id = e.power_plant_id
          WHERE e.power_plant_id = %(plant_id)s
            AND ( %(is_superuser)s = true OR p.customer_id = %(customer_id)s )
          GROUP BY e.device_id
          ORDER BY MAX(e.timestamp) DESC
          LIMIT 1
        ),
        candidate_analog AS (
          SELECT
            a.device_id,
            MAX(a."timestamp") AS last_ts
          FROM {stg_relay_analog} a
          JOIN public.power_plant p ON p.id = a.power_plant_id
          WHERE a.power_plant_id = %(plant_id)s
            AND ( %(is_superuser)s = true OR p.customer_id = %(customer_id)s )
          GROUP BY a.device_id
          ORDER BY MAX(a."timestamp") DESC
          LIMIT 1
        )
        SELECT
          COALESCE(ce.device_id, ca.device_id) AS device_id,
          d.name AS device_name,
          COALESCE(ce.last_ts, ca.last_ts) AS last_ts
        FROM candidate_event ce
        FULL OUTER JOIN candidate_analog ca ON 1=1
        LEFT JOIN public.device d
          ON d.id = COALESCE(ce.device_id, ca.device_id)
        LIMIT 1;
    """).format(
        int_relay_event=q(RT_SCHEMA, "stg_relay_event"),
        stg_relay_analog=q(RT_SCHEMA, "stg_relay_analog")
    ), {
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
            NULL::numeric AS apparent_power_kva,
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
            d.code AS tracker_code,
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

    plant_id = params.get("plant_id")
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

    limit = max(1, min(500, limit))

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
        ORDER BY tc.power_plant_id, tc.context, tc.pathname
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


def datastudio_resolve_hist_aggregation(aggregation: str):
    agg = safe_lower(aggregation)

    if agg in ("sum", "soma"):
        return "sum_value", "sum"

    if agg in ("max", "maxima", "máxima"):
        return "max_value", "max"

    if agg in ("avg", "media", "média", "none", "raw", "sem_agregacao"):
        return "avg_value", (agg or "avg")

    fallback_from = agg or "none"
    return "avg_value", f"avg(fallback:{fallback_from})"


def fetch_datastudio_points_historico(cur, *, customer_id: int, power_plant_id: int,
                                     pathname: str, start_ts, end_ts,
                                     aggregation: str = None, limit: int = 5000):
    hist_table_name, resolved_route = datastudio_choose_hist_table(start_ts, end_ts)
    value_col, aggregation_resolved = datastudio_resolve_hist_aggregation(aggregation)

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
    # 5min consolidado -> usar hist_15min
    # decisão prática: mapear 5min para 15min consolidado
    # =========================================================
    if period_l in ("5min", "15min"):
        value_col = datastudio_consolidado_15min_value_column(pathname)

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
            tbl=q(ANALYTICS_SCHEMA, "mart_datastudio_hist_15min")
        ), {
            "customer_id": customer_id,
            "power_plant_id": power_plant_id,
            "pathname": pathname,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        })
        return cur.fetchall() or [], "consolidado_15min"

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

    # =========================================================
    # fallback legado
    # =========================================================
    rows = fetch_datastudio_points_from_timeseries(
        cur,
        customer_id=customer_id,
        power_plant_id=power_plant_id,
        pathname=pathname,
        start_ts=start_ts,
        end_ts=end_ts,
        source="consolidado",
        limit=limit,
    )
    return rows, "timeseries"


def handle_get_datastudio_series(cur, ctx: dict, params: dict):
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
                "truncated": False
            },
            "series": []
        })

    series_out = []
    selection_truncated = False
    route_hits = {}
    total_points = 0
    power_plant_id = selection.get("power_plant_id")
    start_ts = selection.get("start_ts")
    end_ts = selection.get("end_ts")
    historico_default = selection.get("historico_aggregation_default")
    consolidado_default = selection.get("consolidado_period_default")

    if power_plant_id is None:
        return http_response(400, {"error": "selection sem power_plant_id"})

    for item in items:
        pathname = item.get("pathname")
        if not pathname:
            continue

        source_effective = item.get("source_override") or item.get("source") or "historico"
        aggregation_effective = item.get("aggregation_override") or historico_default or "avg"
        period_effective = item.get("period_override") or consolidado_default or "daily"

        resolved_route = "timeseries"
        aggregation_resolved = aggregation_effective

        source_l = safe_lower(source_effective)
        if source_l == "historico":
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
        elif source_l == "consolidado":
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
            "points": points
        })

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    print(f"[/datastudio/series] selection_id={selection_id} series={len(series_out)} points={total_points} routes={json.dumps(route_hits, ensure_ascii=False)} elapsed_ms={elapsed_ms}")

    return http_response(200, {
        "selection_id": selection_id,
        "selection": selection,
        "meta": {
            "timezone": selection.get("timezone"),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "items_count": len(items),
            "series_count": len(series_out),
            "max_points_per_series": max_points_per_series,
            "truncated": selection_truncated
        },
        "series": series_out
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
              WHEN working_status_raw ~ '^-?\\d+(\\.\\d+)?$' THEN working_status_raw::numeric::int
              ELSE NULL
            END AS working_status,
            CASE
              WHEN state_operation_raw ~ '^-?\\d+(\\.\\d+)?$' THEN state_operation_raw::numeric::int
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

    if power_plant_id is None:
        return http_response(400, {"error": "selection sem power_plant_id"})

    route_hits = {}
    rows_written = 0

    for item in items:
        pathname = item.get("pathname")
        if not pathname:
            continue

        source_effective = item.get("source_override") or item.get("source") or "historico"
        aggregation_effective = item.get("aggregation_override") or historico_default or "avg"
        period_effective = item.get("period_override") or consolidado_default or "daily"

        resolved_route = "timeseries"
        aggregation_resolved = aggregation_effective

        source_l = safe_lower(source_effective)

        if source_l == "historico":
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
        elif source_l == "consolidado":
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
                row.get("ts"),
                v
            ])

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    print(f"[/datastudio/export] selection_id={selection_id} rows={rows_written} routes={json.dumps(route_hits, ensure_ascii=False)} elapsed_ms={elapsed_ms}")

    csv_content = output.getvalue()
    filename = f"datastudio_export_{selection_id}.csv"
    return file_response(200, csv_content, filename)


def lambda_handler(event, context):
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
                    is_active
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
                    "is_superuser": user["is_superuser"]
                }
            })
        finally:
            cur.close()
            end_request(conn)

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
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_relay_realtime(cur, int(plant_id), ctx)
        except Exception as e:
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
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_multimeter_realtime(cur, int(plant_id), ctx)
        except Exception as e:
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
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            return handle_get_trackers_realtime(cur, int(plant_id), ctx)
        except Exception as e:
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
                WITH pivot AS (
                  SELECT
                    r.power_plant_id,
                    r.device_id,
                    COALESCE(MAX(r.device_name), MAX(d.name)) AS inverter_name,
                    MAX(CASE WHEN r.point_name = 'active_power_kw' THEN r.point_value END) AS power_kw,
                    MAX(CASE WHEN r.point_name = 'frequency_hz' THEN r.point_value END) AS freq_hz,
                    MAX(CASE WHEN r.point_name = 'power_factor' THEN r.point_value END) AS power_factor,
                    MAX(CASE WHEN r.point_name = 'efficiency_pct' THEN r.point_value END) AS efficiency_pct,
                    MAX(CASE WHEN r.point_name = 'power_input_kw' THEN r.point_value END) AS power_input_kw,
                    MAX(CASE WHEN r.point_name = 'working_status' THEN r.point_value END) AS working_status,
                    MAX(CASE WHEN r.point_name = 'state_operation' THEN r.point_value END) AS state_operation,
                    MAX(CASE WHEN r.point_name = 'string_voltage_v' THEN r.point_value END) AS string_voltage_v,
                    MAX(CASE WHEN r.point_name = 'current_phase_a_a' THEN r.point_value END) AS current_phase_a_a,
                    MAX(CASE WHEN r.point_name = 'current_phase_b_a' THEN r.point_value END) AS current_phase_b_a,
                    MAX(CASE WHEN r.point_name = 'current_phase_c_a' THEN r.point_value END) AS current_phase_c_a,
                    MAX(CASE WHEN r.point_name = 'line_voltage_ab_v' THEN r.point_value END) AS line_voltage_ab_v,
                    MAX(CASE WHEN r.point_name = 'line_voltage_bc_v' THEN r.point_value END) AS line_voltage_bc_v,
                    MAX(CASE WHEN r.point_name = 'line_voltage_ca_v' THEN r.point_value END) AS line_voltage_ca_v,
                    MAX(CASE WHEN r.point_name = 'apparent_power_kva' THEN r.point_value END) AS apparent_power_kva,
                    MAX(CASE WHEN r.point_name = 'power_reactive_kvar' THEN r.point_value END) AS power_reactive_kvar,
                    MAX(CASE WHEN r.point_name = 'temperature_internal_c' THEN r.point_value END) AS temp_c,
                    MAX(CASE WHEN r.point_name = 'daily_active_energy_kwh' THEN r.point_value END) AS daily_active_energy_kwh,
                    MAX(CASE WHEN r.point_name = 'resistance_insulation_mohm' THEN r.point_value END) AS resistance_insulation_mohm,
                    MAX(CASE WHEN r.point_name = 'cumulative_active_energy_kwh' THEN r.point_value END) AS cumulative_active_energy_kwh,
                    MAX(CASE WHEN r.point_name = 'power_dc_kw' THEN r.point_value END) AS power_dc_kw,
                    MAX(d.cabin_id) AS cabin_id,
                    MAX(c.code) AS cabin_code,
                    MAX(c.name) AS cabin_name,
                    MAX(c.display_order) AS cabin_display_order,
                    MAX(r."timestamp") AS last_reading_ts
                  FROM {tbl} r
                  LEFT JOIN public.device d ON d.id = r.device_id
                  LEFT JOIN public.cabin c ON c.id = d.cabin_id
                  WHERE r.power_plant_id = %(plant_id)s
                    AND LOWER(COALESCE(r.device_type_name, '')) = 'inverter'
                    AND LOWER(COALESCE(r.reading_source, '')) = 'inverter'
                  GROUP BY r.power_plant_id, r.device_id
                )
                SELECT
                  p.customer_id,
                  pv.power_plant_id,
                  p.name AS power_plant_name,
                  pv.device_id,
                  pv.inverter_name,
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
                  pv.cabin_id,
                  pv.cabin_code,
                  pv.cabin_name,
                  pv.cabin_display_order,
                  pv.working_status,
                  NULL::text AS inverter_status,
                  pv.state_operation,
                  NULL::int AS communication_fault_code,
                  true AS is_communication_ok,
                  COALESCE(pv.state_operation, pv.working_status) AS status_code_raw,
                  CASE
                    WHEN pv.state_operation = 2 THEN 'RUNNING'
                    WHEN pv.state_operation = 3 THEN 'OFF'
                    WHEN pv.working_status = 1 THEN 'RUNNING'
                    WHEN pv.working_status = 8 THEN 'OFF'
                    ELSE 'UNKNOWN'
                  END AS status
                FROM pivot pv
                JOIN public.power_plant p ON p.id = pv.power_plant_id
                WHERE (%(is_superuser)s = true OR p.customer_id = %(customer_id)s)
                ORDER BY pv.device_id;
            """).format(
                tbl=q(RT_SCHEMA, "mart_latest_reading")
            ), {
                "customer_id": int(effective_customer_id),
                "is_superuser": ctx["is_superuser"],
                "plant_id": int(plant_id),
            })

            rows = cur.fetchall() or []

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

            items = []
            for r in rows:
                device_id = r.get("device_id")
                inverter_id = int(device_id) if device_id is not None else None

                status_code = inum(r, "status_code_raw")
                if status_code is None:
                    status_code = 4

                item = {
                    "inverter_id": inverter_id,
                    "inverter_name": r.get("inverter_name"),
                    "power_kw": fnum(r, "power_kw") or 0.0,
                    "efficiency_pct": fnum(r, "efficiency_pct") or 0.0,
                    "temp_c": fnum(r, "temp_c"),
                    "freq_hz": fnum(r, "freq_hz"),
                    "pr": fnum(r, "pr"),
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
                    "cabin_id": int(r["cabin_id"]) if r.get("cabin_id") is not None else None,
                    "cabin_code": r.get("cabin_code"),
                    "cabin_name": r.get("cabin_name"),
                    "cabin_display_order": int(r["cabin_display_order"]) if r.get("cabin_display_order") is not None else None,

                    "working_status": inum(r, "working_status"),
                    "inverter_status": r.get("inverter_status"),
                    "state_operation": inum(r, "state_operation"),
                    "communication_fault_code": r.get("communication_fault_code"),
                    "is_communication_ok": r.get("is_communication_ok"),
                }

                items.append(item)

            return http_response(200, {
                "power_plant_id": int(plant_id),
                "customer_id": int(effective_customer_id),
                "items": items,
                "meta": {
                    "source": f"{RT_SCHEMA}.mart_latest_reading"
                }
            })

        except Exception as e:
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

            cur.execute(sql.SQL("""
                WITH all_strings AS (
                  SELECT generate_series(1, 30) AS string_index
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
                "max_strings": 30,
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
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SET LOCAL statement_timeout = '12000ms';")

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
                    p.power_plant_name,
                    p.rated_power_kwp AS rated_power_kw,
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
                LEFT JOIN weather w ON w.power_plant_id = p.power_plant_id
                LEFT JOIN alarms a ON a.power_plant_id = p.power_plant_id
                WHERE p.power_plant_id = %(plant_id)s
                  AND (
                    %(is_superuser)s = true
                    OR p.customer_id = %(customer_id)s
                  )
            """).format(
                mart_portfolio_overview=q(RT_SCHEMA, "mart_portfolio_overview"),
                stg_weather_station_analog=q(RT_SCHEMA, "stg_weather_station_analog"),
                int_events_alarms=q(RT_SCHEMA, "int_events_alarms")
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
            print("[/realtime] ERROR:", repr(e))
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
    # GET /plants
    # ========================================================
    if method == "GET" and is_path(path, "/plants"):
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql.SQL("""
                SELECT
                    customer_id,
                    customer_name,
                    customer_rated_power_kwp,
                    power_plant_id,
                    power_plant_name,
                    rated_power_kwp,
                    rated_power_ac_kw,
                    active_power_inverter_kw,
                    active_power_meter_kw,
                    active_power_kw,
                    daily_energy_kwh,
                    generation_liquid_meter_kwh,
                    generation_accumulated_kwh,
                    irradiance_wm2,
                    irradiation_accumulated_kwh_m2,
                    pr_daily_pct,
                    pr_accumulated_pct,
                    capacity_factor_daily_pct,
                    capacity_factor_pct,
                    inverter_availability_pct,
                    relay_availability_pct,
                    plant_status_color,
                    red_alarm_count,
                    yellow_alarm_count,
                    active_alarm_count,
                    updated_at
                FROM {mart_portfolio_overview} p
                WHERE (
                    %(is_superuser)s = true
                    OR p.customer_id = %(customer_id)s
                )
                ORDER BY p.power_plant_name
            """).format(
                mart_portfolio_overview=q(RT_SCHEMA, "mart_portfolio_overview")
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
                    "power_plant_name": r.get("power_plant_name"),

                    "rated_power_kw": float(r["rated_power_kwp"]) if r.get("rated_power_kwp") is not None else None,
                    "rated_power_kwp": float(r["rated_power_kwp"]) if r.get("rated_power_kwp") is not None else None,
                    "rated_power_ac_kw": float(r["rated_power_ac_kw"]) if r.get("rated_power_ac_kw") is not None else None,

                    "active_power_inverter_kw": float(r["active_power_inverter_kw"]) if r.get("active_power_inverter_kw") is not None else None,
                    "active_power_meter_kw": float(r["active_power_meter_kw"]) if r.get("active_power_meter_kw") is not None else None,
                    "active_power_kw": float(r["active_power_inverter_kw"]) if r.get("active_power_inverter_kw") is not None else 0.0,
                    "active_power_total_kw": float(r["active_power_kw"]) if r.get("active_power_kw") is not None else None,

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

        if action not in ("on", "off", "reset"):
            return http_response(400, {"error": "action inválida. Use: on, off ou reset"})
        if not username or not password:
            return http_response(400, {"error": "username e password são obrigatórios"})
        if not str(device_id_fallback).isdigit():
            return http_response(400, {"error": "device_id inválido"})

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

            user = validate_operational_user(cur, username, password, ctx)
            if not user:
                return http_response(401, {"error": "credenciais operacionais inválidas"})

            target = resolve_device_command_target(cur, int(plant_id), int(device_id_fallback), ctx)
            if not target:
                return http_response(404, {"error": "device não encontrado para esta usina"})

            customer_id = resolve_customer_id_for_plant(cur, int(plant_id), ctx)
            if customer_id is None:
                return http_response(400, {"error": "não foi possível resolver customer_id"})

            device_index = infer_device_index(target.get("device_name"), default_index=1)
            mqtt_topic = build_device_command_topic(
                power_plant_name=target.get("power_plant_name") or f"plant{plant_id}",
                device_type=target.get("device_type") or "device",
                device_index=device_index,
            )
            payload = build_device_command_payload(action=action, target=target, requested_by=requested_by)

            command_id = insert_device_command_audit(
                cur,
                customer_id=customer_id,
                target=target,
                action=action,
                mqtt_topic=mqtt_topic,
                command_payload=payload,
                requested_by=requested_by,
                requested_username=username,
            )
            commit_quiet(conn)

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
                return http_response(200, {
                    "ok": True,
                    "command_id": command_id,
                    "status": "SUCCESS",
                    "mqtt_topic": mqtt_topic,
                    "payload": payload,
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
                    "error": "falha ao publicar comando",
                })
        except Exception as e:
            rollback_quiet(conn)
            print("[/plants/{id}/devices/{id}/command] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
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
                  d.name AS device_name,
                  dt.name AS device_type
                FROM public.device d
                JOIN public.device_type dt ON dt.id = d.device_type_id
                WHERE d.power_plant_id = %(plant_id)s
                  AND d.is_active = true
                ORDER BY dt.name, d.name, d.id;
            """, {"plant_id": int(plant_id)})
            rows = cur.fetchall() or []

            items = [{
                "device_id": int(r["device_id"]),
                "device_name": r.get("device_name"),
                "device_type": r.get("device_type"),
                "label": " • ".join([x for x in [r.get("device_type"), r.get("device_name")] if x]) or f"Device {r['device_id']}",
            } for r in rows]

            return http_response(200, {
                "items": items,
                "meta": {
                    "source": "public.device/public.device_type"
                }
            })
        except Exception as e:
            print("[/plants/{id}/devices/options] ERROR:", repr(e))
            print(traceback.format_exc())
            return http_response(500, {"error": "Internal Server Error", "hint": "check CloudWatch logs"})
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
            if not ensure_plant_access(cur, int(plant_id), ctx):
                return http_response(403, {"error": "sem permissão para esta usina"})

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

            cur.execute(sql.SQL("""
                WITH all_strings AS (
                  SELECT generate_series(1, 30) AS string_index
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
            })

            rows = cur.fetchall() or []
            return http_response(200, {
                "inverter_id": int(inverter_id),
                "max_strings": 30,
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

    return http_response(404, {"error": "rota não encontrada", "path": path, "method": method})
