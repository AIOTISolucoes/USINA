import json
import re
import psycopg2
from datetime import datetime


# ============================================================
# CONFIG BANCO
# ============================================================

DB_CONFIG = {
    "host": "172.31.70.48",
    "port": 5432,
    "dbname": "scada_ingestion_v2",
    "user": "iot_user",
    "password": "Maxwell1617!",
    "connect_timeout": 5
}


# ============================================================
# NORMALIZA EVENTO
# ============================================================

def normalize_event(event):
    print("EVENT_RAW:", event)

    if isinstance(event, dict):
        return event

    if isinstance(event, str):
        return json.loads(event)

    raise Exception("Formato de evento inválido")


# ============================================================
# EXTRAI USINA, TIPO E ÍNDICE DO TOPIC
# Exemplo:
# dev/read/UFV/Acopiara/weather/Weather01
# dev/read/UFV/Massape1/multimeter/1
# dev/read/UFV/Massape1/relay/1
# ============================================================

def parse_identity_from_topic(topic: str):
    if not topic:
        return None, None, None

    parts = str(topic).split("/")
    if len(parts) < 6:
        return None, None, None

    power_plant = parts[3]
    device_type = parts[4]
    device_index = parts[5]

    return power_plant, device_type, device_index


# ============================================================
# TIMESTAMP
# ============================================================

def parse_timestamp(payload):
    return (
        payload.get("timestamp")
        or payload.get("date_time")
        or datetime.utcnow().isoformat()
    )


# ============================================================
# RESOLVE TABELA RAW
# ============================================================

def resolve_raw_table(device_type):
    dt = str(device_type or "").strip().lower()

    if dt == "inverter":
        return "public.raw_inverter"

    if dt == "relay":
        return "public.raw_relay"

    if dt in ("meter", "multimeter", "medidor", "multimedidor"):
        return "public.raw_meter"

    if dt in ("weather", "weather_station"):
        return "public.raw_weather_station"

    if dt in ("tracker", "tcu"):
        return "public.raw_tracker"

    if dt == "rsu":
        return "public.raw_rsu"

    return None


# ============================================================
# NORMALIZA PAYLOAD WEATHER STATION
# ============================================================

def normalize_weather_payload(payload: dict) -> dict:
    normalized = dict(payload)

    mapping = {
        "SR05_GHI_irradiance": "irradiance_ghi",
        "SR05_POA_irradiance": "irradiance_poa",
        "SSTRH_temperatura": "air_temperature",
        "pt1000_painel1": "module_temperature",
        "vel_vento": "wind_speed",
        "acumulador_pluv_hour": "hourly_accumulated_rain",
        "acumulador_pluv_day": "accumulated_rain",
    }

    for src, dst in mapping.items():
        if src in payload and dst not in normalized:
            normalized[dst] = payload[src]

    normalized["asset_type"] = "weather_station"
    return normalized


# ============================================================
# NORMALIZA PAYLOAD MULTIMEDIDOR / METER
# ============================================================

def normalize_meter_payload(payload: dict) -> dict:
    normalized = dict(payload)

    mapping = {
        "volt_uab_line": "voltage_ab_v",
        "volt_ubc_line": "voltage_bc_v",
        "volt_uca_line": "voltage_ca_v",

        "current_a_phase_a": "current_a_a",
        "current_b_phase_b": "current_b_a",
        "current_c_phase_c": "current_c_a",

        "active_power": "active_power_kw",
        "react_power": "reactive_power_kvar",
        "frequency": "frequency_hz",
        "power_factor": "power_factor",

        "energy_imp": "energy_import_kwh",
        "energy_exp": "energy_export_kwh",
    }

    for src, dst in mapping.items():
        if src in payload and dst not in normalized:
            normalized[dst] = payload[src]

    normalized["asset_type"] = "meter"
    return normalized


# ============================================================
# NORMALIZA PAYLOAD RELAY
# Mantém as chaves originais e adiciona aliases úteis.
# ============================================================

def normalize_relay_payload(payload: dict) -> dict:
    normalized = dict(payload)

    mapping = {
        "active_power": "active_power_kw",
        "apparent_power": "apparent_power_kva",
        "reactive_power": "reactive_power_kvar",
        "voltage_ab": "voltage_ab_v",
        "voltage_bc": "voltage_bc_v",
        "voltage_ca": "voltage_ca_v",
        "current_a": "current_a_a",
        "current_b": "current_b_a",
        "current_c": "current_c_a",
    }

    for src, dst in mapping.items():
        if src in payload and dst not in normalized:
            normalized[dst] = payload[src]

    normalized["asset_type"] = "relay"
    return normalized


# ============================================================
# VALIDAÇÕES DE PAYLOAD REAL
# ============================================================

def looks_like_real_relay_payload(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False

    expected_keys = {
        "communication_fault",
        "active_power",
        "apparent_power",
        "reactive_power",
        "voltage_ab",
        "voltage_bc",
        "voltage_ca",
        "current_a",
        "current_b",
        "current_c",
        "status_relay",
        "flag_27",
        "flag_46",
        "flag_47",
        "flag_50",
        "flag_50N",
        "flag_51N",
        "flag_51GS",
        "flag_59",
        "flag_81_O",
        "flag_81_U",
    }

    return len(expected_keys.intersection(set(payload.keys()))) > 0


def looks_like_envelope_only(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False

    keys = set(payload.keys())
    envelope_keys = {"mqtt_topic", "received_at", "topic"}

    return keys.issubset(envelope_keys)


# ============================================================
# POWER PLANT
# ============================================================

def resolve_power_plant(cur, name):
    cur.execute(
        """
        SELECT id
        FROM public.power_plant
        WHERE name = %s
        LIMIT 1
        """,
        (name,)
    )
    row = cur.fetchone()
    return row[0] if row else None


# ============================================================
# DEVICE — resolve por nome exato
# ============================================================

def resolve_device(cur, device_name, device_type, power_plant_id):
    cur.execute("""
        SELECT d.id
        FROM public.device d
        JOIN public.device_type dt
          ON dt.id = d.device_type_id
        WHERE d.name = %s
          AND d.power_plant_id = %s
          AND lower(dt.name) = lower(%s)
          AND d.is_active = true
        LIMIT 1
    """, (device_name, power_plant_id, device_type))

    row = cur.fetchone()
    return row[0] if row else None


# ============================================================
# DEVICE — resolve por índice do tópico
# Ex.: relay/1 -> tenta casar com nome terminado em 1
# ============================================================

def resolve_device_by_topic_index(cur, power_plant_id, device_type, device_index):
    cur.execute("""
        SELECT
          d.id,
          d.name
        FROM public.device d
        JOIN public.device_type dt
          ON dt.id = d.device_type_id
        WHERE d.power_plant_id = %s
          AND lower(dt.name) = lower(%s)
          AND d.is_active = true
        ORDER BY d.id
    """, (power_plant_id, device_type))

    rows = cur.fetchall() or []
    wanted = str(device_index or "").strip()

    if not wanted:
        return None

    for row in rows:
        device_id, device_name = row
        m = re.search(r"(\d+)\s*$", str(device_name or ""))
        if m and m.group(1) == wanted:
            return device_id

    return None


# ============================================================
# HELPERS DE RESOLUÇÃO
# ============================================================

def build_fallback_device_name_from_topic(device_type_topic: str, device_index: str):
    if not device_type_topic or not device_index:
        return None
    return f"{device_type_topic}_{device_index}"


def should_prefer_topic_index_resolution(internal_device_type: str) -> bool:
    return internal_device_type in ("relay", "meter", "rsu", "tracker", "inverter")


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    conn = None
    cur = None

    try:
        evt = normalize_event(event)

        payload = evt.get("payload") if isinstance(evt, dict) and "payload" in evt else evt

        if not isinstance(payload, dict):
            raise Exception("Payload inválido: esperado dict/json")

        topic = (
            evt.get("topic")
            or evt.get("mqtt_topic")
            or payload.get("topic")
            or payload.get("mqtt_topic")
        )

        if not topic:
            print("IGNORED: missing mqtt topic", payload)
            return {"status": "ignored", "reason": "missing mqtt topic"}

        power_plant, device_type_topic, device_index = parse_identity_from_topic(topic)
        if not power_plant or not device_type_topic:
            print("IGNORED: invalid topic", topic)
            return {"status": "ignored", "reason": "invalid topic"}

        dt_lower = str(device_type_topic or "").strip().lower()

        internal_device_type = (
            "weather_station" if dt_lower in ("weather", "weather_station")
            else "tracker" if dt_lower in ("tcu", "tracker")
            else "rsu" if dt_lower == "rsu"
            else "meter" if dt_lower in ("meter", "multimeter", "medidor", "multimedidor")
            else dt_lower
        )

        table = resolve_raw_table(dt_lower)
        if not table:
            print("IGNORED: unsupported device type", {
                "topic": topic,
                "device_type_topic": device_type_topic,
                "internal_device_type": internal_device_type
            })
            return {"status": "ignored", "reason": "unsupported device type"}

        # ----------------------------------------------------
        # normalizações por tipo
        # ----------------------------------------------------
        if internal_device_type == "weather_station":
            payload = normalize_weather_payload(payload)

        if internal_device_type == "meter":
            payload = normalize_meter_payload(payload)

        if internal_device_type == "relay":
            payload = normalize_relay_payload(payload)

        # ----------------------------------------------------
        # validação extra de relay
        # não inserir envelope inútil no raw_relay
        # ----------------------------------------------------
        if internal_device_type == "relay":
            if looks_like_envelope_only(payload):
                print("IGNORED: relay payload é só envelope", {
                    "topic": topic,
                    "payload": payload
                })
                return {"status": "ignored", "reason": "relay envelope only"}

            if not looks_like_real_relay_payload(payload):
                print("IGNORED: relay payload sem dados reais", {
                    "topic": topic,
                    "payload": payload
                })
                return {"status": "ignored", "reason": "relay payload without real data"}

        ts = parse_timestamp(payload)

        print("TOPIC_RESOLVED:", topic)
        print("POWER_PLANT_NAME_FROM_TOPIC:", power_plant)
        print("DEVICE_TYPE_TOPIC:", device_type_topic)
        print("DEVICE_INDEX_FROM_TOPIC:", device_index)
        print("INTERNAL_DEVICE_TYPE:", internal_device_type)
        print("RAW_TABLE:", table)
        print("TIMESTAMP:", ts)

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # ----------------------------------------------------
        # USINA: só aceita se já existir
        # ----------------------------------------------------
        power_plant_id = resolve_power_plant(cur, power_plant)
        if not power_plant_id:
            print("IGNORED: power plant not registered", {
                "topic": topic,
                "power_plant_name": power_plant,
                "device_type": internal_device_type
            })
            conn.rollback()
            return {
                "status": "ignored",
                "reason": "power plant not registered",
                "power_plant_name": power_plant,
                "device_type_topic": device_type_topic,
                "device_type": internal_device_type
            }

        # ----------------------------------------------------
        # DEVICE: resolver preferencialmente por índice do tópico
        # ----------------------------------------------------
        device_id = None
        resolved_by = None
        device_name_for_log = None

        if should_prefer_topic_index_resolution(internal_device_type):
            device_id = resolve_device_by_topic_index(
                cur,
                power_plant_id=power_plant_id,
                device_type=internal_device_type,
                device_index=device_index
            )
            if device_id:
                resolved_by = "topic_index"

        # fallback por nome do payload
        if not device_id:
            candidate_device_name = (
                payload.get("asset")
                or payload.get("device_type")
            )

            if candidate_device_name:
                device_id = resolve_device(
                    cur,
                    candidate_device_name,
                    internal_device_type,
                    power_plant_id
                )
                if device_id:
                    resolved_by = "payload_name"
                    device_name_for_log = candidate_device_name

        # fallback final por nome montado do tópico
        if not device_id:
            fallback_device_name = build_fallback_device_name_from_topic(device_type_topic, device_index)
            if fallback_device_name:
                device_id = resolve_device(
                    cur,
                    fallback_device_name,
                    internal_device_type,
                    power_plant_id
                )
                if device_id:
                    resolved_by = "topic_fallback_name"
                    device_name_for_log = fallback_device_name

        if not device_id:
            print("IGNORED: device not registered", {
                "topic": topic,
                "power_plant_id": power_plant_id,
                "power_plant_name": power_plant,
                "device_type": internal_device_type,
                "device_index": device_index,
                "payload_asset": payload.get("asset"),
                "payload_device_type": payload.get("device_type")
            })
            conn.rollback()
            return {
                "status": "ignored",
                "reason": "device not registered",
                "power_plant_id": power_plant_id,
                "power_plant_name": power_plant,
                "device_type_topic": device_type_topic,
                "device_type": internal_device_type,
                "device_index": device_index,
                "payload_asset": payload.get("asset"),
                "payload_device_type": payload.get("device_type")
            }

        print("DEVICE_RESOLVED:", {
            "device_id": device_id,
            "resolved_by": resolved_by,
            "device_name_for_log": device_name_for_log,
            "device_index": device_index
        })

        print("INSERT_PREPARE:", {
            "table": table,
            "power_plant_id": power_plant_id,
            "device_id": device_id,
            "device_type": internal_device_type,
            "resolved_by": resolved_by
        })

        cur.execute(f"""
            INSERT INTO {table} (
                timestamp,
                power_plant_id,
                device_id,
                json_data
            )
            VALUES (%s, %s, %s, %s::jsonb)
        """, (
            ts,
            power_plant_id,
            device_id,
            json.dumps(payload, ensure_ascii=False)
        ))

        conn.commit()

        print("INSERT_OK:", table, power_plant_id, device_id)

        return {
            "status": "inserted",
            "table": table,
            "power_plant_id": power_plant_id,
            "device_id": device_id,
            "device_type_topic": device_type_topic,
            "device_type": internal_device_type,
            "device_index": device_index,
            "resolved_by": resolved_by
        }

    except Exception as e:
        if conn:
            conn.rollback()
        print("LAMBDA_ERROR:", str(e))
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()