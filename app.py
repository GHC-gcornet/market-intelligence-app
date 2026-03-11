#!/usr/bin/env python3
"""Executive audit app: one-question flow, segmented benchmark, admin console."""

from __future__ import annotations

import csv
import cgi
import base64
import copy
import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parent
IS_VERCEL = bool(os.getenv("VERCEL"))
DB_PATH = Path("/tmp/results.db") if IS_VERCEL else (BASE_DIR / "results.db")
CONFIG_PATH = BASE_DIR / "barometer_config.json"
SEGMENT_DATA_PATH = BASE_DIR / "segment_benchmarks.csv"
STYLE_PATH = BASE_DIR / "static" / "style.css"
MANIFEST_PATH = BASE_DIR / "static" / "manifest.webmanifest"
SW_PATH = BASE_DIR / "static" / "sw.js"

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "cambia-esta-clave")
SESSION_SECRET = os.getenv("ADMIN_SESSION_SECRET", "secreto-cambiar")
SESSION_TTL_SECONDS = 8 * 60 * 60
MAX_ADMIN_ROWS = 500

REPORT_WEBHOOK_URL = os.getenv("REPORT_WEBHOOK_URL", "").strip()
REPORT_WEBHOOK_TOKEN = os.getenv("REPORT_WEBHOOK_TOKEN", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_API_BASE = os.getenv("GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta").strip()
GEMINI_TRANSCRIBE_MODEL = os.getenv("GEMINI_TRANSCRIBE_MODEL", "gemini-2.0-flash").strip()
GEMINI_EXTRACT_MODEL = os.getenv("GEMINI_EXTRACT_MODEL", "gemini-2.0-flash").strip()

ADMIN_SESSIONS: dict[str, dict[str, str | float] | float] = {}
BOOTSTRAPPED = False

# Minimal credential store for admin login.
# The requested user uses the same password as ADMIN_PASSWORD unless overridden.
ADMIN_USERS = {
    "gcornet@globalhumancon.com": os.getenv("GCORNET_PASSWORD", ADMIN_PASSWORD),
}

INSIDE_SCOPE_AREA_LEVEL_OPTIONS = [
    "C-Level",
    "Operaciones/Logística",
    "Ventas/Marketing",
    "Finanzas",
    "RRHH",
    "IT/Tech",
    "Otro",
]

INSIDE_SCOPE_EXIT_REASON_OPTIONS = [
    "Liderazgo tóxico",
    "Tope salarial",
    "Proyecto sin rumbo",
    "Reestructuración",
    "Falta de flexibilidad",
    "Otro",
]

INSIDE_SCOPE_SYSTEM_PROMPT = """
Eres un analista experto en Market Intelligence para una firma de Executive Search.
Tu objetivo es leer la transcripcion de un consultor y extraer los siguientes datos en formato JSON estricto.
Si un dato no se menciona explicitamente, devuelve el valor null o una cadena vacia. No inventes informacion.

Claves del JSON esperado:
- empresa: Nombre de la empresa mencionada (String).
- area_nivel: Categoriza en uno de estos valores exactos:
  [C-Level, Operaciones/Logística, Ventas/Marketing, Finanzas, RRHH, IT/Tech, Otro] (String).
- rango_salarial: Extrae la cifra o rango mencionado (String).
- motivo_salida: Categoriza en uno de estos valores exactos:
  [Liderazgo tóxico, Tope salarial, Proyecto sin rumbo, Reestructuración, Falta de flexibilidad, Otro] (String).
- insight: Escribe un resumen ejecutivo de maximo 2 lineas con el dato estrategico mas relevante aportado por el consultor (String).
""".strip()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def esc(value: str) -> str:
    return html.escape(value, quote=True)


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if "questions" not in data or not data["questions"]:
        raise RuntimeError("barometer_config.json debe incluir questions")

    return data


CONFIG = load_config()
QUESTIONS = CONFIG["questions"]
QUESTION_BY_ID = {q["id"]: q for q in QUESTIONS}
COMPARISON_QUESTION_IDS = CONFIG["comparison_question_ids"]
SEGMENT_KEYS = ["facturacion_2024", "tamano_empresa", "sector"]
SEGMENT_FIELD_KEYS = ("sector", "tamano_empresa", "facturacion_2024")
CAMPAIGN_QUESTION_TYPES = {"text", "single", "multiple", "scale"}
DEFAULT_BAROMETER_SLUG = "barometro-2026"
DEFAULT_FLASH_AUDIT_SLUG = "flash-audit-general"


def load_segment_rows() -> list[dict]:
    if not SEGMENT_DATA_PATH.exists():
        return []

    with SEGMENT_DATA_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]

    for row in rows:
        try:
            row["sample_size"] = int(row.get("sample_size", "1") or "1")
        except ValueError:
            row["sample_size"] = 1

    return rows


SEGMENT_ROWS = load_segment_rows()


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_responses (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                email TEXT NOT NULL,
                facturacion_2024 TEXT NOT NULL,
                tamano_empresa TEXT NOT NULL,
                sector TEXT NOT NULL,
                answers_json TEXT NOT NULL,
                benchmark_json TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                report_html TEXT NOT NULL,
                overall_score REAL NOT NULL,
                overall_benchmark REAL NOT NULL,
                red_alerts INTEGER NOT NULL,
                yellow_alerts INTEGER NOT NULL,
                delivery_status TEXT NOT NULL,
                delivery_error TEXT,
                source_ip TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inside_scope_logs (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                consultant_email TEXT NOT NULL,
                empresa TEXT,
                area_nivel TEXT,
                rango_salarial TEXT,
                motivo_salida TEXT,
                insight TEXT,
                transcription_raw TEXT,
                source_mode TEXT NOT NULL,
                source_ip TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_barometer (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                year INTEGER NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                public_url TEXT NOT NULL,
                description TEXT,
                questions_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data_barometer (
                id TEXT PRIMARY KEY,
                barometer_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sector TEXT,
                tamano_empresa TEXT,
                facturacion_2024 TEXT,
                answers_json TEXT NOT NULL,
                sample_weight INTEGER NOT NULL DEFAULT 1,
                source_ip TEXT,
                FOREIGN KEY (barometer_id) REFERENCES campaign_barometer(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_flash_audit (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                public_url TEXT NOT NULL,
                barometer_id TEXT NOT NULL,
                questions_json TEXT NOT NULL,
                mapping_json TEXT NOT NULL,
                cta_label TEXT NOT NULL,
                cta_url TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (barometer_id) REFERENCES campaign_barometer(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads_flash_audit (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                flash_audit_id TEXT NOT NULL,
                barometer_id TEXT NOT NULL,
                lead_name TEXT,
                lead_email TEXT NOT NULL,
                lead_company TEXT,
                sector TEXT,
                tamano_empresa TEXT,
                facturacion_2024 TEXT,
                answers_json TEXT NOT NULL,
                benchmark_json TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                scoring_json TEXT NOT NULL,
                overall_score REAL NOT NULL,
                red_alerts INTEGER NOT NULL,
                yellow_alerts INTEGER NOT NULL,
                result_url TEXT NOT NULL,
                source_ip TEXT,
                FOREIGN KEY (flash_audit_id) REFERENCES campaign_flash_audit(id),
                FOREIGN KEY (barometer_id) REFERENCES campaign_barometer(id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_data_barometer_segment
            ON data_barometer (barometer_id, sector, tamano_empresa, facturacion_2024)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_flash_audit_created
            ON leads_flash_audit (flash_audit_id, created_at DESC)
            """
        )


def ensure_bootstrap() -> None:
    global BOOTSTRAPPED
    if BOOTSTRAPPED:
        return
    init_db()
    seed_default_campaigns()
    BOOTSTRAPPED = True


def option_index(question: dict, option: str) -> int:
    try:
        return question["options"].index(option) + 1
    except ValueError as exc:
        raise ValueError(f"Opcion invalida para {question['id']}: {option}") from exc


def cardinal_score(question: dict, option: str) -> float:
    idx = option_index(question, option)
    count = len(question["options"])
    if count <= 1:
        return 100.0

    direction = question.get("direction", "higher_better")
    if direction == "lower_better":
        return round((count - idx) / (count - 1) * 100, 1)

    return round((idx - 1) / (count - 1) * 100, 1)


def segment_match_score(row: dict, segment: dict) -> int:
    score = 0
    for key in SEGMENT_KEYS:
        row_val = (row.get(key) or "").strip()
        seg_val = (segment.get(key) or "").strip()

        if row_val == seg_val:
            score += 3
        elif row_val == "*":
            score += 0
        else:
            return -1

    return score


def weighted_mode(values: list[tuple[str, int]]) -> str:
    counter: dict[str, int] = {}
    for value, weight in values:
        counter[value] = counter.get(value, 0) + max(weight, 1)

    best_value = ""
    best_weight = -1
    for value, weight in counter.items():
        if weight > best_weight:
            best_value = value
            best_weight = weight

    return best_value


def pick_segment_rows(segment: dict) -> tuple[list[dict], str]:
    if not SEGMENT_ROWS:
        return [], "sin_base_maestra"

    scored = [(row, segment_match_score(row, segment)) for row in SEGMENT_ROWS]
    valid = [(row, score) for row, score in scored if score >= 0]
    if not valid:
        return SEGMENT_ROWS, "fallback_global"

    best_score = max(score for _, score in valid)
    selected = [row for row, score in valid if score == best_score]

    if best_score >= 9:
        return selected, "match_exacto"
    if best_score >= 6:
        return selected, "match_parcial"
    return selected, "fallback_global"


def build_segment_benchmark(segment: dict) -> tuple[dict[str, str], dict]:
    selected_rows, match_type = pick_segment_rows(segment)

    benchmark_answers: dict[str, str] = {}
    for qid in COMPARISON_QUESTION_IDS:
        question = QUESTION_BY_ID[qid]
        weighted = []
        for row in selected_rows:
            value = (row.get(qid) or "").strip()
            if value and value in question["options"]:
                weighted.append((value, int(row.get("sample_size", 1))))

        if weighted:
            benchmark_answers[qid] = weighted_mode(weighted)
        else:
            benchmark_answers[qid] = question["options"][len(question["options"]) // 2]

    sample_size = sum(int(r.get("sample_size", 1)) for r in selected_rows) if selected_rows else 0
    meta = {
        "match_type": match_type,
        "sample_size": sample_size,
        "selected_rows": len(selected_rows),
    }
    return benchmark_answers, meta


def gap_status(question: dict, user_option: str, benchmark_option: str) -> tuple[int, str]:
    user_idx = option_index(question, user_option)
    benchmark_idx = option_index(question, benchmark_option)
    direction = question.get("direction", "higher_better")
    sign = 1 if direction == "lower_better" else -1
    gap = (user_idx - benchmark_idx) * sign

    if gap >= 2:
        return gap, "red"
    if gap == 1:
        return gap, "yellow"
    if gap <= -2:
        return gap, "green"
    return gap, "neutral"


def ndt_numeric(option: str) -> float:
    table = {
        "<= 2 dias": 2.0,
        "3-7 dias": 5.0,
        "8-14 dias": 11.0,
        "15-30 dias": 22.5,
        ">30 dias": 35.0,
    }
    return table.get(option, 11.0)


def analyze_answers(answers: dict[str, str], benchmark: dict[str, str], segment_meta: dict) -> dict:
    findings = []
    red_alerts = 0
    yellow_alerts = 0
    score_values = []
    benchmark_scores = []

    for qid in COMPARISON_QUESTION_IDS:
        question = QUESTION_BY_ID[qid]
        user_value = answers[qid]
        bench_value = benchmark[qid]
        gap, status = gap_status(question, user_value, bench_value)

        user_score = cardinal_score(question, user_value)
        bench_score = cardinal_score(question, bench_value)
        score_values.append(user_score)
        benchmark_scores.append(bench_score)

        if status == "red":
            red_alerts += 1
        if status == "yellow":
            yellow_alerts += 1

        if status == "red":
            message = (
                f"Atencion: {question['title']} esta claramente por debajo del segmento "
                f"({user_value} vs {bench_value})."
            )
        elif status == "yellow":
            message = (
                f"Aviso: {question['title']} va por debajo de la referencia "
                f"({user_value} vs {bench_value})."
            )
        elif status == "green":
            message = (
                f"Fortaleza: {question['title']} supera la media del segmento "
                f"({user_value} vs {bench_value})."
            )
        else:
            message = f"En linea con la media del segmento ({user_value})."

        findings.append(
            {
                "qid": qid,
                "question": question["title"],
                "user": user_value,
                "benchmark": bench_value,
                "gap": gap,
                "status": status,
                "message": message,
            }
        )

    overall_score = round(sum(score_values) / len(score_values), 1) if score_values else 0.0
    overall_benchmark = (
        round(sum(benchmark_scores) / len(benchmark_scores), 1) if benchmark_scores else 0.0
    )

    ndt_user = answers.get("q4")
    ndt_bench = benchmark.get("q4")
    ndt_slowdown_pct = 0.0
    if ndt_user and ndt_bench:
        b = ndt_numeric(ndt_bench)
        if b > 0:
            ndt_slowdown_pct = round((ndt_numeric(ndt_user) - b) / b * 100, 1)

    priority_findings = [f for f in findings if f["status"] in {"red", "yellow"}][:3]

    return {
        "overall_score": overall_score,
        "overall_benchmark": overall_benchmark,
        "overall_delta": round(overall_score - overall_benchmark, 1),
        "red_alerts": red_alerts,
        "yellow_alerts": yellow_alerts,
        "findings": findings,
        "priority_findings": priority_findings,
        "segment_meta": segment_meta,
        "ndt_slowdown_pct": ndt_slowdown_pct,
    }


def color_for_status(status: str) -> str:
    if status == "red":
        return "#CC3A2D"
    if status == "yellow":
        return "#B89B5B"
    if status == "green":
        return "#1F7A4F"
    return "#5F6D7A"


def build_report_html(email: str, segment: dict, analysis: dict) -> str:
    findings_html = []
    for item in analysis["findings"]:
        findings_html.append(
            f"""
            <tr>
              <td style="padding:8px;border-bottom:1px solid #DBE3EB;">{esc(item['question'])}</td>
              <td style="padding:8px;border-bottom:1px solid #DBE3EB;">{esc(item['user'])}</td>
              <td style="padding:8px;border-bottom:1px solid #DBE3EB;">{esc(item['benchmark'])}</td>
              <td style="padding:8px;border-bottom:1px solid #DBE3EB;color:{color_for_status(item['status'])};font-weight:700;">{esc(item['status'].upper())}</td>
            </tr>
            """
        )

    ndt_line = ""
    if analysis["ndt_slowdown_pct"] > 0:
        ndt_line = (
            "<p style='margin:0 0 14px;'>Atencion: vuestro tiempo neto de decision es "
            f"aprox. {analysis['ndt_slowdown_pct']:.1f}% mas lento que la referencia de segmento.</p>"
        )

    return f"""
<!doctype html>
<html lang="es">
<head><meta charset="utf-8" /></head>
<body style="font-family:Inter, Arial, sans-serif;background:#FFFFFF;color:#0D1B2A;padding:24px;">
  <div style="max-width:760px;margin:0 auto;background:#FFFFFF;border:1px solid #DBE3EB;border-radius:12px;padding:22px;">
    <p style="margin:0 0 8px;text-transform:uppercase;letter-spacing:.06em;color:#B89B5B;font-size:12px;font-family:Montserrat, Arial, sans-serif;font-weight:700;">{esc(CONFIG['app_name'])}</p>
    <h1 style="margin:0 0 8px;font-size:26px;font-family:Montserrat, Arial, sans-serif;color:#0D1B2A;">Informe comparativo express</h1>
    <p style="margin:0 0 16px;color:#5F6D7A;">Email destino: {esc(email)}</p>
    <p style="margin:0 0 14px;">Segmento usado: {esc(segment['facturacion_2024'])} · {esc(segment['tamano_empresa'])} · {esc(segment['sector'])}</p>
    {ndt_line}
    <div style="display:flex;gap:16px;flex-wrap:wrap;margin:0 0 16px;">
      <div style="background:#FFFFFF;border:1px solid #DBE3EB;border-radius:10px;padding:10px 12px;min-width:170px;">
        <p style="margin:0;font-size:12px;color:#5F6D7A;">Score empresa</p>
        <p style="margin:4px 0 0;font-size:24px;font-weight:700;color:#0D1B2A;font-family:Montserrat, Arial, sans-serif;">{analysis['overall_score']}</p>
      </div>
      <div style="background:#FFFFFF;border:1px solid #DBE3EB;border-radius:10px;padding:10px 12px;min-width:170px;">
        <p style="margin:0;font-size:12px;color:#5F6D7A;">Score benchmark</p>
        <p style="margin:4px 0 0;font-size:24px;font-weight:700;color:#0D1B2A;font-family:Montserrat, Arial, sans-serif;">{analysis['overall_benchmark']}</p>
      </div>
      <div style="background:#FFFFFF;border:1px solid #DBE3EB;border-radius:10px;padding:10px 12px;min-width:170px;">
        <p style="margin:0;font-size:12px;color:#5F6D7A;">Alertas rojas</p>
        <p style="margin:4px 0 0;font-size:24px;font-weight:700;color:#0D1B2A;font-family:Montserrat, Arial, sans-serif;">{analysis['red_alerts']}</p>
      </div>
    </div>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <thead>
        <tr>
          <th style="text-align:left;padding:8px;border-bottom:1px solid #DBE3EB;color:#0D1B2A;">Indicador</th>
          <th style="text-align:left;padding:8px;border-bottom:1px solid #DBE3EB;color:#0D1B2A;">Empresa</th>
          <th style="text-align:left;padding:8px;border-bottom:1px solid #DBE3EB;color:#0D1B2A;">Benchmark</th>
          <th style="text-align:left;padding:8px;border-bottom:1px solid #DBE3EB;color:#0D1B2A;">Estado</th>
        </tr>
      </thead>
      <tbody>
        {''.join(findings_html)}
      </tbody>
    </table>
    <p style="margin:16px 0 0;color:#5F6D7A;font-size:12px;">Generado automaticamente por el motor de comparativa segmentada.</p>
  </div>
</body>
</html>
"""


def send_webhook(payload: dict) -> tuple[str, str]:
    if not REPORT_WEBHOOK_URL:
        return "not_configured", "REPORT_WEBHOOK_URL no configurado"

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(REPORT_WEBHOOK_URL, data=data, method="POST")
    request.add_header("Content-Type", "application/json")
    if REPORT_WEBHOOK_TOKEN:
        request.add_header("Authorization", f"Bearer {REPORT_WEBHOOK_TOKEN}")

    try:
        with urlopen(request, timeout=9) as response:
            code = response.getcode()
            if 200 <= code < 300:
                return "sent", ""
            return "failed", f"Webhook respondio {code}"
    except URLError as exc:
        return "failed", str(exc)


def corporate_email_valid(email: str) -> tuple[bool, str]:
    email_re = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    if not email_re.match(email):
        return False, "Email invalido"

    domain = email.split("@", 1)[1].lower()
    if domain in set(CONFIG.get("free_email_domains", [])):
        return False, "Usa un email corporativo"

    return True, ""


def validate_answers(payload: dict) -> tuple[dict[str, str], str]:
    answers = payload.get("answers")
    if not isinstance(answers, dict):
        return {}, "answers debe ser un objeto"

    validated: dict[str, str] = {}
    for question in QUESTIONS:
        qid = question["id"]
        value = answers.get(qid)
        if not isinstance(value, str) or not value.strip():
            return {}, f"Falta respuesta para {qid}"

        value = value.strip()
        if value not in question["options"]:
            return {}, f"Respuesta no valida en {qid}"

        validated[qid] = value

    return validated, ""


def response_segment(answers: dict[str, str]) -> dict:
    return {
        "facturacion_2024": answers["q1"],
        "tamano_empresa": answers["q2"],
        "sector": answers["q3"],
    }


def store_response(
    *,
    email: str,
    segment: dict,
    answers: dict,
    benchmark: dict,
    analysis: dict,
    report_html: str,
    delivery_status: str,
    delivery_error: str,
    source_ip: str,
) -> str:
    response_id = uuid.uuid4().hex[:14]
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO audit_responses (
              id, created_at, email, facturacion_2024, tamano_empresa, sector,
              answers_json, benchmark_json, analysis_json, report_html,
              overall_score, overall_benchmark, red_alerts, yellow_alerts,
              delivery_status, delivery_error, source_ip
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                response_id,
                now_iso(),
                email,
                segment["facturacion_2024"],
                segment["tamano_empresa"],
                segment["sector"],
                json.dumps(answers, ensure_ascii=False),
                json.dumps(benchmark, ensure_ascii=False),
                json.dumps(analysis, ensure_ascii=False),
                report_html,
                analysis["overall_score"],
                analysis["overall_benchmark"],
                analysis["red_alerts"],
                analysis["yellow_alerts"],
                delivery_status,
                delivery_error,
                source_ip,
            ),
        )

    return response_id


def slugify(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", folded).strip("-").lower()
    return cleaned or uuid.uuid4().hex[:8]


def json_loads_or_default(raw: object, default: object) -> object:
    if not isinstance(raw, str) or not raw.strip():
        return copy.deepcopy(default)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return copy.deepcopy(default)

    if isinstance(default, dict) and isinstance(parsed, dict):
        return parsed
    if isinstance(default, list) and isinstance(parsed, list):
        return parsed
    return copy.deepcopy(default)


def normalize_question_id(raw: str, *, fallback: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_]+", "_", raw.strip()).strip("_").lower()
    return token or fallback


def normalize_question_type(raw_type: object) -> str:
    token = str(raw_type or "").strip().lower()
    if token in {"cards", "select"}:
        return "single"
    if token in {"multiselect", "multi"}:
        return "multiple"
    if token in CAMPAIGN_QUESTION_TYPES:
        return token
    return "single"


def normalize_campaign_questions(raw_questions: object) -> tuple[list[dict], str]:
    if not isinstance(raw_questions, list) or not raw_questions:
        return [], "questions debe ser una lista no vacia"

    normalized: list[dict] = []
    used_ids: set[str] = set()
    for idx, raw in enumerate(raw_questions, start=1):
        if not isinstance(raw, dict):
            return [], f"Pregunta {idx} invalida"

        qid = normalize_question_id(str(raw.get("id", "")), fallback=f"q{idx}")
        if qid in used_ids:
            return [], f"ID de pregunta duplicado: {qid}"
        used_ids.add(qid)

        title = str(raw.get("title", "")).strip()
        if not title:
            return [], f"Falta title en la pregunta {qid}"

        qtype = normalize_question_type(raw.get("type"))
        question: dict[str, object] = {
            "id": qid,
            "title": title,
            "type": qtype,
            "required": bool(raw.get("required", True)),
        }

        options_raw = raw.get("options")
        options: list[str] = []
        if isinstance(options_raw, list):
            for item in options_raw:
                if isinstance(item, str) and item.strip():
                    options.append(item.strip())

        if qtype in {"single", "multiple"}:
            if not options:
                return [], f"La pregunta {qid} requiere options"
            question["options"] = options
        elif qtype == "scale":
            if options:
                question["options"] = options
            else:
                try:
                    min_v = float(raw.get("min", 1))
                    max_v = float(raw.get("max", 5))
                except (TypeError, ValueError):
                    return [], f"Rango invalido en {qid}"

                if min_v >= max_v:
                    return [], f"Rango invalido en {qid}: min debe ser menor que max"
                question["min"] = min_v
                question["max"] = max_v

        direction = str(raw.get("direction", "higher_better")).strip().lower()
        if direction in {"higher_better", "lower_better"}:
            question["direction"] = direction

        segment_key = str(raw.get("segment_key", "")).strip()
        if segment_key in SEGMENT_FIELD_KEYS:
            question["segment_key"] = segment_key

        compare = bool(raw.get("compare", False))
        if segment_key:
            compare = False
        if qtype in {"text", "multiple"}:
            compare = False
        question["compare"] = compare

        block = str(raw.get("block", "")).strip()
        if block:
            question["block"] = block

        helper = str(raw.get("helper", "")).strip()
        if helper:
            question["helper"] = helper

        normalized.append(question)

    return normalized, ""


def normalize_flash_mapping(raw_mapping: object, flash_questions: list[dict], barometer_questions: list[dict]) -> tuple[dict[str, str], str]:
    if not isinstance(raw_mapping, dict):
        return {}, "mapping debe ser un objeto"

    flash_ids = {str(q["id"]) for q in flash_questions}
    barometer_ids = {str(q["id"]) for q in barometer_questions}
    mapping: dict[str, str] = {}
    for key, value in raw_mapping.items():
        flash_id = str(key).strip()
        barometer_id = str(value).strip()
        if flash_id not in flash_ids:
            return {}, f"mapping contiene pregunta Flash inexistente: {flash_id}"
        if barometer_id not in barometer_ids:
            return {}, f"mapping apunta a pregunta Barometro inexistente: {barometer_id}"
        mapping[flash_id] = barometer_id

    return mapping, ""


def validate_campaign_answers(payload_answers: object, questions: list[dict]) -> tuple[dict[str, object], str]:
    if not isinstance(payload_answers, dict):
        return {}, "answers debe ser un objeto"

    validated: dict[str, object] = {}
    for question in questions:
        qid = str(question["id"])
        qtype = str(question.get("type", "single"))
        required = bool(question.get("required", True))
        value = payload_answers.get(qid)

        if qtype == "text":
            text = str(value or "").strip()
            if required and not text:
                return {}, f"Falta respuesta para {qid}"
            validated[qid] = text
            continue

        if qtype == "single":
            if not isinstance(value, str) or not value.strip():
                if required:
                    return {}, f"Falta respuesta para {qid}"
                validated[qid] = ""
                continue
            candidate = value.strip()
            options = [str(o) for o in question.get("options", [])]
            if candidate not in options:
                return {}, f"Respuesta no valida en {qid}"
            validated[qid] = candidate
            continue

        if qtype == "multiple":
            if value is None and not required:
                validated[qid] = []
                continue
            if not isinstance(value, list):
                return {}, f"Formato invalido en {qid}"

            options = {str(o) for o in question.get("options", [])}
            selected: list[str] = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    token = item.strip()
                    if token not in options:
                        return {}, f"Respuesta no valida en {qid}"
                    if token not in selected:
                        selected.append(token)

            if required and not selected:
                return {}, f"Falta respuesta para {qid}"
            validated[qid] = selected
            continue

        if qtype == "scale":
            options = [str(o) for o in question.get("options", [])]
            if options:
                if isinstance(value, (int, float)):
                    candidate = str(int(value) if float(value).is_integer() else value)
                else:
                    candidate = str(value or "").strip()

                if not candidate:
                    if required:
                        return {}, f"Falta respuesta para {qid}"
                    validated[qid] = ""
                    continue

                if candidate not in options:
                    return {}, f"Respuesta no valida en {qid}"
                validated[qid] = candidate
                continue

            if value in {"", None}:
                if required:
                    return {}, f"Falta respuesta para {qid}"
                validated[qid] = None
                continue

            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return {}, f"Valor numerico invalido en {qid}"

            min_v = float(question.get("min", 1))
            max_v = float(question.get("max", 5))
            if numeric < min_v or numeric > max_v:
                return {}, f"Valor fuera de rango en {qid}"

            validated[qid] = round(numeric, 2)
            continue

        return {}, f"Tipo de pregunta no soportado en {qid}"

    return validated, ""


def answer_to_text(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(str(v) for v in value if str(v).strip())
    if value is None:
        return "-"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.1f}"
    return str(value)


def extract_segment_from_campaign_answers(questions: list[dict], answers: dict[str, object]) -> dict[str, str]:
    segment = {"sector": "", "tamano_empresa": "", "facturacion_2024": ""}
    for question in questions:
        segment_key = str(question.get("segment_key", "")).strip()
        if segment_key not in segment:
            continue
        segment[segment_key] = answer_to_text(answers.get(str(question["id"]), "")).strip()
    return segment


def fetch_barometer_campaign_by_slug(slug: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM campaign_barometer
            WHERE slug = ? AND is_active = 1
            """,
            (slug,),
        ).fetchone()

    if not row:
        return None

    campaign = dict(row)
    campaign["questions"] = json_loads_or_default(campaign.get("questions_json"), [])
    return campaign


def fetch_barometer_campaign_by_id(barometer_id: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM campaign_barometer
            WHERE id = ? AND is_active = 1
            """,
            (barometer_id,),
        ).fetchone()

    if not row:
        return None

    campaign = dict(row)
    campaign["questions"] = json_loads_or_default(campaign.get("questions_json"), [])
    return campaign


def fetch_flash_audit_by_slug(slug: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM campaign_flash_audit
            WHERE slug = ? AND is_active = 1
            """,
            (slug,),
        ).fetchone()

    if not row:
        return None

    campaign = dict(row)
    campaign["questions"] = json_loads_or_default(campaign.get("questions_json"), [])
    campaign["mapping"] = json_loads_or_default(campaign.get("mapping_json"), {})
    return campaign


def fetch_flash_audit_by_id(flash_id: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM campaign_flash_audit
            WHERE id = ? AND is_active = 1
            """,
            (flash_id,),
        ).fetchone()

    if not row:
        return None

    campaign = dict(row)
    campaign["questions"] = json_loads_or_default(campaign.get("questions_json"), [])
    campaign["mapping"] = json_loads_or_default(campaign.get("mapping_json"), {})
    return campaign


def list_campaigns() -> dict[str, list[dict]]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        barometers = conn.execute(
            """
            SELECT id, name, year, slug, public_url, created_at
            FROM campaign_barometer
            WHERE is_active = 1
            ORDER BY year DESC, created_at DESC
            """
        ).fetchall()
        flash_audits = conn.execute(
            """
            SELECT id, name, slug, public_url, barometer_id, created_at
            FROM campaign_flash_audit
            WHERE is_active = 1
            ORDER BY created_at DESC
            """
        ).fetchall()

    return {
        "barometers": [dict(row) for row in barometers],
        "flash_audits": [dict(row) for row in flash_audits],
    }


def create_barometer_campaign(*, name: str, year: int, slug: str, description: str, questions: list[dict]) -> tuple[str, str]:
    campaign_id = uuid.uuid4().hex[:14]
    public_url = f"/barometro/{slug}"
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute(
                """
                INSERT INTO campaign_barometer (
                  id, name, year, slug, public_url, description, questions_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    campaign_id,
                    name,
                    year,
                    slug,
                    public_url,
                    description,
                    json.dumps(questions, ensure_ascii=False),
                    now_iso(),
                ),
            )
        except sqlite3.IntegrityError:
            return "", "Ya existe un barometro con ese slug"

    return campaign_id, ""


def create_flash_audit_campaign(
    *,
    name: str,
    slug: str,
    barometer_id: str,
    questions: list[dict],
    mapping: dict[str, str],
    cta_label: str,
    cta_url: str,
) -> tuple[str, str]:
    campaign_id = uuid.uuid4().hex[:14]
    public_url = f"/flash-audit/{slug}"
    with sqlite3.connect(DB_PATH) as conn:
        exists = conn.execute(
            "SELECT id FROM campaign_barometer WHERE id = ? AND is_active = 1",
            (barometer_id,),
        ).fetchone()
        if not exists:
            return "", "El Flash Audit debe vincularse a un Barometro valido"

        try:
            conn.execute(
                """
                INSERT INTO campaign_flash_audit (
                  id, name, slug, public_url, barometer_id, questions_json, mapping_json,
                  cta_label, cta_url, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    campaign_id,
                    name,
                    slug,
                    public_url,
                    barometer_id,
                    json.dumps(questions, ensure_ascii=False),
                    json.dumps(mapping, ensure_ascii=False),
                    cta_label,
                    cta_url,
                    now_iso(),
                ),
            )
        except sqlite3.IntegrityError:
            return "", "Ya existe un Flash Audit con ese slug"

    return campaign_id, ""


def default_barometer_questions() -> list[dict]:
    segment_key_by_id = {"q1": "facturacion_2024", "q2": "tamano_empresa", "q3": "sector"}
    normalized: list[dict] = []
    for question in QUESTIONS:
        qtype = normalize_question_type(question.get("type"))
        options = [str(item).strip() for item in question.get("options", []) if str(item).strip()]
        item = {
            "id": str(question["id"]),
            "title": str(question.get("title", question["id"])),
            "type": qtype,
            "required": True,
            "block": str(question.get("block", "")),
            "helper": str(question.get("helper", "")),
            "compare": str(question["id"]) in COMPARISON_QUESTION_IDS,
        }
        if options:
            item["options"] = options
        direction = str(question.get("direction", "")).strip()
        if direction in {"higher_better", "lower_better"}:
            item["direction"] = direction
        segment_key = segment_key_by_id.get(str(question["id"]))
        if segment_key:
            item["segment_key"] = segment_key
            item["compare"] = False
        normalized.append(item)
    return normalized


def default_flash_audit_definition() -> tuple[list[dict], dict[str, str]]:
    q_by_id = {q["id"]: q for q in default_barometer_questions()}

    def clone_question(source_id: str, target_id: str, title: str, *, compare: bool) -> dict:
        source = q_by_id[source_id]
        q = {
            "id": target_id,
            "title": title,
            "type": source["type"],
            "required": True,
            "options": source.get("options", []),
            "direction": source.get("direction", "higher_better"),
            "compare": compare,
            "block": source.get("block", ""),
        }
        if source.get("segment_key"):
            q["segment_key"] = source["segment_key"]
            q["compare"] = False
        return q

    questions = [
        clone_question("q3", "fa_sector", "Sector de tu empresa", compare=False),
        clone_question("q2", "fa_tamano", "Tamano de empresa", compare=False),
        clone_question("q1", "fa_facturacion", "Facturacion anual", compare=False),
        clone_question("q4", "fa_ndt", "Tiempo neto de decision", compare=True),
        clone_question("q5", "fa_decisiones_aplazadas", "Decisiones aplazadas", compare=True),
        clone_question("q8", "fa_rotacion", "Rotacion no deseada en roles criticos", compare=True),
        clone_question("q10", "fa_adopcion_ia", "Nivel de adopcion de IA", compare=True),
    ]

    for item in questions:
        if item["id"] == "fa_sector":
            item["segment_key"] = "sector"
        elif item["id"] == "fa_tamano":
            item["segment_key"] = "tamano_empresa"
        elif item["id"] == "fa_facturacion":
            item["segment_key"] = "facturacion_2024"

    mapping = {
        "fa_sector": "q3",
        "fa_tamano": "q2",
        "fa_facturacion": "q1",
        "fa_ndt": "q4",
        "fa_decisiones_aplazadas": "q5",
        "fa_rotacion": "q8",
        "fa_adopcion_ia": "q10",
    }
    return questions, mapping


def seed_default_campaigns() -> None:
    barometer_questions = default_barometer_questions()
    flash_questions, flash_mapping = default_flash_audit_definition()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        existing_barometer = conn.execute(
            "SELECT id FROM campaign_barometer WHERE slug = ?",
            (DEFAULT_BAROMETER_SLUG,),
        ).fetchone()
        if existing_barometer:
            barometer_id = existing_barometer["id"]
        else:
            barometer_id = uuid.uuid4().hex[:14]
            conn.execute(
                """
                INSERT INTO campaign_barometer (
                  id, name, year, slug, public_url, description, questions_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    barometer_id,
                    "Barometro Alta Direccion 2026",
                    2026,
                    DEFAULT_BAROMETER_SLUG,
                    f"/barometro/{DEFAULT_BAROMETER_SLUG}",
                    "Dataset maestro para benchmark sectorial en tiempo real.",
                    json.dumps(barometer_questions, ensure_ascii=False),
                    now_iso(),
                ),
            )

        rows_count = conn.execute(
            "SELECT COUNT(1) AS c FROM data_barometer WHERE barometer_id = ?",
            (barometer_id,),
        ).fetchone()["c"]
        if rows_count == 0:
            for row in SEGMENT_ROWS:
                sector = str(row.get("sector", "")).strip()
                tamano = str(row.get("tamano_empresa", "")).strip()
                facturacion = str(row.get("facturacion_2024", "")).strip()
                if "*" in {sector, tamano, facturacion}:
                    continue

                answers = {
                    "q1": facturacion,
                    "q2": tamano,
                    "q3": sector,
                    "q4": str(row.get("q4", "")).strip(),
                    "q5": str(row.get("q5", "")).strip(),
                    "q6": str(row.get("q6", "")).strip(),
                    "q7": str(row.get("q7", "")).strip(),
                    "q8": str(row.get("q8", "")).strip(),
                    "q9": str(row.get("q9", "")).strip(),
                    "q10": str(row.get("q10", "")).strip(),
                }
                weight = int(row.get("sample_size", 1) or 1)
                conn.execute(
                    """
                    INSERT INTO data_barometer (
                      id, barometer_id, created_at, sector, tamano_empresa, facturacion_2024,
                      answers_json, sample_weight, source_ip
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid.uuid4().hex[:14],
                        barometer_id,
                        now_iso(),
                        sector,
                        tamano,
                        facturacion,
                        json.dumps(answers, ensure_ascii=False),
                        max(weight, 1),
                        "seed",
                    ),
                )

        existing_flash = conn.execute(
            "SELECT id FROM campaign_flash_audit WHERE slug = ?",
            (DEFAULT_FLASH_AUDIT_SLUG,),
        ).fetchone()
        if not existing_flash:
            conn.execute(
                """
                INSERT INTO campaign_flash_audit (
                  id, name, slug, public_url, barometer_id, questions_json, mapping_json,
                  cta_label, cta_url, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid.uuid4().hex[:14],
                    "Flash Audit General",
                    DEFAULT_FLASH_AUDIT_SLUG,
                    f"/flash-audit/{DEFAULT_FLASH_AUDIT_SLUG}",
                    barometer_id,
                    json.dumps(flash_questions, ensure_ascii=False),
                    json.dumps(flash_mapping, ensure_ascii=False),
                    "Solucionar estas ineficiencias con GHC - Agendar Sesion",
                    CONFIG.get("calendly_url", "https://calendly.com/globalhumancon/diagnostico"),
                    now_iso(),
                ),
            )


def _segment_filters(segment: dict[str, str]) -> list[dict[str, str]]:
    sector = segment.get("sector", "").strip()
    tamano = segment.get("tamano_empresa", "").strip()
    facturacion = segment.get("facturacion_2024", "").strip()

    filters: list[dict[str, str]] = []
    if sector and tamano and facturacion:
        filters.append(
            {
                "sector": sector,
                "tamano_empresa": tamano,
                "facturacion_2024": facturacion,
            }
        )
    if sector and tamano:
        filters.append({"sector": sector, "tamano_empresa": tamano})
    if sector:
        filters.append({"sector": sector})
    filters.append({})
    return filters


def load_barometer_rows_for_segment(barometer_id: str, segment: dict[str, str]) -> tuple[list[sqlite3.Row], str]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        for filters in _segment_filters(segment):
            conditions = ["barometer_id = ?"]
            params: list[object] = [barometer_id]
            for key, value in filters.items():
                conditions.append(f"{key} = ?")
                params.append(value)

            query = (
                "SELECT answers_json, sample_weight FROM data_barometer WHERE "
                + " AND ".join(conditions)
            )
            rows = conn.execute(query, params).fetchall()
            if rows:
                if len(filters) == 3:
                    return rows, "exact_match"
                if len(filters) == 2:
                    return rows, "sector_size_match"
                if len(filters) == 1:
                    return rows, "sector_match"
                return rows, "global_fallback"
    return [], "no_data"


def default_benchmark_value(question: dict) -> object:
    qtype = str(question.get("type", "single"))
    options = [str(item) for item in question.get("options", [])]
    if qtype in {"single", "scale"} and options:
        return options[len(options) // 2]
    if qtype == "scale":
        min_v = float(question.get("min", 1))
        max_v = float(question.get("max", 5))
        return round((min_v + max_v) / 2, 1)
    return None


def parse_numeric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip().replace(",", ".")
        if not token:
            return None
        try:
            return float(token)
        except ValueError:
            return None
    return None


def build_benchmark_from_barometer(
    *,
    barometer_id: str,
    barometer_questions: list[dict],
    requested_qids: set[str],
    segment: dict[str, str],
) -> tuple[dict[str, object], dict]:
    rows, match_type = load_barometer_rows_for_segment(barometer_id, segment)
    question_by_id = {str(q["id"]): q for q in barometer_questions}
    benchmarks: dict[str, object] = {}

    parsed_rows: list[tuple[dict, int]] = []
    for row in rows:
        answers = json_loads_or_default(row["answers_json"], {})
        if not isinstance(answers, dict):
            continue
        try:
            weight = int(row["sample_weight"] or 1)
        except (TypeError, ValueError):
            weight = 1
        parsed_rows.append((answers, max(weight, 1)))

    for qid in requested_qids:
        question = question_by_id.get(qid)
        if not question:
            continue
        qtype = str(question.get("type", "single"))
        values: list[tuple[object, int]] = []
        for answers, weight in parsed_rows:
            if qid in answers:
                value = answers[qid]
                if value not in {"", None} and value != []:
                    values.append((value, weight))

        if not values:
            benchmarks[qid] = default_benchmark_value(question)
            continue

        if qtype == "single":
            weighted_values: list[tuple[str, int]] = []
            valid_options = {str(item) for item in question.get("options", [])}
            for value, weight in values:
                if isinstance(value, str) and value in valid_options:
                    weighted_values.append((value, weight))
            if weighted_values:
                benchmarks[qid] = weighted_mode(weighted_values)
            else:
                benchmarks[qid] = default_benchmark_value(question)
            continue

        if qtype == "scale":
            options = [str(item) for item in question.get("options", [])]
            if options:
                weighted_values = []
                for value, weight in values:
                    token = answer_to_text(value)
                    if token in options:
                        weighted_values.append((token, weight))
                if weighted_values:
                    benchmarks[qid] = weighted_mode(weighted_values)
                else:
                    benchmarks[qid] = default_benchmark_value(question)
                continue

            total_weight = 0
            weighted_sum = 0.0
            for value, weight in values:
                numeric = parse_numeric(value)
                if numeric is None:
                    continue
                total_weight += weight
                weighted_sum += numeric * weight

            if total_weight > 0:
                benchmarks[qid] = round(weighted_sum / total_weight, 1)
            else:
                benchmarks[qid] = default_benchmark_value(question)
            continue

        if qtype == "multiple":
            counter: dict[str, int] = {}
            valid_options = {str(item) for item in question.get("options", [])}
            for value, weight in values:
                if isinstance(value, list):
                    iterable = value
                else:
                    iterable = [value]
                for item in iterable:
                    token = str(item).strip()
                    if not token or token not in valid_options:
                        continue
                    counter[token] = counter.get(token, 0) + weight
            top_values = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:2]
            benchmarks[qid] = [item[0] for item in top_values]
            continue

        benchmarks[qid] = default_benchmark_value(question)

    sample_size = sum(weight for _, weight in parsed_rows)
    return benchmarks, {
        "match_type": match_type,
        "sample_size": sample_size,
        "records_used": len(parsed_rows),
    }


def compare_answer_vs_benchmark(
    *,
    flash_question: dict,
    barometer_question: dict,
    user_value: object,
    benchmark_value: object,
) -> tuple[str, float]:
    qtype = str(barometer_question.get("type", "single"))
    direction = str(barometer_question.get("direction", "higher_better"))

    if qtype == "single":
        options = [str(item) for item in barometer_question.get("options", [])]
        if not options:
            return "neutral", 50.0
        user_token = answer_to_text(user_value)
        bench_token = answer_to_text(benchmark_value)
        if user_token not in options or bench_token not in options:
            return "neutral", 50.0
        user_idx = options.index(user_token)
        bench_idx = options.index(bench_token)
        if user_idx == bench_idx:
            return "yellow", 60.0
        if direction == "lower_better":
            return ("green", 100.0) if user_idx < bench_idx else ("red", 20.0)
        return ("green", 100.0) if user_idx > bench_idx else ("red", 20.0)

    if qtype == "scale":
        options = [str(item) for item in barometer_question.get("options", [])]
        if options:
            user_token = answer_to_text(user_value)
            bench_token = answer_to_text(benchmark_value)
            if user_token not in options or bench_token not in options:
                return "neutral", 50.0
            user_idx = options.index(user_token)
            bench_idx = options.index(bench_token)
            if user_idx == bench_idx:
                return "yellow", 60.0
            if direction == "lower_better":
                return ("green", 100.0) if user_idx < bench_idx else ("red", 20.0)
            return ("green", 100.0) if user_idx > bench_idx else ("red", 20.0)

        user_n = parse_numeric(user_value)
        bench_n = parse_numeric(benchmark_value)
        if user_n is None or bench_n is None:
            return "neutral", 50.0
        if abs(user_n - bench_n) < 1e-9:
            return "yellow", 60.0
        if direction == "lower_better":
            return ("green", 100.0) if user_n < bench_n else ("red", 20.0)
        return ("green", 100.0) if user_n > bench_n else ("red", 20.0)

    # "text" and "multiple" cannot be compared with a deterministic semaforo.
    return "neutral", 50.0


def build_flash_audit_analysis(
    *,
    flash_questions: list[dict],
    barometer_questions: list[dict],
    mapping: dict[str, str],
    answers: dict[str, object],
    benchmark_by_barometer_qid: dict[str, object],
    benchmark_meta: dict,
) -> dict:
    barometer_question_by_id = {str(q["id"]): q for q in barometer_questions}
    findings: list[dict] = []
    scores: list[float] = []
    red_alerts = 0
    yellow_alerts = 0

    for question in flash_questions:
        if not question.get("compare", False):
            continue
        flash_qid = str(question["id"])
        barometer_qid = str(mapping.get(flash_qid, "")).strip()
        if not barometer_qid:
            continue

        barometer_question = barometer_question_by_id.get(barometer_qid)
        if not barometer_question:
            continue

        user_value = answers.get(flash_qid)
        benchmark_value = benchmark_by_barometer_qid.get(barometer_qid)
        status, score = compare_answer_vs_benchmark(
            flash_question=question,
            barometer_question=barometer_question,
            user_value=user_value,
            benchmark_value=benchmark_value,
        )

        if status == "red":
            red_alerts += 1
        elif status == "yellow":
            yellow_alerts += 1
        scores.append(score)

        findings.append(
            {
                "flash_question_id": flash_qid,
                "barometer_question_id": barometer_qid,
                "label": str(question.get("title", flash_qid)),
                "user_value": answer_to_text(user_value),
                "benchmark_value": answer_to_text(benchmark_value),
                "status": status,
                "score": score,
            }
        )

    overall_score = round(sum(scores) / len(scores), 1) if scores else 0.0
    priority = [item for item in findings if item["status"] == "red"][:3]
    if len(priority) < 3:
        priority += [item for item in findings if item["status"] == "yellow"][: 3 - len(priority)]

    return {
        "overall_score": overall_score,
        "red_alerts": red_alerts,
        "yellow_alerts": yellow_alerts,
        "findings": findings,
        "priority_findings": priority,
        "benchmark_meta": benchmark_meta,
    }


def store_barometer_submission(
    *,
    barometer_id: str,
    answers: dict[str, object],
    segment: dict[str, str],
    source_ip: str,
) -> str:
    response_id = uuid.uuid4().hex[:14]
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO data_barometer (
              id, barometer_id, created_at, sector, tamano_empresa, facturacion_2024,
              answers_json, sample_weight, source_ip
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                response_id,
                barometer_id,
                now_iso(),
                segment.get("sector", ""),
                segment.get("tamano_empresa", ""),
                segment.get("facturacion_2024", ""),
                json.dumps(answers, ensure_ascii=False),
                1,
                source_ip,
            ),
        )
    return response_id


def store_flash_lead(
    *,
    flash: dict,
    lead: dict[str, str],
    answers: dict[str, object],
    segment: dict[str, str],
    benchmark: dict[str, object],
    analysis: dict,
    source_ip: str,
) -> str:
    lead_id = uuid.uuid4().hex[:14]
    result_url = f"/flash-audit/resultado/{lead_id}"
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO leads_flash_audit (
              id, created_at, flash_audit_id, barometer_id, lead_name, lead_email, lead_company,
              sector, tamano_empresa, facturacion_2024, answers_json, benchmark_json, analysis_json,
              scoring_json, overall_score, red_alerts, yellow_alerts, result_url, source_ip
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lead_id,
                now_iso(),
                flash["id"],
                flash["barometer_id"],
                lead.get("name", ""),
                lead.get("email", ""),
                lead.get("company", ""),
                segment.get("sector", ""),
                segment.get("tamano_empresa", ""),
                segment.get("facturacion_2024", ""),
                json.dumps(answers, ensure_ascii=False),
                json.dumps(benchmark, ensure_ascii=False),
                json.dumps(analysis, ensure_ascii=False),
                json.dumps({"version": "v2"}, ensure_ascii=False),
                float(analysis.get("overall_score", 0.0)),
                int(analysis.get("red_alerts", 0)),
                int(analysis.get("yellow_alerts", 0)),
                result_url,
                source_ip,
            ),
        )
    return lead_id


def load_flash_lead(lead_id: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM leads_flash_audit
            WHERE id = ?
            """,
            (lead_id,),
        ).fetchone()

    if not row:
        return None

    data = dict(row)
    data["answers"] = json_loads_or_default(data.get("answers_json"), {})
    data["benchmark"] = json_loads_or_default(data.get("benchmark_json"), {})
    data["analysis"] = json_loads_or_default(data.get("analysis_json"), {})
    return data


def load_flash_leads_summary(limit: int = 80) -> tuple[list[sqlite3.Row], dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT l.*, f.name AS flash_name
            FROM leads_flash_audit l
            JOIN campaign_flash_audit f ON f.id = l.flash_audit_id
            ORDER BY l.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    total = len(rows)
    red_total = sum(int(row["red_alerts"]) for row in rows)
    avg_score = round(sum(float(row["overall_score"]) for row in rows) / total, 1) if total else 0.0
    return rows, {
        "total": total,
        "red_total": red_total,
        "avg_score": avg_score,
    }


def normalize_lead_payload(raw: object) -> tuple[dict[str, str], str]:
    if not isinstance(raw, dict):
        return {}, "lead debe ser un objeto"

    lead = {
        "name": str(raw.get("name", "")).strip(),
        "email": str(raw.get("email", "")).strip().lower(),
        "company": str(raw.get("company", "")).strip(),
    }
    if not lead["name"]:
        return {}, "Falta nombre"
    if not lead["company"]:
        return {}, "Falta empresa"
    ok_email, error = corporate_email_valid(lead["email"])
    if not ok_email:
        return {}, error
    return lead, ""


def parse_post_form(handler: BaseHTTPRequestHandler) -> dict[str, str]:
    length = int(handler.headers.get("Content-Length", "0"))
    payload = handler.rfile.read(length).decode("utf-8")
    parsed = parse_qs(payload, keep_blank_values=True)
    return {k: (v[0].strip() if v else "") for k, v in parsed.items()}


def parse_json_body(handler: BaseHTTPRequestHandler) -> tuple[dict, str]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError:
        return {}, "Content-Length invalido"

    raw = handler.rfile.read(length)
    try:
        return json.loads(raw.decode("utf-8")), ""
    except json.JSONDecodeError:
        return {}, "JSON invalido"


def parse_inside_scope_input(
    handler: BaseHTTPRequestHandler,
) -> tuple[str, dict[str, str | bytes] | None, str, str]:
    content_type = (handler.headers.get("Content-Type", "") or "").lower()

    if content_type.startswith("application/json"):
        payload, error = parse_json_body(handler)
        if error:
            return "", None, "", error

        text = str(payload.get("text", "")).strip()
        mode = str(payload.get("mode", "text")).strip().lower() or "text"
        return text, None, mode, ""

    environ = {
        "REQUEST_METHOD": "POST",
        "CONTENT_TYPE": handler.headers.get("Content-Type", ""),
        "CONTENT_LENGTH": handler.headers.get("Content-Length", "0"),
    }

    try:
        form = cgi.FieldStorage(
            fp=handler.rfile,
            headers=handler.headers,
            environ=environ,
            keep_blank_values=True,
        )
    except Exception as exc:
        return "", None, "", f"No se pudo leer el formulario: {exc}"

    text = str(form.getfirst("text", "")).strip()
    mode = str(form.getfirst("mode", "audio")).strip().lower() or "audio"

    audio_info: dict[str, str | bytes] | None = None
    if "audio" in form:
        file_field = form["audio"]
        if isinstance(file_field, list):
            file_field = file_field[0]
        if getattr(file_field, "filename", ""):
            file_bytes = file_field.file.read()
            audio_info = {
                "filename": str(file_field.filename),
                "content_type": str(file_field.type or "application/octet-stream"),
                "bytes": file_bytes,
            }

    return text, audio_info, mode, ""


def gemini_generate_content(model: str, payload: dict, timeout: int = 60) -> tuple[dict, str]:
    if not GEMINI_API_KEY:
        return {}, "GEMINI_API_KEY no configurado"

    clean_model = (model or "").strip()
    if not clean_model:
        return {}, "Modelo Gemini no configurado"

    endpoint = (
        f"{GEMINI_API_BASE}/models/{quote(clean_model, safe='')}:generateContent"
        f"?key={quote(GEMINI_API_KEY, safe='')}"
    )
    request = Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
    )
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/json")

    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            detail = ""

        if detail:
            return {}, f"Error Gemini ({exc.code}): {detail[:320]}"
        return {}, f"Error Gemini ({exc.code}): {exc.reason}"
    except URLError as exc:
        return {}, f"Error conectando con Gemini: {exc}"

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}, "Respuesta invalida de Gemini"

    if isinstance(parsed, dict) and isinstance(parsed.get("error"), dict):
        message = str(parsed["error"].get("message", "")).strip() or "Error Gemini"
        return {}, message

    return parsed if isinstance(parsed, dict) else {}, ""


def gemini_extract_text(model_response: dict) -> str:
    candidates = model_response.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        return ""

    first = candidates[0]
    if not isinstance(first, dict):
        return ""

    content = first.get("content")
    if not isinstance(content, dict):
        return ""

    parts = content.get("parts")
    if not isinstance(parts, list):
        return ""

    chunks: list[str] = []
    for part in parts:
        if isinstance(part, dict):
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                chunks.append(text)

    return "\n".join(chunks).strip()


def parse_json_object_from_text(raw_text: str) -> tuple[dict, str]:
    text = (raw_text or "").strip()
    if not text:
        return {}, "Respuesta de IA vacia"

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}, ""
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}, "Respuesta de IA invalida al extraer campos"

    try:
        parsed = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return {}, "Respuesta de IA invalida al extraer campos"

    if not isinstance(parsed, dict):
        return {}, "Respuesta de IA invalida al extraer campos"
    return parsed, ""


def transcribe_audio_with_gemini(file_name: str, file_content_type: str, audio_bytes: bytes) -> tuple[str, str]:
    if not GEMINI_API_KEY:
        return "", "GEMINI_API_KEY no configurado"

    mime_type = (file_content_type or "").strip() or "audio/webm"
    encoded_audio = base64.b64encode(audio_bytes).decode("ascii")
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": (
                            "Transcribe este audio en español de España. "
                            "Devuelve solo la transcripción en texto plano, sin comentarios."
                        )
                    },
                    {"inline_data": {"mime_type": mime_type, "data": encoded_audio}},
                ],
            }
        ],
        "generationConfig": {"temperature": 0},
    }
    model_response, error = gemini_generate_content(GEMINI_TRANSCRIBE_MODEL, payload, timeout=75)
    if error:
        return "", f"Error transcribiendo audio: {error}"

    text = normalize_text(gemini_extract_text(model_response), max_len=12000)
    if not text:
        return "", "No se pudo generar transcripcion"
    return text, ""


def normalize_option(value: object, options: list[str]) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    folded = unicodedata.normalize("NFKD", normalized).encode("ascii", "ignore").decode("ascii").lower()
    for opt in options:
        folded_opt = unicodedata.normalize("NFKD", opt).encode("ascii", "ignore").decode("ascii").lower()
        if normalized == opt or normalized.lower() == opt.lower() or folded == folded_opt:
            return opt

    return "Otro" if "Otro" in options else None


def normalize_text(value: object, max_len: int = 400) -> str:
    if not isinstance(value, str):
        return ""

    cleaned = value.strip()
    if len(cleaned) > max_len:
        return cleaned[:max_len].strip()
    return cleaned


def extract_inside_scope_fields(transcription: str) -> tuple[dict[str, str | None], str]:
    if not GEMINI_API_KEY:
        return {}, "GEMINI_API_KEY no configurado"

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": (
                            f"{INSIDE_SCOPE_SYSTEM_PROMPT}\n\n"
                            f"Transcripción del consultor:\n{transcription}\n\n"
                            "Devuelve exclusivamente un objeto JSON válido."
                        )
                    }
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
        },
    }
    model_response, error = gemini_generate_content(GEMINI_EXTRACT_MODEL, payload)
    if error:
        return {}, f"Error extrayendo campos: {error}"

    content = gemini_extract_text(model_response)
    raw, parse_error = parse_json_object_from_text(content)
    if parse_error:
        return {}, parse_error

    extracted = {
        "empresa": normalize_text(raw.get("empresa"), 200) or None,
        "area_nivel": normalize_option(raw.get("area_nivel"), INSIDE_SCOPE_AREA_LEVEL_OPTIONS),
        "rango_salarial": normalize_text(raw.get("rango_salarial"), 120) or None,
        "motivo_salida": normalize_option(raw.get("motivo_salida"), INSIDE_SCOPE_EXIT_REASON_OPTIONS),
        "insight": normalize_text(raw.get("insight"), 400) or None,
    }
    return extracted, ""


def store_inside_scope_log(
    *,
    consultant_email: str,
    extracted: dict[str, str | None],
    transcription_raw: str,
    source_mode: str,
    source_ip: str,
) -> str:
    log_id = uuid.uuid4().hex[:14]
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO inside_scope_logs (
              id, created_at, consultant_email, empresa, area_nivel, rango_salarial,
              motivo_salida, insight, transcription_raw, source_mode, source_ip
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                log_id,
                now_iso(),
                consultant_email,
                extracted.get("empresa"),
                extracted.get("area_nivel"),
                extracted.get("rango_salarial"),
                extracted.get("motivo_salida"),
                extracted.get("insight"),
                transcription_raw,
                source_mode,
                source_ip,
            ),
        )
    return log_id


def process_inside_scope_intake(handler: BaseHTTPRequestHandler) -> tuple[dict, int]:
    consultant_email = current_admin_email(handler)
    if not consultant_email:
        return {"ok": False, "error": "Sesion no valida"}, 401

    text, audio_info, mode, parse_error = parse_inside_scope_input(handler)
    if parse_error:
        return {"ok": False, "error": parse_error}, 400

    transcription = text
    source_mode = "text"
    if audio_info and isinstance(audio_info.get("bytes"), bytes):
        audio_bytes = audio_info["bytes"]
        if len(audio_bytes) > 15 * 1024 * 1024:
            return {"ok": False, "error": "Audio demasiado grande (max 15MB)"}, 400

        source_mode = "audio"
        transcription, error = transcribe_audio_with_gemini(
            str(audio_info.get("filename", "inside-scope.webm")),
            str(audio_info.get("content_type", "application/octet-stream")),
            audio_bytes,
        )
        # RGPD: no persistimos el archivo de audio.
        del audio_bytes
        if error:
            return {"ok": False, "error": error}, 502
    elif mode == "audio":
        return {"ok": False, "error": "No se recibio audio"}, 400

    if not transcription.strip():
        return {"ok": False, "error": "No se obtuvo transcripcion util"}, 400

    extracted, extraction_error = extract_inside_scope_fields(transcription)
    if extraction_error:
        return {"ok": False, "error": extraction_error}, 502

    log_id = store_inside_scope_log(
        consultant_email=consultant_email,
        extracted=extracted,
        transcription_raw=transcription,
        source_mode=source_mode,
        source_ip=handler.client_address[0],
    )

    return {
        "ok": True,
        "log_id": log_id,
        "consultor": consultant_email,
        "empresa": extracted.get("empresa"),
        "area_nivel": extracted.get("area_nivel"),
        "rango_salarial": extracted.get("rango_salarial"),
        "motivo_salida": extracted.get("motivo_salida"),
        "insight": extracted.get("insight"),
    }, 200


def json_response(handler: BaseHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def html_response(handler: BaseHTTPRequestHandler, body: bytes, status: int = 200) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def css_response(handler: BaseHTTPRequestHandler) -> None:
    data = STYLE_PATH.read_bytes()
    handler.send_response(200)
    handler.send_header("Content-Type", "text/css; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def static_file_response(
    handler: BaseHTTPRequestHandler, *, file_path: Path, content_type: str, cache_control: str = "no-store"
) -> None:
    if not file_path.exists():
        payload = b"Not found"
        handler.send_response(404)
        handler.send_header("Content-Type", "text/plain; charset=utf-8")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)
        return

    data = file_path.read_bytes()
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Cache-Control", cache_control)
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def redirect_response(handler: BaseHTTPRequestHandler, location: str) -> None:
    handler.send_response(HTTPStatus.SEE_OTHER)
    handler.send_header("Location", location)
    handler.end_headers()


def create_admin_session(email: str) -> str:
    raw = f"{uuid.uuid4().hex}:{time.time()}".encode("utf-8")
    token = hmac.new(SESSION_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    ADMIN_SESSIONS[token] = {
        "exp": time.time() + SESSION_TTL_SECONDS,
        "email": email.strip().lower(),
    }
    return token


def prune_sessions() -> None:
    ts = time.time()
    for token in list(ADMIN_SESSIONS.keys()):
        session = ADMIN_SESSIONS[token]
        exp = session if isinstance(session, float) else float(session.get("exp", 0))
        if exp <= ts:
            ADMIN_SESSIONS.pop(token, None)


def get_session_token(handler: BaseHTTPRequestHandler) -> str | None:
    raw_cookie = handler.headers.get("Cookie")
    if not raw_cookie:
        return None

    cookie = SimpleCookie()
    cookie.load(raw_cookie)
    morsel = cookie.get("admin_session")
    return morsel.value if morsel else None


def is_admin_authenticated(handler: BaseHTTPRequestHandler) -> bool:
    prune_sessions()
    token = get_session_token(handler)
    if not token:
        return False

    session = ADMIN_SESSIONS.get(token)
    if not session:
        return False

    if isinstance(session, float):
        return session > time.time()

    return float(session.get("exp", 0)) > time.time()


def current_admin_email(handler: BaseHTTPRequestHandler) -> str:
    token = get_session_token(handler)
    if not token:
        return ""

    session = ADMIN_SESSIONS.get(token)
    if not isinstance(session, dict):
        return ""

    email = str(session.get("email", "")).strip().lower()
    return email


def consultant_display_name(email: str) -> str:
    cleaned = (email or "").strip().lower()
    if not cleaned or "@" not in cleaned:
        return "Consultor"

    local = cleaned.split("@", 1)[0]
    token = re.sub(r"[^a-z0-9]+", " ", local).strip()
    if not token:
        return cleaned

    return " ".join(chunk.capitalize() for chunk in token.split())


def credentials_valid(email: str, password: str) -> bool:
    expected = ADMIN_USERS.get((email or "").strip().lower())
    if expected is None:
        return False

    return hmac.compare_digest(password, expected)


def set_admin_cookie(handler: BaseHTTPRequestHandler, token: str) -> None:
    cookie = SimpleCookie()
    cookie["admin_session"] = token
    cookie["admin_session"]["path"] = "/"
    cookie["admin_session"]["httponly"] = True
    cookie["admin_session"]["samesite"] = "Lax"
    cookie["admin_session"]["max-age"] = str(SESSION_TTL_SECONDS)

    handler.send_response(HTTPStatus.SEE_OTHER)
    handler.send_header("Location", "/app")
    handler.send_header("Set-Cookie", cookie.output(header="").strip())
    handler.end_headers()


def clear_admin_cookie(handler: BaseHTTPRequestHandler) -> None:
    token = get_session_token(handler)
    if token:
        ADMIN_SESSIONS.pop(token, None)

    cookie = SimpleCookie()
    cookie["admin_session"] = ""
    cookie["admin_session"]["path"] = "/"
    cookie["admin_session"]["max-age"] = "0"

    handler.send_response(HTTPStatus.SEE_OTHER)
    handler.send_header("Location", "/login")
    handler.send_header("Set-Cookie", cookie.output(header="").strip())
    handler.end_headers()


def base_layout(title: str, subtitle: str, body: str) -> bytes:
    doc = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{esc(title)}</title>
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body>
  <main class="site-shell">
    <header class="admin-head">
      <p class="brand-line">{esc(CONFIG['app_name'])}</p>
      <h1>{esc(title)}</h1>
      <p>{esc(subtitle)}</p>
    </header>
    {body}
  </main>
</body>
</html>
"""
    return doc.encode("utf-8")


def app_shell() -> bytes:
    client_config = {
        "app_name": CONFIG["app_name"],
        "accent_color": CONFIG.get("accent_color", "#B89B5B"),
        "welcome": CONFIG["welcome"],
        "lead_capture": CONFIG["lead_capture"],
        "thank_you": CONFIG["thank_you"],
        "loading_messages": CONFIG["loading_messages"],
        "questions": QUESTIONS,
        "calendly_url": CONFIG.get("calendly_url", "https://calendly.com"),
        "free_email_domains": CONFIG.get("free_email_domains", []),
    }

    script_data = json.dumps(client_config, ensure_ascii=False)
    body = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{esc(CONFIG['app_name'])}</title>
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body>
  <main class="quiz-shell" id="app">
    <div class="progress-wrap hidden" id="progressWrap">
      <div class="progress-head">
        <span id="progressLabel">Pregunta 1 de 10</span>
      </div>
      <div class="progress-track">
        <div class="progress-fill" id="progressFill"></div>
      </div>
    </div>
    <section class="screen" id="screen"></section>
  </main>
  <script>
    window.AUDIT_CONFIG = {script_data};
  </script>
  <script>
    (function () {{
      const cfg = window.AUDIT_CONFIG;
      const questions = cfg.questions;
      const totalQuestions = questions.length;
      const screenEl = document.getElementById('screen');
      const progressWrap = document.getElementById('progressWrap');
      const progressFill = document.getElementById('progressFill');
      const progressLabel = document.getElementById('progressLabel');

      const state = {{
        step: 0,
        answers: {{}},
        email: '',
        submitting: false,
        error: '',
        responseId: ''
      }};

      let loadingInterval = null;

      const steps = [
        {{ kind: 'welcome' }},
        ...questions.map((q, i) => ({{ kind: 'question', question: q, qIndex: i + 1 }})),
        {{ kind: 'lead' }},
        {{ kind: 'loading' }},
        {{ kind: 'thanks' }}
      ];

      function escapeHtml(text) {{
        return String(text)
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;')
          .replaceAll('"', '&quot;')
          .replaceAll("'", '&#39;');
      }}

      function emailLooksCorporate(email) {{
        const ok = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!ok.test(email)) return false;
        const domain = email.split('@')[1].toLowerCase();
        return !cfg.free_email_domains.includes(domain);
      }}

      function updateProgress() {{
        const step = steps[state.step];
        if (!step || step.kind !== 'question') {{
          progressWrap.classList.add('hidden');
          return;
        }}

        progressWrap.classList.remove('hidden');
        progressLabel.textContent = `Pregunta ${{step.qIndex}} de ${{totalQuestions}}`;
        progressFill.style.width = `${{(step.qIndex / totalQuestions) * 100}}%`;
      }}

      function goTo(step) {{
        state.step = Math.max(0, Math.min(steps.length - 1, step));
        render();
      }}

      function goNext() {{
        goTo(state.step + 1);
      }}

      function goPrev() {{
        goTo(state.step - 1);
      }}

      function chooseOption(question, option) {{
        state.answers[question.id] = option;
        state.error = '';
        render();
        if (question.auto_advance) {{
          setTimeout(goNext, 130);
        }}
      }}

      function renderQuestion(step) {{
        const q = step.question;
        const selected = state.answers[q.id] || '';

        if (q.type === 'select') {{
          screenEl.innerHTML = `
            <article class="panel panel-animated">
              <p class="step-tag">Bloque: Segmentacion</p>
              <h1>${{escapeHtml(q.title)}}</h1>
              <p class="helper">Selecciona una opcion para continuar.</p>
              <div class="select-wrap">
                <select id="sectorSelect" class="select-input">
                  <option value="">Selecciona sector</option>
                  ${{q.options.map(opt => `<option value="${{escapeHtml(opt)}}" ${{selected === opt ? 'selected' : ''}}>${{escapeHtml(opt)}}</option>`).join('')}}
                </select>
              </div>
              <div class="actions-row">
                <button class="ghost-btn" id="prevBtn">Anterior</button>
                <button class="cta-btn" id="continueBtn" ${{selected ? '' : 'disabled'}}>Continuar</button>
              </div>
            </article>
          `;

          document.getElementById('sectorSelect').addEventListener('change', (e) => {{
            state.answers[q.id] = e.target.value;
            render();
          }});
          document.getElementById('continueBtn').addEventListener('click', goNext);
          document.getElementById('prevBtn').addEventListener('click', goPrev);
          return;
        }}

        screenEl.innerHTML = `
          <article class="panel panel-animated">
            <p class="step-tag">Bloque: ${{escapeHtml(q.block.replaceAll('_', ' '))}}</p>
            <h1>${{escapeHtml(q.title)}}</h1>
            <div class="cards-grid">
              ${{q.options.map(opt => `
                <button class="option-card ${{selected === opt ? 'is-selected' : ''}}" data-option="${{escapeHtml(opt)}}">
                  <span>${{escapeHtml(opt)}}</span>
                </button>
              `).join('')}}
            </div>
            <div class="actions-row">
              <button class="ghost-btn" id="prevBtn">Anterior</button>
            </div>
          </article>
        `;

        screenEl.querySelectorAll('.option-card').forEach((node) => {{
          node.addEventListener('click', () => chooseOption(q, node.dataset.option));
        }});

        document.getElementById('prevBtn').addEventListener('click', goPrev);
      }}

      async function submitAudit() {{
        const email = state.email.trim();
        if (!emailLooksCorporate(email)) {{
          state.error = 'Introduce un email corporativo valido.';
          render();
          return;
        }}

        state.error = '';
        state.submitting = true;
        goNext();

        const minWait = new Promise((resolve) => setTimeout(resolve, 2400));
        const request = fetch('/api/submit', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{ email, answers: state.answers }})
        }}).then(async (res) => {{
          const data = await res.json();
          if (!res.ok) {{
            throw new Error(data.error || 'No se pudo enviar');
          }}
          return data;
        }});

        try {{
          const [result] = await Promise.all([request, minWait]);
          state.responseId = result.response_id || '';
          state.submitting = false;
          goNext();
        }} catch (err) {{
          state.submitting = false;
          state.error = err.message || 'No se pudo completar el envio.';
          goTo(steps.findIndex((s) => s.kind === 'lead'));
        }}
      }}

      function renderLead() {{
        const lead = cfg.lead_capture;
        screenEl.innerHTML = `
          <article class="panel panel-animated narrow">
            <h1>${{escapeHtml(lead.title)}}</h1>
            <p class="helper">${{escapeHtml(lead.subtitle)}}</p>
            <label class="input-label">
              <input type="email" id="emailInput" class="email-input" value="${{escapeHtml(state.email)}}" placeholder="${{escapeHtml(lead.placeholder)}}" />
            </label>
            ${{state.error ? `<p class="error-text">${{escapeHtml(state.error)}}</p>` : ''}}
            <div class="actions-row">
              <button class="ghost-btn" id="prevBtn">Anterior</button>
              <button class="cta-btn" id="submitBtn">${{escapeHtml(lead.cta)}}</button>
            </div>
          </article>
        `;

        const emailInput = document.getElementById('emailInput');
        emailInput.addEventListener('input', (e) => {{
          state.email = e.target.value;
        }});
        emailInput.addEventListener('keydown', (e) => {{
          if (e.key === 'Enter') submitAudit();
        }});

        document.getElementById('submitBtn').addEventListener('click', submitAudit);
        document.getElementById('prevBtn').addEventListener('click', goPrev);
      }}

      function renderLoading() {{
        const messages = cfg.loading_messages;
        screenEl.innerHTML = `
          <article class="panel panel-animated loading-panel">
            <div class="spinner"></div>
            <h1>Procesando...</h1>
            <p class="helper" id="loadingMessage">${{escapeHtml(messages[0])}}</p>
          </article>
        `;

        let i = 0;
        clearInterval(loadingInterval);
        loadingInterval = setInterval(() => {{
          i = (i + 1) % messages.length;
          const node = document.getElementById('loadingMessage');
          if (node) node.textContent = messages[i];
        }}, 1000);
      }}

      function renderThanks() {{
        clearInterval(loadingInterval);
        const t = cfg.thank_you;
        screenEl.innerHTML = `
          <article class="panel panel-animated narrow">
            <h1>${{escapeHtml(t.title)}}</h1>
            <p class="helper">${{escapeHtml(t.message)}}</p>
            ${{state.responseId ? `<p class="meta-line">ID: ${{escapeHtml(state.responseId)}}</p>` : ''}}
            <a href="${{escapeHtml(cfg.calendly_url)}}" target="_blank" rel="noreferrer" class="cta-btn full-width">
              ${{escapeHtml(t.secondary_cta_text)}}
            </a>
          </article>
        `;
      }}

      function renderWelcome() {{
        const w = cfg.welcome;
        screenEl.innerHTML = `
          <article class="panel panel-animated welcome-panel">
            <p class="brand-line">${{escapeHtml(cfg.app_name)}}</p>
            <h1>${{escapeHtml(w.title)}}</h1>
            <p class="helper">${{escapeHtml(w.subtitle)}}</p>
            <button class="cta-btn" id="startBtn">${{escapeHtml(w.cta)}}</button>
          </article>
        `;

        document.getElementById('startBtn').addEventListener('click', goNext);
      }}

      function render() {{
        updateProgress();
        const step = steps[state.step];

        if (step.kind === 'welcome') renderWelcome();
        if (step.kind === 'question') renderQuestion(step);
        if (step.kind === 'lead') renderLead();
        if (step.kind === 'loading') renderLoading();
        if (step.kind === 'thanks') renderThanks();
      }}

      render();
    }})();
  </script>
</body>
</html>
"""
    return body.encode("utf-8")


def campaign_public_view(*, campaign_kind: str, campaign: dict) -> bytes:
    is_flash = campaign_kind == "flash"
    subtitle = (
        "Completa el Flash Audit para compararte en tiempo real con empresas de tu sector."
        if is_flash
        else "Responde el Barometro para construir el benchmark real de tu mercado."
    )
    welcome_title = campaign["name"]
    client_config = {
        "kind": campaign_kind,
        "slug": campaign["slug"],
        "campaign_name": campaign["name"],
        "welcome_title": welcome_title,
        "welcome_subtitle": subtitle,
        "questions": campaign["questions"],
        "loading_messages": CONFIG.get(
            "loading_messages",
            [
                "Cruzando tus datos con el barometro de tu sector...",
                "Calculando diferencias criticas...",
                "Montando tu dashboard directivo...",
            ],
        ),
        "cta_label": campaign.get(
            "cta_label",
            "Solucionar estas ineficiencias con GHC - Agendar Sesion",
        ),
        "cta_url": campaign.get("cta_url", CONFIG.get("calendly_url", "https://calendly.com")),
    }
    script_data = json.dumps(client_config, ensure_ascii=False)
    endpoint = f"/api/flash-audit/{campaign['slug']}/submit" if is_flash else f"/api/barometro/{campaign['slug']}/submit"

    body = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{esc(campaign['name'])}</title>
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body>
  <main class="quiz-shell quiz-shell-campaign">
    <div class="progress-wrap hidden" id="progressWrap">
      <div class="progress-head">
        <span id="progressLabel">Pregunta 1</span>
      </div>
      <div class="progress-track">
        <div class="progress-fill" id="progressFill"></div>
      </div>
    </div>
    <section class="screen" id="screen"></section>
  </main>
  <a id="floatingCta" class="floating-cta is-hidden" target="_blank" rel="noreferrer"></a>

  <script>
    window.CAMPAIGN_CFG = {script_data};
  </script>
  <script>
    (function () {{
      const cfg = window.CAMPAIGN_CFG;
      const isFlash = cfg.kind === "flash";
      const screenEl = document.getElementById("screen");
      const progressWrap = document.getElementById("progressWrap");
      const progressFill = document.getElementById("progressFill");
      const progressLabel = document.getElementById("progressLabel");
      const floatingCta = document.getElementById("floatingCta");

      const state = {{
        step: 0,
        answers: {{}},
        lead: {{ name: "", email: "", company: "" }},
        error: "",
        submitting: false,
        result: null
      }};

      const questions = cfg.questions || [];
      const steps = [{{ kind: "welcome" }}]
        .concat(questions.map((q, index) => ({{ kind: "question", question: q, qIndex: index + 1 }})))
        .concat(isFlash ? [{{ kind: "lead" }}, {{ kind: "loading" }}, {{ kind: "result" }}] : [{{ kind: "thanks" }}]);

      let loadingInterval = null;

      function escapeHtml(text) {{
        return String(text)
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }}

      function answerToText(value) {{
        if (Array.isArray(value)) return value.join(", ");
        if (value === null || value === undefined || value === "") return "-";
        return String(value);
      }}

      function showError(message) {{
        state.error = message || "";
      }}

      function goTo(index) {{
        state.step = Math.max(0, Math.min(steps.length - 1, index));
        render();
      }}

      function goNext() {{
        goTo(state.step + 1);
      }}

      function goPrev() {{
        goTo(state.step - 1);
      }}

      function currentStep() {{
        return steps[state.step] || null;
      }}

      function updateProgress() {{
        const step = currentStep();
        if (!step || step.kind !== "question") {{
          progressWrap.classList.add("hidden");
          return;
        }}
        progressWrap.classList.remove("hidden");
        progressLabel.textContent = `Pregunta ${{step.qIndex}} de ${{questions.length}}`;
        progressFill.style.width = `${{(step.qIndex / questions.length) * 100}}%`;
      }}

      function toggleMultiple(qid, option) {{
        const current = Array.isArray(state.answers[qid]) ? state.answers[qid].slice() : [];
        const index = current.indexOf(option);
        if (index >= 0) {{
          current.splice(index, 1);
        }} else {{
          current.push(option);
        }}
        state.answers[qid] = current;
        showError("");
        render();
      }}

      function selectSingle(qid, value, autoAdvance) {{
        state.answers[qid] = value;
        showError("");
        render();
        if (autoAdvance) window.setTimeout(goNext, 120);
      }}

      function renderSingle(question) {{
        const selected = state.answers[question.id] || "";
        const options = question.options || [];
        screenEl.innerHTML = `
          <article class="panel panel-animated">
            <p class="step-tag">${{escapeHtml((question.block || "diagnostico").replaceAll("_", " "))}}</p>
            <h1>${{escapeHtml(question.title)}}</h1>
            <p class="helper">${{escapeHtml(question.helper || "Selecciona una opcion para continuar.")}}</p>
            <div class="cards-grid">
              ${{options.map((opt) => `
                <button class="option-card ${{selected === opt ? "is-selected" : ""}}" data-value="${{escapeHtml(opt)}}">
                  <span>${{escapeHtml(opt)}}</span>
                </button>
              `).join("")}}
            </div>
            <div class="actions-row">
              <button class="ghost-btn" id="prevBtn">Anterior</button>
            </div>
          </article>
        `;
        screenEl.querySelectorAll(".option-card").forEach((btn) => {{
          btn.addEventListener("click", () => selectSingle(question.id, btn.dataset.value, true));
        }});
        document.getElementById("prevBtn").addEventListener("click", goPrev);
      }}

      function renderMultiple(question) {{
        const selected = Array.isArray(state.answers[question.id]) ? state.answers[question.id] : [];
        const options = question.options || [];
        screenEl.innerHTML = `
          <article class="panel panel-animated">
            <p class="step-tag">${{escapeHtml((question.block || "diagnostico").replaceAll("_", " "))}}</p>
            <h1>${{escapeHtml(question.title)}}</h1>
            <p class="helper">${{escapeHtml(question.helper || "Puedes seleccionar varias opciones.")}}</p>
            <div class="cards-grid">
              ${{options.map((opt) => `
                <button class="option-card ${{selected.includes(opt) ? "is-selected" : ""}}" data-value="${{escapeHtml(opt)}}">
                  <span>${{escapeHtml(opt)}}</span>
                </button>
              `).join("")}}
            </div>
            <div class="actions-row">
              <button class="ghost-btn" id="prevBtn">Anterior</button>
              <button class="cta-btn" id="continueBtn" ${{selected.length ? "" : "disabled"}}>Continuar</button>
            </div>
          </article>
        `;
        screenEl.querySelectorAll(".option-card").forEach((btn) => {{
          btn.addEventListener("click", () => toggleMultiple(question.id, btn.dataset.value));
        }});
        document.getElementById("prevBtn").addEventListener("click", goPrev);
        document.getElementById("continueBtn").addEventListener("click", goNext);
      }}

      function renderText(question) {{
        const value = state.answers[question.id] || "";
        screenEl.innerHTML = `
          <article class="panel panel-animated">
            <p class="step-tag">${{escapeHtml((question.block || "diagnostico").replaceAll("_", " "))}}</p>
            <h1>${{escapeHtml(question.title)}}</h1>
            <textarea id="textInput" class="campaign-textarea" placeholder="Escribe tu respuesta...">${{escapeHtml(value)}}</textarea>
            <div class="actions-row">
              <button class="ghost-btn" id="prevBtn">Anterior</button>
              <button class="cta-btn" id="continueBtn" ${{value.trim() ? "" : "disabled"}}>Continuar</button>
            </div>
          </article>
        `;
        const input = document.getElementById("textInput");
        input.addEventListener("input", (event) => {{
          state.answers[question.id] = event.target.value;
          showError("");
          render();
        }});
        document.getElementById("prevBtn").addEventListener("click", goPrev);
        document.getElementById("continueBtn").addEventListener("click", goNext);
      }}

      function renderScale(question) {{
        const options = question.options || [];
        if (options.length) {{
          renderSingle(question);
          return;
        }}

        const value = state.answers[question.id] || "";
        const min = Number(question.min || 1);
        const max = Number(question.max || 5);
        screenEl.innerHTML = `
          <article class="panel panel-animated">
            <p class="step-tag">${{escapeHtml((question.block || "diagnostico").replaceAll("_", " "))}}</p>
            <h1>${{escapeHtml(question.title)}}</h1>
            <label class="input-label">
              <input class="email-input" type="number" min="${{min}}" max="${{max}}" step="0.1" id="scaleInput" value="${{escapeHtml(value)}}" />
            </label>
            <p class="helper">Rango permitido: ${{min}} a ${{max}}.</p>
            <div class="actions-row">
              <button class="ghost-btn" id="prevBtn">Anterior</button>
              <button class="cta-btn" id="continueBtn" ${{String(value).trim() ? "" : "disabled"}}>Continuar</button>
            </div>
          </article>
        `;
        const input = document.getElementById("scaleInput");
        input.addEventListener("input", (event) => {{
          state.answers[question.id] = event.target.value;
          showError("");
          render();
        }});
        document.getElementById("prevBtn").addEventListener("click", goPrev);
        document.getElementById("continueBtn").addEventListener("click", goNext);
      }}

      function renderQuestion(step) {{
        const q = step.question;
        if (q.type === "multiple") return renderMultiple(q);
        if (q.type === "text") return renderText(q);
        if (q.type === "scale") return renderScale(q);
        return renderSingle(q);
      }}

      function renderLead() {{
        screenEl.innerHTML = `
          <article class="panel panel-animated narrow">
            <h1>Recibe tu dashboard comparativo</h1>
            <p class="helper">Necesitamos tus datos para enviarte el informe y activar el seguimiento consultivo.</p>
            <label class="input-label">
              <input class="email-input" id="leadName" type="text" placeholder="Nombre y apellidos" value="${{escapeHtml(state.lead.name)}}" />
            </label>
            <label class="input-label">
              <input class="email-input" id="leadCompany" type="text" placeholder="Empresa" value="${{escapeHtml(state.lead.company)}}" />
            </label>
            <label class="input-label">
              <input class="email-input" id="leadEmail" type="email" placeholder="Email corporativo" value="${{escapeHtml(state.lead.email)}}" />
            </label>
            ${{state.error ? `<p class="error-text">${{escapeHtml(state.error)}}</p>` : ""}}
            <div class="actions-row">
              <button class="ghost-btn" id="prevBtn">Anterior</button>
              <button class="cta-btn" id="submitBtn">Generar diagnostico</button>
            </div>
          </article>
        `;
        const nameInput = document.getElementById("leadName");
        const companyInput = document.getElementById("leadCompany");
        const emailInput = document.getElementById("leadEmail");
        nameInput.addEventListener("input", (event) => (state.lead.name = event.target.value));
        companyInput.addEventListener("input", (event) => (state.lead.company = event.target.value));
        emailInput.addEventListener("input", (event) => (state.lead.email = event.target.value));
        document.getElementById("prevBtn").addEventListener("click", goPrev);
        document.getElementById("submitBtn").addEventListener("click", submitCampaign);
      }}

      function renderLoading() {{
        const messages = cfg.loading_messages || ["Procesando..."];
        screenEl.innerHTML = `
          <article class="panel panel-animated loading-panel">
            <div class="spinner"></div>
            <h1>Cruzando tus datos...</h1>
            <p class="helper" id="loadingMessage">${{escapeHtml(messages[0])}}</p>
          </article>
        `;
        let idx = 0;
        clearInterval(loadingInterval);
        loadingInterval = setInterval(() => {{
          idx = (idx + 1) % messages.length;
          const node = document.getElementById("loadingMessage");
          if (node) node.textContent = messages[idx];
        }}, 1000);
      }}

      function statusChip(status) {{
        if (status === "green") return "Mejor que media";
        if (status === "yellow") return "Igual que media";
        if (status === "red") return "Peor que media";
        return "Sin comparativa";
      }}

      function renderResult() {{
        clearInterval(loadingInterval);
        const result = state.result || {{}};
        const analysis = result.analysis || {{}};
        const segment = result.segment || {{}};
        const findings = analysis.findings || [];
        const segmentLabel = [segment.sector, segment.tamano_empresa].filter(Boolean).join(" · ") || "Tu segmento";

        floatingCta.textContent = cfg.cta_label;
        floatingCta.href = cfg.cta_url;
        floatingCta.classList.remove("is-hidden");

        screenEl.innerHTML = `
          <article class="panel panel-animated dashboard-panel">
            <p class="step-tag">Resultado en tiempo real</p>
            <h1>Tu diagnostico directivo vs. ${{escapeHtml(segmentLabel)}}</h1>
            <div class="dashboard-kpis">
              <div><p>Score global</p><strong>${{escapeHtml(analysis.overall_score || 0)}}</strong></div>
              <div><p>Alertas rojas</p><strong>${{escapeHtml(analysis.red_alerts || 0)}}</strong></div>
              <div><p>Alertas amarillas</p><strong>${{escapeHtml(analysis.yellow_alerts || 0)}}</strong></div>
            </div>
            <div class="dashboard-grid">
              ${{findings.map((item) => `
                <section class="dashboard-card status-${{escapeHtml(item.status || "neutral")}}">
                  <h3>${{escapeHtml(item.label || "-")}}</h3>
                  <p class="dashboard-line"><span>Tu dato:</span><strong>${{escapeHtml(answerToText(item.user_value))}}</strong></p>
                  <p class="dashboard-line"><span>Media sector:</span><strong>${{escapeHtml(answerToText(item.benchmark_value))}}</strong></p>
                  <p class="dashboard-status">${{escapeHtml(statusChip(item.status))}}</p>
                </section>
              `).join("")}}
            </div>
            <p class="meta-line">Informe disponible tambien en: <a href="${{escapeHtml(result.result_url || "#")}}" target="_blank" rel="noreferrer">${{escapeHtml(result.result_url || "-")}}</a></p>
          </article>
        `;
      }}

      function renderThanks() {{
        clearInterval(loadingInterval);
        screenEl.innerHTML = `
          <article class="panel panel-animated narrow">
            <h1>Gracias por participar</h1>
            <p class="helper">Tu respuesta al barometro se ha registrado correctamente.</p>
            <a class="cta-btn full-width" href="/login">Volver</a>
          </article>
        `;
      }}

      function renderWelcome() {{
        floatingCta.classList.add("is-hidden");
        screenEl.innerHTML = `
          <article class="panel panel-animated welcome-panel">
            <p class="brand-line">${{escapeHtml(cfg.campaign_name)}}</p>
            <h1>${{escapeHtml(cfg.welcome_title)}}</h1>
            <p class="helper">${{escapeHtml(cfg.welcome_subtitle)}}</p>
            <button id="startBtn" class="cta-btn">Comenzar</button>
          </article>
        `;
        document.getElementById("startBtn").addEventListener("click", goNext);
      }}

      async function submitCampaign() {{
        showError("");
        const payload = {{ answers: state.answers }};
        if (isFlash) {{
          payload.lead = state.lead;
          if (!state.lead.name.trim() || !state.lead.company.trim() || !state.lead.email.trim()) {{
            showError("Completa nombre, empresa y email corporativo.");
            render();
            return;
          }}
        }}

        state.submitting = true;
        if (isFlash) goTo(steps.findIndex((s) => s.kind === "loading"));

        const wait = new Promise((resolve) => setTimeout(resolve, 2400));
        const request = fetch("{endpoint}", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(payload)
        }}).then(async (res) => {{
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "No se pudo completar");
          return data;
        }});

        try {{
          const result = isFlash ? (await Promise.all([request, wait]))[0] : await request;
          state.result = result;
          state.submitting = false;
          if (isFlash) {{
            goTo(steps.findIndex((s) => s.kind === "result"));
          }} else {{
            goTo(steps.findIndex((s) => s.kind === "thanks"));
          }}
        }} catch (error) {{
          state.submitting = false;
          showError(error.message || "No se pudo completar");
          if (isFlash) {{
            goTo(steps.findIndex((s) => s.kind === "lead"));
          }} else {{
            goTo(steps.findIndex((s) => s.kind === "question"));
          }}
        }}
      }}

      function render() {{
        updateProgress();
        const step = currentStep();
        if (!step) return;
        if (step.kind === "welcome") return renderWelcome();
        if (step.kind === "question") return renderQuestion(step);
        if (step.kind === "lead") return renderLead();
        if (step.kind === "loading") return renderLoading();
        if (step.kind === "result") return renderResult();
        if (step.kind === "thanks") return renderThanks();
      }}

      render();
    }})();
  </script>
</body>
</html>
"""
    return body.encode("utf-8")


def flash_result_view(lead_id: str) -> bytes:
    lead = load_flash_lead(lead_id)
    if not lead:
        return base_layout(
            "Resultado no encontrado",
            "",
            '<section class="panel"><p>No existe ese resultado de Flash Audit.</p></section>',
        )

    analysis = lead["analysis"] if isinstance(lead["analysis"], dict) else {}
    findings = analysis.get("findings", []) if isinstance(analysis, dict) else []
    rows = []
    for item in findings:
        if not isinstance(item, dict):
            continue
        rows.append(
            f"""
            <tr>
              <td>{esc(str(item.get('label', '-')))}</td>
              <td>{esc(answer_to_text(item.get('user_value')))}</td>
              <td>{esc(answer_to_text(item.get('benchmark_value')))}</td>
              <td>{esc(str(item.get('status', 'neutral')))}</td>
            </tr>
            """
        )

    body = f"""
    <section class="admin-kpis">
      <article><p>Lead</p><h3>{esc(str(lead.get('lead_name', '-')))}</h3></article>
      <article><p>Empresa</p><h3>{esc(str(lead.get('lead_company', '-')))}</h3></article>
      <article><p>Score</p><h3>{float(analysis.get('overall_score', 0)):.1f}</h3></article>
      <article><p>Alertas rojas</p><h3>{int(analysis.get('red_alerts', 0))}</h3></article>
    </section>

    <section class="panel">
      <h2>Diagnostico comparativo</h2>
      <p>Segmento: {esc(str(lead.get('sector', '-')))} | {esc(str(lead.get('tamano_empresa', '-')))} | {esc(str(lead.get('facturacion_2024', '-')))}</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>Metrica</th><th>Tu dato</th><th>Media sector</th><th>Semaforo</th></tr>
          </thead>
          <tbody>
            {''.join(rows) if rows else '<tr><td colspan="4">Sin comparativas disponibles</td></tr>'}
          </tbody>
        </table>
      </div>
    </section>
    """
    return base_layout(
        "Resultado Flash Audit",
        "Comparativa automatica contra el Barometro vinculado",
        body,
    )


def login_view(error: str = "", email: str = "") -> bytes:
    error_box = f'<p class="auth-error">{esc(error)}</p>' if error else ""
    doc = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{esc(CONFIG['app_name'])} | Login</title>
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body class="auth-page">
  <main class="auth-shell">
    <section class="auth-brand">
      <div class="auth-brand-inner">
        <h1 class="auth-logo">GHC</h1>
        <span class="auth-divider"></span>
        <h2 class="auth-brand-title">Market Intelligence</h2>
        <p class="auth-brand-copy">
          Plataforma exclusiva de conocimiento e inteligencia de mercado.
          Acceso restringido a consultores autorizados.
        </p>
      </div>
    </section>
    <section class="auth-login">
      <div class="auth-card">
        <h2>Bienvenido</h2>
        <p class="auth-subtitle">Inicia sesion en tu cuenta</p>
        {error_box}
        <form method="post" action="/admin/login" class="auth-form">
          <label class="auth-field">
            <span class="auth-label">Correo corporativo</span>
            <input
              type="email"
              name="email"
              value="{esc(email)}"
              placeholder="ejemplo@ghc.com"
              required
            />
          </label>
          <div class="auth-password-head">
            <span class="auth-label">Contrasena</span>
            <a class="auth-forgot" href="#" onclick="return false;">¿Olvidaste tu contrasena?</a>
          </div>
          <label class="auth-field">
            <input type="password" name="password" placeholder="........" required />
          </label>
          <button class="auth-submit" type="submit">Entrar al sistema</button>
        </form>
        <p class="auth-foot">GHC • Sistema de Seguridad v2.0</p>
      </div>
    </section>
  </main>
</body>
</html>
"""
    return doc.encode("utf-8")


def interior_app_view(module_slug: str, user_email: str = "") -> bytes:
    modules = {
        "inside-scope": {
            "label": "Inside Scope",
            "subtitle": "Captura de Market Intelligence cualitativa y salarial tras entrevistas con candidatos.",
            "kpis": {"pending": 3, "done": 3, "total": 6},
            "rows": [
                {
                    "origin": "INSIDE",
                    "status": "PLANIFICADO",
                    "date": "13 mar 2026, 15:45",
                    "project": "Consultoria de RH 2026",
                    "subject": "Kick-off coaching Isaac",
                    "owner": "Guillermo Cornet",
                },
                {
                    "origin": "INSIDE",
                    "status": "PLANIFICADO",
                    "date": "13 mar 2026, 14:30",
                    "project": "Liderazgo sin Rodeos (Feb26)",
                    "subject": "M5: Inteligencia Emocional",
                    "owner": "Guillermo Cornet",
                },
                {
                    "origin": "INSIDE",
                    "status": "PLANIFICADO",
                    "date": "12 mar 2026, 11:00",
                    "project": "Liderazgo sin Rodeos (Feb26)",
                    "subject": "Preparacion M6",
                    "owner": "Guillermo Cornet",
                },
            ],
        },
        "lead-engine": {
            "label": "Lead Engine",
            "subtitle": "Pipeline comercial para captacion, calificacion y activacion de oportunidades.",
            "kpis": {"pending": 5, "done": 2, "total": 7},
            "rows": [
                {
                    "origin": "LEAD",
                    "status": "NUEVO",
                    "date": "13 mar 2026, 16:10",
                    "project": "Expansion Iberia 2026",
                    "subject": "Discovery call con prospect A",
                    "owner": "Guillermo Cornet",
                },
                {
                    "origin": "LEAD",
                    "status": "EN CURSO",
                    "date": "13 mar 2026, 13:20",
                    "project": "Programa Executive Search",
                    "subject": "Validacion de fit con cliente B",
                    "owner": "Guillermo Cornet",
                },
                {
                    "origin": "LEAD",
                    "status": "NUEVO",
                    "date": "12 mar 2026, 10:05",
                    "project": "Advisory Board Launch",
                    "subject": "Enviar propuesta comercial",
                    "owner": "Guillermo Cornet",
                },
            ],
        },
    }

    module_slug = module_slug if module_slug in modules else "inside-scope"
    module_data = modules[module_slug]
    user_name = consultant_display_name(user_email)
    user_email_line = user_email.strip().lower() if user_email else ""
    lead_rows, lead_stats = load_flash_leads_summary(limit=120)

    table_rows = []
    if module_slug == "lead-engine":
        for row in lead_rows:
            analysis = json_loads_or_default(row["analysis_json"], {})
            findings = analysis.get("findings", []) if isinstance(analysis, dict) else []
            red_alerts = []
            if isinstance(findings, list):
                for item in findings:
                    if isinstance(item, dict) and item.get("status") == "red":
                        label = str(item.get("label", "")).strip()
                        if label:
                            red_alerts.append(label)
            red_preview = ", ".join(red_alerts[:2]) if red_alerts else "-"
            status_label = "ALTA PRIORIDAD" if int(row["red_alerts"]) > 0 else "NUEVO"
            table_rows.append(
                f"""
                <tr>
                  <td><span class="workspace-origin">LEAD</span></td>
                  <td><span class="workspace-status">{esc(status_label)}</span></td>
                  <td>{esc(row['created_at'])}</td>
                  <td>{esc(row['flash_name'])}</td>
                  <td>{esc(row['lead_name'] or '-')} · {esc(row['lead_company'] or '-')}</td>
                  <td>{esc(red_preview)}</td>
                  <td><a class="workspace-open" href="/flash-audit/resultado/{esc(row['id'])}" target="_blank" rel="noreferrer">Abrir</a></td>
                </tr>
                """
            )
    else:
        for item in module_data["rows"]:
            table_rows.append(
                f"""
                <tr>
                  <td><span class="workspace-origin">{esc(item['origin'])}</span></td>
                  <td><span class="workspace-status">{esc(item['status'])}</span></td>
                  <td>{esc(item['date'])}</td>
                  <td>{esc(item['project'])}</td>
                  <td>{esc(item['subject'])}</td>
                  <td>{esc(item['owner'])}</td>
                  <td><a class="workspace-open" href="#">Abrir</a></td>
                </tr>
                """
            )

    module_links = []
    for slug, info in modules.items():
        active = " is-active" if slug == module_slug else ""
        module_links.append(
            f'<a class="workspace-nav-item{active}" href="/app/{esc(slug)}">{esc(info["label"])}</a>'
        )

    lead_engine_content = f"""
      <section class="workspace-filters">
        <label class="workspace-search">
          <span>Buscar</span>
          <input type="text" placeholder="Filtrado rapido (visual)." />
        </label>
        <select class="workspace-select">
          <option>Leads ({lead_stats['total']})</option>
        </select>
        <select class="workspace-select">
          <option>Rojas acumuladas: {lead_stats['red_total']}</option>
        </select>
        <select class="workspace-select">
          <option>Score medio: {lead_stats['avg_score']}</option>
        </select>
        <select class="workspace-select">
          <option>Campanas activas: {len(list_campaigns().get('flash_audits', []))}</option>
        </select>
        <a class="workspace-refresh" href="/admin/leads" title="Abrir CRM">↗</a>
      </section>

      <section class="workspace-table-wrap">
        <table class="workspace-table">
          <thead>
            <tr>
              <th>Origen</th>
              <th>Estado</th>
              <th>Fecha</th>
              <th>Campana</th>
              <th>Lead</th>
              <th>Alertas Rojas</th>
              <th>Abrir</th>
            </tr>
          </thead>
          <tbody>
            {''.join(table_rows) if table_rows else '<tr><td colspan="7">Sin leads registrados</td></tr>'}
          </tbody>
        </table>
      </section>
    """

    inside_scope_content = """
      <section class="inside-hub">
        <button class="inside-record-btn" id="recordBtn" type="button">
          <span class="inside-record-icon">🎤</span>
          <span class="inside-record-label" id="recordLabel">Grabar Insight</span>
          <span class="inside-record-timer" id="recordTimer">00:00</span>
        </button>

        <div class="inside-wave is-hidden" id="insideWave" aria-hidden="true">
          <span></span><span></span><span></span><span></span><span></span>
        </div>

        <button class="inside-fallback-btn" id="pasteBtn" type="button">Pegar texto</button>

        <p class="inside-status" id="insideStatus">Listo para capturar inteligencia de mercado.</p>

        <div class="inside-processing is-hidden" id="insideProcessing">
          <span class="inside-spinner" aria-hidden="true"></span>
          <span>Procesando Inteligencia de Mercado...</span>
        </div>

        <p class="inside-success is-hidden" id="insideSuccess">✓ Insight guardado correctamente.</p>
        <p class="inside-error is-hidden" id="insideError"></p>
      </section>

      <div class="inside-modal is-hidden" id="insideModal" role="dialog" aria-modal="true" aria-labelledby="insideModalTitle">
        <div class="inside-modal-card">
          <h2 id="insideModalTitle">Pegar texto</h2>
          <textarea id="insideText" placeholder="Pega aqui tus notas de entrevista..."></textarea>
          <div class="inside-modal-actions">
            <button type="button" class="ghost-btn" id="insideCancelBtn">Cancelar</button>
            <button type="button" class="cta-btn" id="insideSubmitTextBtn">Guardar insight</button>
          </div>
        </div>
      </div>
    """

    inside_scope_script = """
  <script>
    (function () {
      const recordBtn = document.getElementById("recordBtn");
      if (!recordBtn) return;

      const pasteBtn = document.getElementById("pasteBtn");
      const statusNode = document.getElementById("insideStatus");
      const processingNode = document.getElementById("insideProcessing");
      const successNode = document.getElementById("insideSuccess");
      const errorNode = document.getElementById("insideError");
      const recordLabel = document.getElementById("recordLabel");
      const recordTimer = document.getElementById("recordTimer");
      const waveNode = document.getElementById("insideWave");
      const modal = document.getElementById("insideModal");
      const textArea = document.getElementById("insideText");
      const cancelBtn = document.getElementById("insideCancelBtn");
      const submitTextBtn = document.getElementById("insideSubmitTextBtn");

      let mediaRecorder = null;
      let activeStream = null;
      let audioChunks = [];
      let timerInterval = null;
      let startedAt = 0;

      function setStatus(message) {
        statusNode.textContent = message;
      }

      function setProcessing(show) {
        processingNode.classList.toggle("is-hidden", !show);
      }

      function setSuccess(show) {
        successNode.classList.toggle("is-hidden", !show);
      }

      function setWave(show) {
        waveNode.classList.toggle("is-hidden", !show);
      }

      function setError(message) {
        if (!message) {
          errorNode.classList.add("is-hidden");
          errorNode.textContent = "";
          return;
        }
        errorNode.textContent = message;
        errorNode.classList.remove("is-hidden");
      }

      function formatElapsed(ms) {
        const total = Math.max(0, Math.floor(ms / 1000));
        const mm = String(Math.floor(total / 60)).padStart(2, "0");
        const ss = String(total % 60).padStart(2, "0");
        return mm + ":" + ss;
      }

      function resetTimer() {
        if (timerInterval) window.clearInterval(timerInterval);
        timerInterval = null;
        recordTimer.textContent = "00:00";
      }

      function stopStream() {
        if (!activeStream) return;
        activeStream.getTracks().forEach((track) => track.stop());
        activeStream = null;
      }

      function openModal() {
        modal.classList.remove("is-hidden");
        textArea.focus();
      }

      function closeModal() {
        modal.classList.add("is-hidden");
      }

      async function submitInsight(formData) {
        setError("");
        setSuccess(false);
        setProcessing(true);
        setStatus("Procesando Inteligencia de Mercado...");
        recordBtn.disabled = true;
        pasteBtn.disabled = true;

        try {
          const response = await fetch("/api/inside-scope/intake", {
            method: "POST",
            body: formData
          });
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data.error || "No se pudo procesar el insight");
          }

          setStatus("Insight guardado para " + (data.empresa || "empresa no identificada") + ".");
          setSuccess(true);
          window.setTimeout(() => setSuccess(false), 2500);
        } catch (err) {
          setStatus("No se pudo completar la carga.");
          setError(err.message || "Error inesperado");
        } finally {
          setProcessing(false);
          recordBtn.disabled = false;
          pasteBtn.disabled = false;
        }
      }

      async function startRecording() {
        if (!window.MediaRecorder || !navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
          setError("Tu navegador no soporta grabacion de audio.");
          return;
        }

        setError("");
        setSuccess(false);

        try {
          activeStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        } catch (err) {
          setError("No se concedio permiso de microfono.");
          return;
        }

        audioChunks = [];
        mediaRecorder = new MediaRecorder(activeStream);
        mediaRecorder.ondataavailable = (event) => {
          if (event.data && event.data.size > 0) audioChunks.push(event.data);
        };

        mediaRecorder.onstop = async () => {
          stopStream();
          resetTimer();
          setWave(false);
          recordBtn.classList.remove("is-recording");
          recordLabel.textContent = "Grabar Insight";
          setStatus("Audio capturado. Iniciando procesamiento...");

          const blob = new Blob(audioChunks, { type: "audio/webm" });
          const formData = new FormData();
          formData.append("mode", "audio");
          formData.append("audio", blob, "inside-scope.webm");
          await submitInsight(formData);
        };

        startedAt = Date.now();
        recordBtn.classList.add("is-recording");
        recordLabel.textContent = "Detener";
        setWave(true);
        setStatus("Grabando insight...");
        timerInterval = window.setInterval(() => {
          recordTimer.textContent = formatElapsed(Date.now() - startedAt);
        }, 250);

        mediaRecorder.start();
      }

      function stopRecording() {
        if (!mediaRecorder) return;
        if (mediaRecorder.state === "recording") {
          mediaRecorder.stop();
        }
        setWave(false);
        mediaRecorder = null;
      }

      recordBtn.addEventListener("click", async () => {
        if (recordBtn.classList.contains("is-recording")) {
          stopRecording();
          return;
        }
        await startRecording();
      });

      pasteBtn.addEventListener("click", openModal);
      cancelBtn.addEventListener("click", closeModal);
      modal.addEventListener("click", (event) => {
        if (event.target === modal) closeModal();
      });

      submitTextBtn.addEventListener("click", async () => {
        const text = textArea.value.trim();
        if (!text) {
          setError("Pega un texto antes de guardar.");
          return;
        }
        closeModal();
        const formData = new FormData();
        formData.append("mode", "text");
        formData.append("text", text);
        await submitInsight(formData);
        textArea.value = "";
      });
    })();
  </script>
    """

    module_content = inside_scope_content if module_slug == "inside-scope" else lead_engine_content
    module_script = inside_scope_script if module_slug == "inside-scope" else ""
    module_kpis = ""

    doc = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="theme-color" content="#0D1B2A" />
  <title>{esc(CONFIG['app_name'])} | {esc(module_data['label'])}</title>
  <link rel="stylesheet" href="/static/style.css" />
  <link rel="manifest" href="/static/manifest.webmanifest" />
</head>
<body class="workspace-page">
  <main class="workspace-shell">
    <aside class="workspace-sidebar">
      <div class="workspace-brand">
        <p class="workspace-logo">GHC</p>
        <p class="workspace-logo-sub">Market Intelligence</p>
      </div>
      <nav class="workspace-nav">
        {''.join(module_links)}
      </nav>
      <div class="workspace-user">
        <p class="workspace-user-name">{esc(user_name)}</p>
        <p class="workspace-user-email">{esc(user_email_line or 'consultor@globalhumancon.com')}</p>
        <a class="workspace-logout" href="/admin/logout">Cerrar sesion</a>
      </div>
    </aside>

    <section class="workspace-main">
      <header class="workspace-head">
        <div>
          <h1>{esc(module_data['label'])}</h1>
          <p>{esc(module_data['subtitle'])}</p>
        </div>
        {module_kpis}
      </header>

      {module_content}
    </section>
  </main>
  <script>
    if ("serviceWorker" in navigator) {{
      window.addEventListener("load", function () {{
        navigator.serviceWorker.register("/static/sw.js").catch(function () {{}});
      }});
    }}
  </script>
  {module_script}
</body>
</html>
"""
    return doc.encode("utf-8")


def load_admin_rows() -> tuple[list[sqlite3.Row], dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM audit_responses
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (MAX_ADMIN_ROWS,),
        ).fetchall()

    total = len(rows)
    avg_score = round(sum(float(r["overall_score"]) for r in rows) / total, 1) if total else 0.0
    red_total = sum(int(r["red_alerts"]) for r in rows)
    sent_total = sum(1 for r in rows if r["delivery_status"] == "sent")

    return rows, {
        "total": total,
        "avg_score": avg_score,
        "red_total": red_total,
        "sent_total": sent_total,
        "last_at": rows[0]["created_at"] if rows else "-",
    }


def admin_dashboard(rows: list[sqlite3.Row], stats: dict) -> bytes:
    line_items = []
    for row in rows:
        segment = f"{row['facturacion_2024']} | {row['tamano_empresa']} | {row['sector']}"
        line_items.append(
            f"""
            <tr>
              <td>{esc(row['created_at'])}</td>
              <td>{esc(row['email'])}</td>
              <td>{esc(segment)}</td>
              <td>{float(row['overall_score']):.1f}</td>
              <td>{int(row['red_alerts'])}/{int(row['yellow_alerts'])}</td>
              <td>{esc(row['delivery_status'])}</td>
              <td><a href="/admin/response?id={esc(row['id'])}">Ver</a></td>
            </tr>
            """
        )

    body = f"""
    <section class="admin-kpis">
      <article><p>Total respuestas</p><h3>{stats['total']}</h3></article>
      <article><p>Score medio</p><h3>{stats['avg_score']}</h3></article>
      <article><p>Alertas rojas</p><h3>{stats['red_total']}</h3></article>
      <article><p>Emails enviados</p><h3>{stats['sent_total']}</h3></article>
    </section>

    <section class="admin-actions">
      <a class="ghost-btn" href="/admin/export.csv">Descargar CSV</a>
      <a class="ghost-btn" href="/admin/leads">Lead Engine CRM</a>
      <a class="ghost-btn" href="/admin/logout">Cerrar sesion</a>
      <p class="meta-line">Ultima respuesta: {esc(stats['last_at'])}</p>
    </section>

    <section class="panel">
      <h2>Respuestas</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Fecha</th><th>Email</th><th>Segmento</th><th>Score</th><th>R/Y</th><th>Delivery</th><th>Detalle</th>
            </tr>
          </thead>
          <tbody>
            {''.join(line_items) if line_items else '<tr><td colspan="7">Sin respuestas</td></tr>'}
          </tbody>
        </table>
      </div>
    </section>
    """

    return base_layout("Panel administrador", "Lectura agregada y exportable", body)


def admin_leads_dashboard() -> bytes:
    rows, stats = load_flash_leads_summary(limit=300)
    line_items = []
    for row in rows:
        analysis = json_loads_or_default(row["analysis_json"], {})
        findings = []
        if isinstance(analysis, dict):
            findings = analysis.get("findings", [])
        red_findings: list[str] = []
        if isinstance(findings, list):
            for item in findings:
                if isinstance(item, dict) and item.get("status") == "red":
                    label = str(item.get("label", "")).strip()
                    if label:
                        red_findings.append(label)
        red_preview = " | ".join(red_findings[:2]) if red_findings else "-"

        segment = " | ".join(
            filter(
                None,
                [
                    str(row["sector"] or "").strip(),
                    str(row["tamano_empresa"] or "").strip(),
                    str(row["facturacion_2024"] or "").strip(),
                ],
            )
        )
        line_items.append(
            f"""
            <tr>
              <td>{esc(row['created_at'])}</td>
              <td>{esc(row['flash_name'])}</td>
              <td>{esc(row['lead_name'] or '-')}</td>
              <td>{esc(row['lead_email'])}</td>
              <td>{esc(row['lead_company'] or '-')}</td>
              <td>{esc(segment or '-')}</td>
              <td>{float(row['overall_score']):.1f}</td>
              <td>{int(row['red_alerts'])}/{int(row['yellow_alerts'])}</td>
              <td>{esc(red_preview)}</td>
              <td><a href="/flash-audit/resultado/{esc(row['id'])}" target="_blank" rel="noreferrer">Ver</a></td>
            </tr>
            """
        )

    body = f"""
    <section class="admin-kpis">
      <article><p>Total leads</p><h3>{stats['total']}</h3></article>
      <article><p>Score medio</p><h3>{stats['avg_score']}</h3></article>
      <article><p>Alertas rojas</p><h3>{stats['red_total']}</h3></article>
      <article><p>Campanas activas</p><h3>{len(list_campaigns().get('flash_audits', []))}</h3></article>
    </section>

    <section class="admin-actions">
      <a class="ghost-btn" href="/admin">Volver al panel</a>
      <a class="ghost-btn" href="/app/lead-engine">Lead Engine</a>
    </section>

    <section class="panel">
      <h2>Leads Flash Audit</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Fecha</th>
              <th>Campana</th>
              <th>Nombre</th>
              <th>Email</th>
              <th>Empresa</th>
              <th>Segmento</th>
              <th>Score</th>
              <th>R/Y</th>
              <th>Alertas rojas</th>
              <th>Resultado</th>
            </tr>
          </thead>
          <tbody>
            {''.join(line_items) if line_items else '<tr><td colspan="10">Sin leads registrados</td></tr>'}
          </tbody>
        </table>
      </div>
    </section>
    """

    return base_layout("Lead Engine CRM", "Directivos captados por Flash Audits y sus alertas rojas", body)


def admin_detail(response_id: str) -> bytes:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT *
            FROM audit_responses
            WHERE id = ?
            """,
            (response_id,),
        ).fetchone()

    if not row:
        return base_layout(
            "No encontrado",
            "",
            '<section class="panel"><p>No existe esa respuesta.</p><a href="/admin">Volver</a></section>',
        )

    answers = json.loads(row["answers_json"])
    benchmark = json.loads(row["benchmark_json"])
    analysis = json.loads(row["analysis_json"])

    finding_rows = []
    for item in analysis.get("findings", []):
        finding_rows.append(
            f"""
            <tr>
              <td>{esc(item['question'])}</td>
              <td>{esc(item['user'])}</td>
              <td>{esc(item['benchmark'])}</td>
              <td>{esc(item['status'])}</td>
            </tr>
            """
        )

    qa_rows = []
    for q in QUESTIONS:
        qid = q["id"]
        qa_rows.append(
            f"""
            <tr>
              <td>{esc(q['title'])}</td>
              <td>{esc(answers.get(qid, '-'))}</td>
              <td>{esc(benchmark.get(qid, '-'))}</td>
            </tr>
            """
        )

    body = f"""
    <section class="admin-actions">
      <a class="ghost-btn" href="/admin">Volver al panel</a>
    </section>

    <section class="panel">
      <h2>Respuesta {esc(row['id'])}</h2>
      <p>Email: {esc(row['email'])}</p>
      <p>Segmento: {esc(row['facturacion_2024'])} | {esc(row['tamano_empresa'])} | {esc(row['sector'])}</p>
      <p>Score: {float(row['overall_score']):.1f} (benchmark {float(row['overall_benchmark']):.1f})</p>
      <p>Delivery: {esc(row['delivery_status'])} {esc(row['delivery_error'] or '')}</p>
    </section>

    <section class="panel">
      <h2>Comparativa detallada</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>Indicador</th><th>Empresa</th><th>Benchmark</th><th>Estado</th></tr>
          </thead>
          <tbody>{''.join(finding_rows)}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>Respuestas crudas</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>Pregunta</th><th>Respuesta</th><th>Benchmark</th></tr>
          </thead>
          <tbody>{''.join(qa_rows)}</tbody>
        </table>
      </div>
    </section>
    """

    return base_layout("Detalle de respuesta", "Inspeccion para equipo admin", body)


def export_csv(handler: BaseHTTPRequestHandler) -> None:
    if not is_admin_authenticated(handler):
        redirect_response(handler, "/login")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM audit_responses ORDER BY created_at DESC
            """
        ).fetchall()

    headers = [
        "id",
        "created_at",
        "email",
        "facturacion_2024",
        "tamano_empresa",
        "sector",
        "overall_score",
        "overall_benchmark",
        "red_alerts",
        "yellow_alerts",
        "delivery_status",
        "delivery_error",
    ] + [q["id"] for q in QUESTIONS] + [f"benchmark_{q['id']}" for q in QUESTIONS]

    out = StringIO()
    writer = csv.writer(out)
    writer.writerow(headers)

    for row in rows:
        answers = json.loads(row["answers_json"])
        benchmark = json.loads(row["benchmark_json"])
        writer.writerow(
            [
                row["id"],
                row["created_at"],
                row["email"],
                row["facturacion_2024"],
                row["tamano_empresa"],
                row["sector"],
                row["overall_score"],
                row["overall_benchmark"],
                row["red_alerts"],
                row["yellow_alerts"],
                row["delivery_status"],
                row["delivery_error"],
            ]
            + [answers.get(q["id"], "") for q in QUESTIONS]
            + [benchmark.get(q["id"], "") for q in QUESTIONS]
        )

    data = out.getvalue().encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/csv; charset=utf-8")
    handler.send_header("Content-Disposition", 'attachment; filename="audit_responses.csv"')
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def admin_create_barometer(payload: dict) -> tuple[dict, int]:
    name = str(payload.get("name", "")).strip()
    if not name:
        return {"ok": False, "error": "name es obligatorio"}, 400

    try:
        year = int(payload.get("year", datetime.now().year))
    except (TypeError, ValueError):
        return {"ok": False, "error": "year invalido"}, 400

    questions, error = normalize_campaign_questions(payload.get("questions"))
    if error:
        return {"ok": False, "error": error}, 400

    raw_slug = str(payload.get("slug", "")).strip()
    slug = slugify(raw_slug or name)
    description = str(payload.get("description", "")).strip()
    campaign_id, create_error = create_barometer_campaign(
        name=name,
        year=year,
        slug=slug,
        description=description,
        questions=questions,
    )
    if create_error:
        return {"ok": False, "error": create_error}, 400

    return {
        "ok": True,
        "barometer_id": campaign_id,
        "slug": slug,
        "public_url": f"/barometro/{slug}",
    }, 201


def admin_create_flash_audit(payload: dict) -> tuple[dict, int]:
    name = str(payload.get("name", "")).strip()
    if not name:
        return {"ok": False, "error": "name es obligatorio"}, 400

    barometer_id = str(payload.get("barometer_id", "")).strip()
    barometer_slug = str(payload.get("barometer_slug", "")).strip()
    if not barometer_id and barometer_slug:
        barometer = fetch_barometer_campaign_by_slug(barometer_slug)
        if barometer:
            barometer_id = str(barometer["id"])

    if not barometer_id:
        return {"ok": False, "error": "barometer_id es obligatorio"}, 400

    barometer = fetch_barometer_campaign_by_id(barometer_id)
    if not barometer:
        return {"ok": False, "error": "Barometro no encontrado o inactivo"}, 404

    questions, error = normalize_campaign_questions(payload.get("questions"))
    if error:
        return {"ok": False, "error": error}, 400

    mapping, map_error = normalize_flash_mapping(
        payload.get("mapping"),
        questions,
        barometer["questions"] if isinstance(barometer.get("questions"), list) else [],
    )
    if map_error:
        return {"ok": False, "error": map_error}, 400

    raw_slug = str(payload.get("slug", "")).strip()
    slug = slugify(raw_slug or name)
    cta_label = str(payload.get("cta_label", "")).strip() or "Solucionar estas ineficiencias con GHC - Agendar Sesion"
    cta_url = str(payload.get("cta_url", "")).strip() or CONFIG.get("calendly_url", "https://calendly.com")

    campaign_id, create_error = create_flash_audit_campaign(
        name=name,
        slug=slug,
        barometer_id=barometer_id,
        questions=questions,
        mapping=mapping,
        cta_label=cta_label,
        cta_url=cta_url,
    )
    if create_error:
        return {"ok": False, "error": create_error}, 400

    return {
        "ok": True,
        "flash_audit_id": campaign_id,
        "slug": slug,
        "barometer_id": barometer_id,
        "public_url": f"/flash-audit/{slug}",
    }, 201


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        ensure_bootstrap()
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/static/style.css":
            css_response(self)
            return

        if path == "/static/manifest.webmanifest":
            static_file_response(
                self,
                file_path=MANIFEST_PATH,
                content_type="application/manifest+json; charset=utf-8",
                cache_control="no-store",
            )
            return

        if path == "/static/sw.js":
            static_file_response(
                self,
                file_path=SW_PATH,
                content_type="application/javascript; charset=utf-8",
                cache_control="no-store",
            )
            return

        if path == "/":
            redirect_response(self, "/login")
            return

        if path == "/login":
            html_response(self, login_view())
            return

        if path == "/cuestionario":
            html_response(self, app_shell())
            return

        if path.startswith("/barometro/"):
            slug = path.removeprefix("/barometro/").strip("/")
            if not slug:
                html_response(
                    self,
                    base_layout("No encontrado", "", '<section class="panel">Barometro no valido.</section>'),
                    404,
                )
                return
            campaign = fetch_barometer_campaign_by_slug(slug)
            if not campaign:
                html_response(
                    self,
                    base_layout("No encontrado", "", '<section class="panel">Barometro no encontrado.</section>'),
                    404,
                )
                return
            html_response(self, campaign_public_view(campaign_kind="barometer", campaign=campaign))
            return

        if path.startswith("/flash-audit/resultado/"):
            lead_id = path.removeprefix("/flash-audit/resultado/").strip("/")
            if not lead_id:
                html_response(
                    self,
                    base_layout("No encontrado", "", '<section class="panel">Resultado no valido.</section>'),
                    404,
                )
                return
            html_response(self, flash_result_view(lead_id))
            return

        if path.startswith("/flash-audit/"):
            slug = path.removeprefix("/flash-audit/").strip("/")
            if not slug:
                html_response(
                    self,
                    base_layout("No encontrado", "", '<section class="panel">Flash Audit no valido.</section>'),
                    404,
                )
                return
            campaign = fetch_flash_audit_by_slug(slug)
            if not campaign:
                html_response(
                    self,
                    base_layout("No encontrado", "", '<section class="panel">Flash Audit no encontrado.</section>'),
                    404,
                )
                return
            html_response(self, campaign_public_view(campaign_kind="flash", campaign=campaign))
            return

        if path == "/app":
            if not is_admin_authenticated(self):
                redirect_response(self, "/login")
                return

            redirect_response(self, "/app/inside-scope")
            return

        if path in {"/app/inside-scope", "/app/lead-engine"}:
            if not is_admin_authenticated(self):
                redirect_response(self, "/login")
                return

            module_slug = path.rsplit("/", 1)[-1]
            html_response(self, interior_app_view(module_slug, current_admin_email(self)))
            return

        if path == "/admin":
            if not is_admin_authenticated(self):
                redirect_response(self, "/login")
                return

            rows, stats = load_admin_rows()
            html_response(self, admin_dashboard(rows, stats))
            return

        if path == "/admin/response":
            if not is_admin_authenticated(self):
                redirect_response(self, "/login")
                return

            response_id = parse_qs(parsed.query).get("id", [""])[0].strip()
            if not response_id:
                redirect_response(self, "/admin")
                return

            html_response(self, admin_detail(response_id))
            return

        if path == "/admin/leads":
            if not is_admin_authenticated(self):
                redirect_response(self, "/login")
                return
            html_response(self, admin_leads_dashboard())
            return

        if path == "/admin/export.csv":
            export_csv(self)
            return

        if path == "/admin/logout":
            clear_admin_cookie(self)
            return

        if path == "/api/admin/campaigns":
            if not is_admin_authenticated(self):
                json_response(self, {"ok": False, "error": "No autenticado"}, 401)
                return
            payload = {"ok": True, "campaigns": list_campaigns()}
            json_response(self, payload, 200)
            return

        if path == "/api/admin/leads":
            if not is_admin_authenticated(self):
                json_response(self, {"ok": False, "error": "No autenticado"}, 401)
                return
            rows, stats = load_flash_leads_summary(limit=200)
            items = []
            for row in rows:
                items.append(
                    {
                        "id": row["id"],
                        "created_at": row["created_at"],
                        "flash_name": row["flash_name"],
                        "name": row["lead_name"],
                        "email": row["lead_email"],
                        "company": row["lead_company"],
                        "sector": row["sector"],
                        "tamano_empresa": row["tamano_empresa"],
                        "facturacion_2024": row["facturacion_2024"],
                        "overall_score": row["overall_score"],
                        "red_alerts": row["red_alerts"],
                        "yellow_alerts": row["yellow_alerts"],
                        "result_url": row["result_url"],
                    }
                )
            json_response(self, {"ok": True, "stats": stats, "rows": items}, 200)
            return

        if path == "/health":
            payload = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        html_response(self, base_layout("No encontrado", "", '<section class="panel">Ruta no valida.</section>'), 404)

    def do_POST(self) -> None:
        ensure_bootstrap()
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/inside-scope/intake":
            if not is_admin_authenticated(self):
                json_response(self, {"ok": False, "error": "No autenticado"}, 401)
                return

            payload, status = process_inside_scope_intake(self)
            json_response(self, payload, status)
            return

        if path == "/api/submit":
            payload, error = parse_json_body(self)
            if error:
                json_response(self, {"ok": False, "error": error}, 400)
                return

            email = str(payload.get("email", "")).strip().lower()
            ok_email, email_error = corporate_email_valid(email)
            if not ok_email:
                json_response(self, {"ok": False, "error": email_error}, 400)
                return

            answers, answers_error = validate_answers(payload)
            if answers_error:
                json_response(self, {"ok": False, "error": answers_error}, 400)
                return

            segment = response_segment(answers)
            benchmark_answers, segment_meta = build_segment_benchmark(segment)
            for q in ["q1", "q2", "q3"]:
                benchmark_answers[q] = answers[q]

            analysis = analyze_answers(answers, benchmark_answers, segment_meta)
            report_html = build_report_html(email, segment, analysis)

            webhook_payload = {
                "response_created_at": now_iso(),
                "email": email,
                "segment": segment,
                "answers": answers,
                "benchmark": benchmark_answers,
                "analysis": analysis,
                "report_html": report_html,
            }
            delivery_status, delivery_error = send_webhook(webhook_payload)

            response_id = store_response(
                email=email,
                segment=segment,
                answers=answers,
                benchmark=benchmark_answers,
                analysis=analysis,
                report_html=report_html,
                delivery_status=delivery_status,
                delivery_error=delivery_error,
                source_ip=self.client_address[0],
            )

            json_response(
                self,
                {
                    "ok": True,
                    "response_id": response_id,
                    "delivery_status": delivery_status,
                    "red_alerts": analysis["red_alerts"],
                    "yellow_alerts": analysis["yellow_alerts"],
                },
                200,
            )
            return

        if path.startswith("/api/barometro/") and path.endswith("/submit"):
            parts = [token for token in path.split("/") if token]
            if len(parts) != 4:
                json_response(self, {"ok": False, "error": "Ruta invalida"}, 404)
                return

            slug = parts[2]
            campaign = fetch_barometer_campaign_by_slug(slug)
            if not campaign:
                json_response(self, {"ok": False, "error": "Barometro no encontrado"}, 404)
                return

            payload, error = parse_json_body(self)
            if error:
                json_response(self, {"ok": False, "error": error}, 400)
                return

            answers, validation_error = validate_campaign_answers(payload.get("answers"), campaign["questions"])
            if validation_error:
                json_response(self, {"ok": False, "error": validation_error}, 400)
                return

            segment = extract_segment_from_campaign_answers(campaign["questions"], answers)
            response_id = store_barometer_submission(
                barometer_id=campaign["id"],
                answers=answers,
                segment=segment,
                source_ip=self.client_address[0],
            )
            json_response(
                self,
                {
                    "ok": True,
                    "response_id": response_id,
                    "segment": segment,
                    "campaign": {"id": campaign["id"], "name": campaign["name"], "slug": campaign["slug"]},
                },
                200,
            )
            return

        if path.startswith("/api/flash-audit/") and path.endswith("/submit"):
            parts = [token for token in path.split("/") if token]
            if len(parts) != 4:
                json_response(self, {"ok": False, "error": "Ruta invalida"}, 404)
                return

            slug = parts[2]
            flash = fetch_flash_audit_by_slug(slug)
            if not flash:
                json_response(self, {"ok": False, "error": "Flash Audit no encontrado"}, 404)
                return

            barometer = fetch_barometer_campaign_by_id(str(flash["barometer_id"]))
            if not barometer:
                json_response(
                    self,
                    {"ok": False, "error": "El Flash Audit no tiene un Barometro padre valido"},
                    500,
                )
                return

            payload, error = parse_json_body(self)
            if error:
                json_response(self, {"ok": False, "error": error}, 400)
                return

            answers, validation_error = validate_campaign_answers(payload.get("answers"), flash["questions"])
            if validation_error:
                json_response(self, {"ok": False, "error": validation_error}, 400)
                return

            lead, lead_error = normalize_lead_payload(payload.get("lead"))
            if lead_error:
                json_response(self, {"ok": False, "error": lead_error}, 400)
                return

            mapping = flash["mapping"] if isinstance(flash.get("mapping"), dict) else {}
            segment = extract_segment_from_campaign_answers(flash["questions"], answers)
            requested_qids: set[str] = set()
            for flash_question in flash["questions"]:
                if bool(flash_question.get("compare", False)):
                    mapped = str(mapping.get(str(flash_question["id"]), "")).strip()
                    if mapped:
                        requested_qids.add(mapped)

            benchmark, benchmark_meta = build_benchmark_from_barometer(
                barometer_id=barometer["id"],
                barometer_questions=barometer["questions"],
                requested_qids=requested_qids,
                segment=segment,
            )
            analysis = build_flash_audit_analysis(
                flash_questions=flash["questions"],
                barometer_questions=barometer["questions"],
                mapping=mapping,
                answers=answers,
                benchmark_by_barometer_qid=benchmark,
                benchmark_meta=benchmark_meta,
            )
            lead_id = store_flash_lead(
                flash=flash,
                lead=lead,
                answers=answers,
                segment=segment,
                benchmark=benchmark,
                analysis=analysis,
                source_ip=self.client_address[0],
            )

            json_response(
                self,
                {
                    "ok": True,
                    "lead_id": lead_id,
                    "result_url": f"/flash-audit/resultado/{lead_id}",
                    "segment": segment,
                    "benchmark": benchmark,
                    "analysis": analysis,
                    "campaign": {"id": flash["id"], "name": flash["name"], "slug": flash["slug"]},
                },
                200,
            )
            return

        if path == "/api/admin/barometers":
            if not is_admin_authenticated(self):
                json_response(self, {"ok": False, "error": "No autenticado"}, 401)
                return
            payload, error = parse_json_body(self)
            if error:
                json_response(self, {"ok": False, "error": error}, 400)
                return
            result, status = admin_create_barometer(payload)
            json_response(self, result, status)
            return

        if path == "/api/admin/flash-audits":
            if not is_admin_authenticated(self):
                json_response(self, {"ok": False, "error": "No autenticado"}, 401)
                return
            payload, error = parse_json_body(self)
            if error:
                json_response(self, {"ok": False, "error": error}, 400)
                return
            result, status = admin_create_flash_audit(payload)
            json_response(self, result, status)
            return

        if path == "/admin/login":
            form = parse_post_form(self)
            email = form.get("email", "").strip().lower()
            if not email:
                html_response(self, login_view(error="Introduce tu correo", email=email), 401)
                return

            if not credentials_valid(email, form.get("password", "")):
                html_response(self, login_view(error="Credenciales incorrectas", email=email), 401)
                return

            token = create_admin_session(email)
            set_admin_cookie(self, token)
            return

        html_response(self, base_layout("No encontrado", "", '<section class="panel">Ruta no valida.</section>'), 404)

    def log_message(self, fmt: str, *args) -> None:
        print("%s - - [%s] %s" % (self.address_string(), self.log_date_time_string(), fmt % args))


def run() -> None:
    ensure_bootstrap()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    print(f"Servidor en http://{host}:{port}")
    print(f"Login:   http://localhost:{port}/login")
    print(f"App:     http://localhost:{port}/app")
    print(f"Cliente: http://localhost:{port}/cuestionario")
    print(f"Barometro demo: http://localhost:{port}/barometro/{DEFAULT_BAROMETER_SLUG}")
    print(f"Flash Audit demo: http://localhost:{port}/flash-audit/{DEFAULT_FLASH_AUDIT_SLUG}")
    print(f"Admin:   http://localhost:{port}/admin")
    if ADMIN_PASSWORD == "cambia-esta-clave":
        print("ADVERTENCIA: define ADMIN_PASSWORD antes de publicar.")
    if not REPORT_WEBHOOK_URL:
        print("NOTA: REPORT_WEBHOOK_URL no configurado; no se enviaran emails reales.")
    if not GEMINI_API_KEY:
        print("NOTA: GEMINI_API_KEY no configurado; Inside Scope no podra procesar audios/texto.")

    server = ThreadingHTTPServer((host, port), AppHandler)
    server.serve_forever()


if __name__ == "__main__":
    run()
