# 5bot.py — HR-бот: ES/UA, Google Sheet (FAQ / Forms / Profiles)
import os, re, csv, html, json, asyncio, logging, urllib.parse, io, hashlib, unicodedata
from io import StringIO, BytesIO
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import aiosqlite
import httpx
import smtplib, ssl, secrets
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, InputFile
from telegram.constants import ChatAction
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, ContextTypes, filters
)

# ---------- базовая настройка ----------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger("hr_tg_bot")

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in (os.getenv("ADMIN_IDS") or "").split(",") if x.strip()]

WEBAPP_URL = os.getenv("WEBAPP_URL") or ""
SYNC_INTERVAL_MIN = int(os.getenv("SYNC_INTERVAL_MIN") or "0")  # 0 = off

GOOGLE_SHEET_EDIT_URL = os.getenv("GOOGLE_SHEET_EDIT_URL") or ""
GOOGLE_FAQ_GID = os.getenv("GOOGLE_FAQ_GID") or ""
GOOGLE_FORMS_GID = os.getenv("GOOGLE_FORMS_GID") or ""
GOOGLE_PROFILES_GID = os.getenv("GOOGLE_PROFILES_GID") or ""

# SMTP/OTP
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "no-reply@example.com")
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() in {"1","true","yes"}

OTP_TTL_MIN = int(os.getenv("OTP_TTL_MIN", "10"))
OTP_ATTEMPTS_MAX = int(os.getenv("OTP_ATTEMPTS_MAX", "5"))
OTP_RESEND_MAX = int(os.getenv("OTP_RESEND_MAX", "3"))
OTP_PEPPER = os.getenv("OTP_PEPPER", "change-me")

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "hr_forms.db"

LANGS = ("es", "uk")

def to_html(text: str) -> str:
    esc = html.escape(text or "")
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", esc)

def card(title: str, body_lines: List[str]) -> str:
    body = "\n".join(f"• {html.escape(line)}" for line in body_lines if str(line).strip() != "")
    return f"╭─╴<b>{html.escape(title)}</b>\n│ {body}\n╰──────────────────"

def is_valid_webapp_url(url: str) -> bool:
    if not url:
        return False
    try:
        u = urllib.parse.urlparse(url)
        return u.scheme == "https" and bool(u.netloc) and "<" not in url and ">" not in url and " " not in url
    except Exception:
        return False

# ---------- тексты ----------
TX: Dict[str, Dict[str, str]] = {
    "start_banner": {
        "es": "✨ <b>HR Assistant</b>\nTe ayudo con vacaciones, bajas médicas, nómina, formularios y contactos.\nElige abajo o escribe tu consulta.",
        "uk": "✨ <b>HR Assistant</b>\nДопоможу з відпустками, лікарняними, зарплатою, формами та контактами.\nОбери нижче або напишіть запит."
    },
    "help": {
        "es": ("Comandos:\n"
               "/start — menú\n"
               "/help — ayuda\n"
               "/cancel — cancelar formulario\n"
               "/myid — tu Telegram ID\n"
               "/whoami — ver tu perfil\n"
               "/logout — desvincular login\n"
               "/verify — verificación\n"
               "/resend — reenviar código de verificación\n"
               "/stats — estadísticas (admin)\n"
               "/users [offset] [limit] — lista (admin)\n"
               "/export_users — exportar CSV (admin)\n"
               "/setprofile <login> <json> — guardar perfil (admin)\n"
               "/import_profiles — importar CSV de perfiles (admin)\n"
               "/refresh — recargar Google Sheet (admin)\n"
               "/dump_profile <login> — ver perfil crudo (admin)\n"),
        "uk": ("Команди:\n"
               "/start — меню\n"
               "/help — допомога\n"
               "/cancel — скасувати форму\n"
               "/myid — ваш Telegram ID\n"
               "/whoami — показати профіль\n"
               "/logout — відʼєднати логін\n"
               "/verify — верифікація\n"
               "/resend — надіслати код повторно\n"
               "/stats — статистика (адмін)\n"
               "/users [offset] [limit] — список (адмін)\n"
               "/export_users — експорт CSV (адмін)\n"
               "/setprofile <login> <json> — зберегти профіль (адмін)\n"
               "/import_profiles — імпорт CSV профілів (адмін)\n"
               "/refresh — перезавантажити Google Sheet (адмін)\n"
               "/dump_profile <login> — подивитись сирий профіль (адмін)\n")
    },
    "menu_main": {"es": "Menú principal:", "uk": "Головне меню:"},
    "menu_quick_title": {"es": "⚡ <b>Tópicos rápidos</b>\nElige una opción:", "uk": "⚡ <b>Швидкі теми</b>\nОберіть пункт:"},
    "menu_forms_title": {
        "es": "📝 <b>Formularios y documentos</b>\nElige un formulario:",
        "uk": "📝 <b>Форми та документи</b>\nОберіть форму:"
    },
    "menu_forms_fill": {"es": "✍️ <b>Rellenar formulario</b>\nElige:", "uk": "✍️ <b>Заповнення форми</b>\nОберіть:"},
    "fill_start_hint": {
        "es": "Para empezar, pulsa «✍️ Rellenar en el bot» y responde a los campos. /cancel — cancelar.",
        "uk": "Щоб почати заповнення — натисніть «✍️ Заповнити в боті» та відповідайте на запити полів. /cancel — скасувати."
    }
}

# ---------- нормализация текста ----------
NL_SPLIT = re.compile(r"[;\|\n,]")

def _clean_text(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\\n", "\n").replace("\\t", "\t")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"(?m)^[ \t]+", "", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def _split_fields(s: str) -> List[str]:
    s = _clean_text(s or "")
    if not s:
        return []
    parts = [x.strip() for x in NL_SPLIT.split(s)]
    return [p for p in parts if p]

def _split_keywords(s: str) -> List[str]:
    s = _clean_text(s or "")
    if not s:
        return []
    parts = [x.strip() for x in NL_SPLIT.split(s)]
    return [p for p in parts if p]

# ---------- ДИНАМИКА из Google Sheet ----------
KB_ES: Dict[str, Dict[str, Any]] = {}
KB_UK: Dict[str, Dict[str, Any]] = {}
FORMS_ES: Dict[str, Dict[str, Any]] = {}
FORMS_UK: Dict[str, Dict[str, Any]] = {}

def kb_for_lang(lang: str): return KB_ES if lang == "es" else KB_UK
def forms_for_lang(lang: str): return FORMS_ES if lang == "es" else FORMS_UK

async def fetch_rows_from_sheet(edit_url: str, override_gid: Optional[str]) -> List[dict]:
    if not edit_url:
        raise RuntimeError("GOOGLE_SHEET_EDIT_URL is empty")
    try:
        u = urllib.parse.urlparse(edit_url)
        parts = [p for p in u.path.split("/") if p]
        doc_id = parts[2] if len(parts) >= 3 else parts[-1]
        gid = (override_gid or (urllib.parse.parse_qs(u.query).get("gid") or ["0"])[0])
        urls = [
            f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}",
            f"https://docs.google.com/spreadsheets/d/{doc_id}/gviz/tq?tqx=out:csv&gid={gid}",
        ]
    except Exception:
        urls = [edit_url]

    raw = None
    last_err = None
    for url in urls:
        log.info(f"[gsheet] try CSV URL: {url}")
        try:
            async with httpx.AsyncClient(
                timeout=25, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*;q=0.1", "Cache-Control": "no-cache"},
            ) as client:
                r = await client.get(url)
                r.raise_for_status()
                raw = r.text
                if raw and raw.strip():
                    break
        except Exception as e:
            last_err = e
            log.error(f"[gsheet] fetch failed for {url}: {e}")
    if not raw:
        raise RuntimeError(f"CSV not loaded. Last error: {last_err}")

    reader = csv.DictReader(StringIO(raw))
    return list(reader)

async def fetch_sheet_configs():
    rows_faq = []
    rows_forms = []
    rows_profiles = []
    if GOOGLE_FAQ_GID:
        rows_faq = await fetch_rows_from_sheet(GOOGLE_SHEET_EDIT_URL, GOOGLE_FAQ_GID)
    if GOOGLE_FORMS_GID:
        rows_forms = await fetch_rows_from_sheet(GOOGLE_SHEET_EDIT_URL, GOOGLE_FORMS_GID)
    if GOOGLE_PROFILES_GID:
        rows_profiles = await fetch_rows_from_sheet(GOOGLE_SHEET_EDIT_URL, GOOGLE_PROFILES_GID)

    if not any([rows_faq, rows_forms, rows_profiles]):
        rows_faq = await fetch_rows_from_sheet(GOOGLE_SHEET_EDIT_URL, None)

    KB_es, KB_uk = {}, {}
    FORMS_es_new, FORMS_uk_new = {}, {}
    PROFILES: Dict[str, dict] = {}

    def ingest_row(row: dict):
        typ  = (row.get("type") or "").strip().lower()
        lang = (row.get("lang") or "").strip().lower()
        key  = (row.get("key") or row.get("login") or "").strip()

        title      = _clean_text(row.get("title") or "")
        text       = _clean_text(row.get("text") or "")
        fields_str = _clean_text(row.get("fields") or "")
        icon       = (row.get("icon") or "").strip() or "📝"
        keywords   = _split_keywords(row.get("keywords") or "")
        url        = (row.get("url") or "").strip()

        if typ == "faq" and lang in ("es", "uk") and key:
            entry = {
                "title": title or key,
                "keywords": keywords if keywords else [key],
                "response": text or title or key
            }
            (KB_es if lang == "es" else KB_uk)[key] = entry

        elif typ == "form" and lang in ("es", "uk") and key:
            entry = {
                "name": title or key,
                "fields": _split_fields(fields_str),
                "icon": icon or "📝",
                "url": url if url else None
            }
            (FORMS_es_new if lang == "es" else FORMS_uk_new)[key] = entry

        elif typ == "profile" and key:
            login = key
            PROFILES[login] = {
                "login": login,
                "full_name": _clean_text(row.get("full_name") or ""),
                "position":  _clean_text(row.get("position")  or ""),
                "team":      _clean_text(row.get("department") or row.get("team") or ""),
                "email":     (row.get("email") or "").strip(),
                "phone":     (row.get("phone") or "").strip(),
                "manager":   _clean_text(row.get("manager") or ""),
                "vacation_left": int((row.get("vacation_left") or "0").strip() or 0),
                "salary_usd":   int((row.get("salary_usd") or "0").strip() or 0),
                "extra_json": None,
            }

    for r in rows_faq:     ingest_row(r)
    for r in rows_forms:   ingest_row(r)
    for r in rows_profiles:ingest_row(r)

    if not FORMS_es_new and not FORMS_uk_new:
        FORMS_es_new.update({"vacation": {"name":"Solicitud de vacaciones","fields":["Nombre","Posición","Inicio","Fin","Días"],"icon":"📅","url":None}})
        FORMS_uk_new.update({"vacation": {"name":"Заява на відпустку","fields":["ПІБ","Посада","Початок","Завершення","Кількість днів"],"icon":"📅","url":None}})
    if not KB_es and not KB_uk:
        KB_es.update({"vacaciones": {"title":"Vacaciones","keywords":["vacaciones"], "response":"📅 **Vacaciones**: 24 días."}})
        KB_uk.update({"відпустка": {"title":"Відпустка","keywords":["відпустка"], "response":"📅 **Відпустка**: 24 дні."}})

    log.info(f"[gsheet] built: KB_es={len(KB_es)} KB_uk={len(KB_uk)} FORMS_es={len(FORMS_es_new)} FORMS_uk={len(FORMS_uk_new)} PROFILES={len(PROFILES)}")
    return KB_es, KB_uk, FORMS_es_new, FORMS_uk_new, PROFILES

# ---------- профиль ----------
def profile_card(lang: str, p: dict) -> str:
    if lang == "es":
        lines = [
            f"Nombre: {p.get('full_name','—')}",
            f"Puesto: {p.get('position','—')}",
            f"Equipo: {p.get('team','—')}",
            f"Email: {p.get('email','—')}",
            f"Tel.: {p.get('phone','—')}",
            f"Manager: {p.get('manager','—')}",
            f"Vacaciones restantes: {p.get('vacation_left','—')} días",
            f"Salario: ${p.get('salary_usd','—')} USD/mes",
        ]
        title = "👤 <b>Tu perfil</b>"
        note  = "Si ves datos incorrectos, avisa a RR. HH."
    else:
        lines = [
            f"Імʼя: {p.get('full_name','—')}",
            f"Посада: {p.get('position','—')}",
            f"Команда: {p.get('team','—')}",
            f"Email: {p.get('email','—')}",
            f"Тел.: {p.get('phone','—')}",
            f"Менеджер: {p.get('manager','—')}",
            f"Залишок відпустки: {p.get('vacation_left','—')} днів",
            f"Зарплата: ${p.get('salary_usd','—')} USD/міс",
        ]
        title = "👤 <b>Ваш профіль</b>"
        note  = "Якщо дані некоректні — повідомте HR."
    return f"{title}\n" + card(p.get("login","—"), lines) + f"\n\n{note}"

# ---------- безопасные callback токены для FAQ ----------
CB_MAP = {"es": {}, "uk": {}}

# ---------- клавиатуры ----------
def lang_toggle_row(lang: str) -> List[InlineKeyboardButton]:
    return [InlineKeyboardButton("🇺🇦 UA", callback_data="lang_uk")] if lang == "es" else [InlineKeyboardButton("🇪🇸 ES", callback_data="lang_es")]

def kb_back(lang: str, target_cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Atrás" if lang == "es" else "⬅️ Назад", callback_data=target_cb)],
        lang_toggle_row(lang)
    ])

async def kb_main_for(user_id: int) -> InlineKeyboardMarkup:
    lang = await get_pref_lang(user_id)
    rows: List[List[InlineKeyboardButton]] = []

    login = await get_user_login(user_id)
    if login:
        rows.append([InlineKeyboardButton("👤 Mi perfil" if lang=="es" else "👤 Мій профіль", callback_data="menu_profile")])

    if is_valid_webapp_url(WEBAPP_URL):
        rows.append([InlineKeyboardButton("🚀 WebApp HR", web_app=WebAppInfo(url=WEBAPP_URL))])

    if lang=="es":
        rows += [
            [InlineKeyboardButton("⚡ Tópicos rápidos", callback_data="menu_quick")],
            [InlineKeyboardButton("📝 Formularios y documentos", callback_data="menu_forms")],
        ]
    else:
        rows += [
            [InlineKeyboardButton("⚡ Швидкі теми", callback_data="menu_quick")],
            [InlineKeyboardButton("📝 Форми та документи", callback_data="menu_forms")],
        ]

    if not await is_verified(user_id):
        rows.append([InlineKeyboardButton("🔒 Verificación" if lang=="es" else "🔒 Верифікація", callback_data="start_verify")])

    rows.append(lang_toggle_row(lang))
    return InlineKeyboardMarkup(rows)

def kb_forms_info(lang: str) -> InlineKeyboardMarkup:
    forms = forms_for_lang(lang)
    items = sorted(forms.items(), key=lambda kv: kv[1].get("name",""))
    rows = []
    for key, meta in items:
        rows.append([
            InlineKeyboardButton(f"{meta.get('icon','📝')} {meta['name']}", callback_data=f"formchoice_{key}")
        ])
    rows.append([InlineKeyboardButton("⬅️ Atrás" if lang=="es" else "⬅️ Назад", callback_data="back_main")])
    rows.append(lang_toggle_row(lang))
    return InlineKeyboardMarkup(rows)

def kb_form_choice(lang: str, form_key: str) -> InlineKeyboardMarkup:
    f = forms_for_lang(lang).get(form_key) or {}
    rows = []
    if f.get("fields"):
        rows.append([InlineKeyboardButton("✍️ Rellenar en el bot" if lang=="es" else "✍️ Заповнити в боті", callback_data=f"formfill_{form_key}")])
    if f.get("url"):
        rows.append([InlineKeyboardButton("🌐 Abrir Google Form" if lang=="es" else "🌐 Відкрити Google Form", url=f["url"])])
    rows.append([InlineKeyboardButton("⬅️ Atrás" if lang=="es" else "⬅️ Назад", callback_data="menu_forms")])
    rows.append(lang_toggle_row(lang))
    return InlineKeyboardMarkup(rows)

def kb_quick(lang: str) -> InlineKeyboardMarkup:
    KB = kb_for_lang(lang)
    items: List[Tuple[str, str]] = []
    for k, v in KB.items():
        t = (v.get("title") or k).strip()
        r = (v.get("response") or "").strip()
        if t and r:
            items.append((k, t))
    items.sort(key=lambda it: it[1].lower())

    CB_MAP[lang] = {}

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for k, t in items:
        token = hashlib.md5(k.encode("utf-8")).hexdigest()[:10]
        CB_MAP[lang][token] = k
        row.append(InlineKeyboardButton(t, callback_data=f"faq_{token}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row: rows.append(row)

    rows.append([InlineKeyboardButton("⬅️ Назад" if lang=="uk" else "⬅️ Atrás", callback_data="back_main")])
    rows.append(lang_toggle_row(lang))
    return InlineKeyboardMarkup(rows)

# ---------- текстовые блоки по формам ----------
def _form_choice_text(lang: str, key: str) -> str:
    forms = forms_for_lang(lang)
    f = forms.get(key)
    if not f: return "—"
    name_clean   = _clean_text(f.get("name",""))
    fields_clean = [_clean_text(x) for x in (f.get("fields") or [])]

    if lang == "es":
        title = f"📝 <b>{html.escape(name_clean)}</b>\n\n"
        desc  = "Elige cómo quieres rellenar este formulario:\n\n"
        opt1  = "• <b>En el bot</b> — paso a paso aquí en Telegram\n"
        opt2  = "• <b>Google Form</b> — abre el formulario en tu navegador\n" if f.get("url") else ""
        fields_title = "<b>Campos:</b>"
    else:
        title = f"📝 <b>{html.escape(name_clean)}</b>\n\n"
        desc  = "Оберіть спосіб заповнення форми:\n\n"
        opt1  = "• <b>В боті</b> — крок за кроком тут у Telegram\n"
        opt2  = "• <b>Google Form</b> — відкрити форму в браузері\n" if f.get("url") else ""
        fields_title = "<b>Поля:</b>"

    fields_list = "\n".join([f"  ▫️ {html.escape(x)}" for x in fields_clean])
    fields_section = f"\n{fields_title}\n{fields_list}" if fields_list else ""
    return f"{title}{desc}{opt1}{opt2}{fields_section}"

def _form_info_text(lang: str, key: str) -> str:
    forms = forms_for_lang(lang)
    f = forms.get(key)
    if not f: return "—"
    name_clean   = _clean_text(f.get("name",""))
    fields_clean = [_clean_text(x) for x in (f.get("fields") or [])]
    title = ("ℹ️ <b>{name}</b>\nНеобхідні поля:" if lang=="uk" else "ℹ️ <b>{name}</b>\nCampos necesarios:").format(name=html.escape(name_clean))
    lines = "\n".join([f"• {html.escape(x)}" for x in fields_clean])
    hint = TX["fill_start_hint"][lang]
    url_section = ""
    if f.get("url"):
        url_text = "🔗 <b>Заповнити онлайн:</b>" if lang=="uk" else "🔗 <b>Rellenar online:</b>"
        url_section = f"\n\n{url_text}\n{html.escape(f['url'])}"
    return f"{title}\n{lines}\n\n{hint}{url_section}"

# ---------- сервиски ----------
async def show_loader_and_edit(query, final_text: str, reply_markup=None, parse_mode="HTML", delay_ms=200, lang="es"):
    try: await query.edit_message_text("⏳ <i>Cargando…</i>" if lang=="es" else "⏳ <i>Завантаження…</i>", parse_mode="HTML")
    except: pass
    try: await query.message.chat.send_action(ChatAction.TYPING)
    except: pass
    await asyncio.sleep(delay_ms/1000)
    await query.edit_message_text(final_text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)

def find_best_match(user_message: str, lang: str) -> Optional[str]:
    msg = (user_message or "").lower()
    KB = kb_for_lang(lang)
    for _, data in KB.items():
        for kw in data.get("keywords", []):
            if kw.lower() in msg:
                return data["response"]
    return None

# ---------- БД ----------
CREATE_FORMS_SQL = """
CREATE TABLE IF NOT EXISTS form_submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_user_id INTEGER,
    username TEXT,
    form_key TEXT,
    data_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    language_code TEXT,
    pref_lang TEXT DEFAULT 'es',
    login TEXT,
    verified INTEGER DEFAULT 0,
    is_bot INTEGER DEFAULT 0,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    msg_count  INTEGER DEFAULT 0,
    click_count INTEGER DEFAULT 0
);
"""
CREATE_PROFILES_SQL = """
CREATE TABLE IF NOT EXISTS profiles (
    login TEXT PRIMARY KEY,
    full_name TEXT,
    position TEXT,
    team TEXT,
    email TEXT,
    phone TEXT,
    manager TEXT,
    vacation_left INTEGER,
    salary_usd INTEGER,
    extra_json TEXT
);
"""
CREATE_VERIFY_SQL = """
CREATE TABLE IF NOT EXISTS verify_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_user_id INTEGER,
    email TEXT,
    code_hash TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    resend_count INTEGER DEFAULT 0
);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute(CREATE_FORMS_SQL)
        await db.execute(CREATE_USERS_SQL)
        await db.execute(CREATE_PROFILES_SQL)
        await db.execute(CREATE_VERIFY_SQL)
        cur = await db.execute("PRAGMA table_info(users)")
        cols = {row[1] for row in await cur.fetchall()}
        if "pref_lang" not in cols:   await db.execute("ALTER TABLE users ADD COLUMN pref_lang TEXT DEFAULT 'es'")
        if "login" not in cols:       await db.execute("ALTER TABLE users ADD COLUMN login TEXT")
        if "verified" not in cols:    await db.execute("ALTER TABLE users ADD COLUMN verified INTEGER DEFAULT 0")
        if "msg_count" not in cols:   await db.execute("ALTER TABLE users ADD COLUMN msg_count INTEGER DEFAULT 0")
        if "click_count" not in cols: await db.execute("ALTER TABLE users ADD COLUMN click_count INTEGER DEFAULT 0")
        await db.execute("UPDATE users SET pref_lang = COALESCE(pref_lang,'es')")
        await db.commit()

def is_admin(uid: int) -> bool: return uid in ADMIN_IDS

async def get_pref_lang(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("SELECT pref_lang FROM users WHERE id=?", (user_id,))
        row = await cur.fetchone()
    return row[0] if row and row[0] in LANGS else "es"

async def set_pref_lang(user_id: int, lang: str):
    if lang not in LANGS: return
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("UPDATE users SET pref_lang=? WHERE id=?", (lang, user_id))
        await db.commit()

async def track_user(update: Update, *, inc_msg=0, inc_click=0):
    u = update.effective_user
    if not u: return
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("""
            INSERT INTO users (id, username, first_name, last_name, language_code, pref_lang, is_bot, msg_count, click_count)
            VALUES (?, ?, ?, ?, ?, COALESCE((SELECT pref_lang FROM users WHERE id=?),'es'), ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              username=excluded.username,
              first_name=excluded.first_name,
              last_name=excluded.last_name,
              language_code=excluded.language_code,
              is_bot=excluded.is_bot,
              last_seen=CURRENT_TIMESTAMP,
              msg_count = users.msg_count + ?,
              click_count = users.click_count + ?;
        """, (
            u.id, u.username or "", u.first_name or "", u.last_name or "",
            getattr(u, "language_code", None) or "",
            u.id, int(u.is_bot), inc_msg, inc_click, inc_msg, inc_click
        ))
        await db.commit()

async def get_user_login(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("SELECT login FROM users WHERE id=?", (user_id,))
        row = await cur.fetchone()
    return row[0] if row and row[0] else None

async def set_user_login(user_id: int, login: str):
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("UPDATE users SET login=?, verified=0 WHERE id=?", (login, user_id))
        await db.commit()

async def clear_user_login(user_id: int):
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("UPDATE users SET login=NULL, verified=0 WHERE id=?", (user_id,))
        await db.commit()

async def get_profile_by_login(login: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("""
            SELECT login, full_name, position, team, email, phone, manager, vacation_left, salary_usd, extra_json
            FROM profiles WHERE login=?
        """, (login,))
        row = await cur.fetchone()
    if not row:
        return None
    keys = ["login","full_name","position","team","email","phone","manager","vacation_left","salary_usd","extra_json"]
    data = dict(zip(keys, row))
    try:
        data["extra"] = json.loads(data["extra_json"]) if data["extra_json"] else {}
    except Exception:
        data["extra"] = {}
    return data

async def upsert_profiles(profiles: Dict[str, dict]):
    if not profiles: return
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        for p in profiles.values():
            await db.execute("""
                INSERT INTO profiles (login, full_name, position, team, email, phone, manager, vacation_left, salary_usd, extra_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(login) DO UPDATE SET
                  full_name=excluded.full_name,
                  position=excluded.position,
                  team=excluded.team,
                  email=excluded.email,
                  phone=excluded.phone,
                  manager=excluded.manager,
                  vacation_left=excluded.vacation_left,
                  salary_usd=excluded.salary_usd,
                  extra_json=excluded.extra_json
            """, (
                p.get("login"), p.get("full_name"), p.get("position"), p.get("team"),
                p.get("email"), p.get("phone"), p.get("manager"),
                int(p.get("vacation_left") or 0),
                int(p.get("salary_usd") or 0),
                p.get("extra_json")
            ))
        await db.commit()

# ---------- OTP/Email утилиты ----------
def _mask_email(s: str) -> str:
    s = (s or "").strip()
    if "@" not in s:
        return s
    name, dom = s.split("@", 1)
    name_m = (name[0] + "*"*(len(name)-1)) if len(name) > 1 else name
    dom_parts = dom.split(".")
    dom_m = ".".join([p[0] + "*"*(len(p)-1) if p else p for p in dom_parts])
    return f"{name_m}@{dom_m}"

def _otp_hash(code: str) -> str:
    return hashlib.sha256((OTP_PEPPER + str(code)).encode("utf-8")).hexdigest()

def _now_utc():
    return datetime.now(timezone.utc)

async def _otp_purge_old():
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("DELETE FROM verify_codes WHERE expires_at < ?", (_now_utc().isoformat(),))
        await db.commit()

async def _otp_create(user_id: int, email: str) -> str:
    await _otp_purge_old()
    code = f"{secrets.randbelow(1000000):06d}"
    h = _otp_hash(code)
    expires = (_now_utc() + timedelta(minutes=OTP_TTL_MIN)).isoformat()
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("DELETE FROM verify_codes WHERE tg_user_id=?", (user_id,))
        await db.execute("""
            INSERT INTO verify_codes (tg_user_id, email, code_hash, expires_at, attempts, resend_count)
            VALUES (?, ?, ?, ?, 0, 0)
        """, (user_id, (email or "").strip().lower(), h, expires))
        await db.commit()
    return code

async def _otp_inc_attempt(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("UPDATE verify_codes SET attempts = attempts + 1 WHERE tg_user_id=?", (user_id,))
        await db.commit()
        cur = await db.execute("SELECT attempts FROM verify_codes WHERE tg_user_id=?", (user_id,))
        row = await cur.fetchone()
    return int(row[0]) if row else OTP_ATTEMPTS_MAX

async def _otp_can_resend(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("SELECT resend_count FROM verify_codes WHERE tg_user_id=?", (user_id,))
        row = await cur.fetchone()
    return (row is None) or (int(row[0]) < OTP_RESEND_MAX)

async def _otp_mark_resend(user_id: int):
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("UPDATE verify_codes SET resend_count = resend_count + 1 WHERE tg_user_id=?", (user_id,))
        await db.commit()

async def _otp_verify(user_id: int, code: str) -> Tuple[bool, str]:
    await _otp_purge_old()
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("""
            SELECT code_hash, expires_at, attempts FROM verify_codes
            WHERE tg_user_id=?
        """, (user_id,))
        row = await cur.fetchone()
    if not row:
        return False, "no_active"
    code_hash, expires_at, attempts = row
    if datetime.fromisoformat(expires_at) < _now_utc():
        return False, "expired"
    if int(attempts) >= OTP_ATTEMPTS_MAX:
        return False, "too_many"
    if _otp_hash(code) == code_hash:
        async with aiosqlite.connect(DB_PATH.as_posix()) as db:
            await db.execute("DELETE FROM verify_codes WHERE tg_user_id=?", (user_id,))
            await db.commit()
        return True, "ok"
    att = await _otp_inc_attempt(user_id)
    if att >= OTP_ATTEMPTS_MAX:
        return False, "too_many"
    return False, "mismatch"

def _send_email_sync(host, port, user, pwd, use_ssl, from_addr, to_addr, subject, body):
    msg = f"From: {from_addr}\r\nTo: {to_addr}\r\nSubject: {subject}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{body}"
    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=context) as s:
            if user:
                s.login(user, pwd)
            s.sendmail(from_addr, [to_addr], msg.encode("utf-8"))
    else:
        with smtplib.SMTP(host, port) as s:
            s.ehlo()
            s.starttls(context=ssl.create_default_context())
            if user:
                s.login(user, pwd)
            s.sendmail(from_addr, [to_addr], msg.encode("utf-8"))

async def _send_email_otp(to_email: str, code: str, lang: str):
    if not SMTP_HOST:
        raise RuntimeError("SMTP_HOST не задан — відправка коду неможлива")
    subj = "Код підтвердження" if lang == "uk" else "Código de verificación"
    body = (
        f"Ваш код підтвердження: {code}\n"
        f"Діє {OTP_TTL_MIN} хвилин.\n\n"
        f"Якщо це не ви — проігноруйте лист."
        if lang == "uk" else
        f"Tu código de verificación: {code}\n"
        f"Válido durante {OTP_TTL_MIN} minutos.\n\n"
        f"Si no fuiste tú, ignora este correo."
    )
    await asyncio.to_thread(
        _send_email_sync, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_USE_SSL,
        SMTP_FROM, to_email, subj, body
    )

async def _issue_and_send_otp(user_id: int, email: str, lang: str) -> None:
    code = await _otp_create(user_id, email)
    await _send_email_otp(email, code, lang)

# ---------- верификация ----------
def _digits_only(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = s.replace("\u200e","").replace("\u200f","").replace("\u202a","").replace("\u202b","").replace("\u202c","").replace("\xa0"," ")
    digits = []
    for ch in s:
        if ch.isdigit():
            try:
                d = unicodedata.digit(ch)
            except Exception:
                d = int(ch)
            digits.append(str(d))
    return "".join(digits)

def _last_n(s: str, n: int) -> str:
    d = _digits_only(s)
    return d[-n:] if len(d) >= n else d

def _phones_match(user_input: str, expected: str) -> bool:
    ui = _digits_only(user_input)
    ex = _digits_only(expected)
    ok = (
        ui == ex or
        (len(ui) >= 10 and len(ex) >= 10 and _last_n(ui, 10) == _last_n(ex, 10)) or
        (len(ui) >= 9  and len(ex) >= 9  and _last_n(ui, 9)  == _last_n(ex, 9))
    )
    if not ok:
        log.warning("[verify] phone mismatch | ui='%s' ex='%s'", ui, ex)
    else:
        log.info("[verify] phone matched")
    return ok

def _norm_email(s: str) -> str:
    return (s or "").strip().lower()

async def set_verified(user_id: int, value: int):
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("UPDATE users SET verified=? WHERE id=?", (value, user_id))
        await db.commit()

async def get_verified(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("SELECT verified FROM users WHERE id=?", (user_id,))
        row = await cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0

async def is_verified(user_id: int) -> bool:
    return (await get_verified(user_id)) >= 1

async def start_verification_flow(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(update_or_query, Update) and update_or_query.message:
        uid = update_or_query.effective_user.id
        lang = await get_pref_lang(uid)
    else:
        q = update_or_query
        uid = q.from_user.id
        lang = await get_pref_lang(uid)

    login = await get_user_login(uid)
    if not login:
        txt = "🔐 Спочатку введіть корпоративний логін (/start)." if lang=="uk" else "🔐 Primero introduce tu login corporativo (/start)."
        if isinstance(update_or_query, Update) and update_or_query.message:
            await update_or_query.message.reply_text(txt)
        else:
            await show_loader_and_edit(update_or_query, txt, reply_markup=None, lang=lang)
        return

    prof = await get_profile_by_login(login)
    if not prof:
        txt = "❌ Профіль не знайдено. Зверніться до HR." if lang=="uk" else "❌ Perfil no encontrado. Contacta RR. HH."
        if isinstance(update_or_query, Update) and update_or_query.message:
            await update_or_query.message.reply_text(txt)
        else:
            await show_loader_and_edit(update_or_query, txt, reply_markup=None, lang=lang)
        return

    context.user_data["verify"] = {
        "step": 1,  # 1: телефон; 3: код из email
        "expect_phone": (prof.get("phone") or ""),
        "email": _norm_email(prof.get("email")),
        "lang": lang
    }

    prompt = "📞 Вкажіть номер телефону (тільки цифри)." if lang=="uk" else "📞 Indica tu número (solo dígitos)."
    if isinstance(update_or_query, Update) and update_or_query.message:
        await update_or_query.message.reply_text(prompt)
    else:
        await show_loader_and_edit(update_or_query, prompt, reply_markup=None, lang=lang)

# ---------- состояния ----------
LOGIN = 2

# ---------- хендлеры ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_user(update, inc_msg=1)
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)

    login = await get_user_login(uid)
    if not login:
        await update.message.reply_text("🔐 Введіть свій <b>корпоративний логін</b>:" if lang=="uk" else "🔐 Introduce tu <b>login corporativo</b>:", parse_mode="HTML")
        return LOGIN

    if not await is_verified(uid) and not is_admin(uid):
        await start_verification_flow(update, context)
        return

    await update.message.reply_text(
        TX["start_banner"][lang],
        parse_mode="HTML",
        reply_markup=await kb_main_for(uid),
        disable_web_page_preview=True
    )

async def login_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    login_text = (update.message.text or "").strip()

    prof = await get_profile_by_login(login_text)
    if not prof:
        await update.message.reply_text("❌ Не знайдено такий логін. Спробуйте ще раз або зверніться до HR." if lang=="uk" else "❌ No encontré este login. Intenta de nuevo o contacta RR. HH.")
        return LOGIN

    await set_user_login(uid, login_text)  # verified=0
    await start_verification_flow(update, context)
    return ConversationHandler.END

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = await get_pref_lang(update.effective_user.id)
    await update.message.reply_text(TX["help"][lang], reply_markup=await kb_main_for(update.effective_user.id))

async def cmd_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_verification_flow(update, context)

async def cmd_resend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    vf = context.user_data.get("verify")
    if not vf or vf.get("step") != 3:
        msg = "Немає активної перевірки коду." if lang=="uk" else "No hay verificación de código activa."
        await update.message.reply_text(msg, reply_markup=await kb_main_for(uid))
        return
    if not await _otp_can_resend(uid):
        msg = "⛔ Ліміт повторних відправок вичерпано. Спробуйте пізніше." if lang=="uk" else "⛔ Límite de reenvíos alcanzado. Intenta más tarde."
        await update.message.reply_text(msg)
        return
    try:
        await _issue_and_send_otp(uid, vf.get("email") or "", lang)
        await _otp_mark_resend(uid)
        em_mask = _mask_email(vf.get("email") or "")
        msg = ("✉️ Новий код відправлено на " if lang=="uk" else "✉️ Nuevo código enviado a ") + f"<b>{html.escape(em_mask)}</b>"
        await update.message.reply_html(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")

async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = await get_pref_lang(update.effective_user.id)
    await update.message.reply_text(("👤 Ваш Telegram ID: {id}" if lang=="uk" else "👤 Tu Telegram ID: {id}").format(id=update.effective_user.id),
                                    reply_markup=await kb_main_for(update.effective_user.id))

async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    await update.message.reply_text(TX["menu_main"][lang], reply_markup=await kb_main_for(uid))

async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    await clear_user_login(uid)
    await update.message.reply_text("🔐 Введіть свій <b>корпоративний логін</b>:" if lang=="uk" else "🔐 Introduce tu <b>login corporativo</b>:", parse_mode="HTML")
    return LOGIN

async def _start_form_fill(update_or_query, context: ContextTypes.DEFAULT_TYPE, lang: str, key: str):
    f = forms_for_lang(lang).get(key)
    if not f:
        return
    fields = f.get("fields", [])
    if not fields:
        txt = _form_info_text(lang, key)
        if isinstance(update_or_query, Update) and update_or_query.message:
            await update_or_query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb_back(lang, f"formchoice_{key}"))
        else:
            q = update_or_query
            await show_loader_and_edit(q, txt, reply_markup=kb_back(lang, f"formchoice_{key}"), parse_mode="HTML", lang=lang)
        return

    context.user_data["form_fill"] = {"key": key, "fields": fields, "answers": [], "i": 0, "lang": lang}
    prompt = ("✍️ <b>Вкажіть</b>: {field}" if lang=="uk" else "✍️ <b>Introduce</b>: {field}").format(field=fields[0])
    if isinstance(update_or_query, Update) and update_or_query.message:
        await update_or_query.message.reply_text(prompt, parse_mode="HTML")
    else:
        q = update_or_query
        await show_loader_and_edit(q, prompt, reply_markup=None, parse_mode="HTML", lang=lang)

async def save_form_submission(user_id: int, username: str, form_key: str, data_dict: dict):
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute("""
            INSERT INTO form_submissions (tg_user_id, username, form_key, data_json)
            VALUES (?, ?, ?, ?)
        """, (user_id, username or "", form_key, json.dumps(data_dict, ensure_ascii=False)))
        await db.commit()

# ---------- единый обработчик кнопок ----------
async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await track_user(update, inc_click=1)
    try: await query.answer()
    except: pass

    data = query.data
    uid  = update.effective_user.id
    lang = await get_pref_lang(uid)

    # Переключение языка
    if data in ("lang_es", "lang_uk"):
        await set_pref_lang(uid, "es" if data.endswith("es") else "uk")
        lang = await get_pref_lang(uid)
        if not await is_verified(uid) and not is_admin(uid):
            await start_verification_flow(query, context); return
        await show_loader_and_edit(query, TX["menu_main"][lang], reply_markup=await kb_main_for(uid), lang=lang); return

    # Верификация
    if data == "start_verify":
        await start_verification_flow(query, context); return

    # Главные пункты
    if data == "menu_quick":
        if not is_admin(uid) and not await is_verified(uid):
            await show_loader_and_edit(query, "🔒 Спершу пройдіть верифікацію: натисніть «Верифікація».", reply_markup=await kb_main_for(uid), lang=lang); return
        await show_loader_and_edit(query, TX["menu_quick_title"][lang], kb_quick(lang), lang=lang); return

    if data == "menu_forms":
        if not is_admin(uid) and not await is_verified(uid):
            await show_loader_and_edit(query, "🔒 Спершу пройдіть верифікацію: натисніть «Верифікація».", reply_markup=await kb_main_for(uid), lang=lang); return
        await show_loader_and_edit(query, TX["menu_forms_title"][lang], kb_forms_info(lang), lang=lang); return

    if data == "back_main":
        await show_loader_and_edit(query, TX["menu_main"][lang], reply_markup=await kb_main_for(uid), lang=lang); return

    # Профиль
    if data == "menu_profile":
        login = await get_user_login(uid)
        if not login:
            await show_loader_and_edit(query, "🔐 Введіть свій <b>корпоративний логін</b>:" if lang=="uk" else "🔐 Introduce tu <b>login corporativo</b>:", reply_markup=None, lang=lang); return
        prof = await get_profile_by_login(login)
        if not prof:
            await show_loader_and_edit(query, "❌ Профіль не знайдено." if lang=="uk" else "❌ Perfil no encontrado.", reply_markup=await kb_main_for(uid), lang=lang); return
        await show_loader_and_edit(query, profile_card(lang, prof), reply_markup=kb_back(lang, "back_main"), parse_mode="HTML", lang=lang); return

    # Меню выбора способа заполнения формы
    if data.startswith("formchoice_"):
        if not is_admin(uid) and not await is_verified(uid):
            await show_loader_and_edit(query, "🔒 Спершу пройдіть верифікацію.", reply_markup=await kb_main_for(uid), lang=lang); return
        key = data.split("_", 1)[1]
        text = _form_choice_text(lang, key)
        await show_loader_and_edit(query, text, reply_markup=kb_form_choice(lang, key), parse_mode="HTML", lang=lang); return

    # Ретрансляция старого префикса
    if data.startswith("forminfo_"):
        key = data.split("_", 1)[1]
        text = _form_choice_text(lang, key)
        await show_loader_and_edit(query, text, reply_markup=kb_form_choice(lang, key), parse_mode="HTML", lang=lang); return

    # Пошаговое заполнение в боте
    if data.startswith("formfill_"):
        if not is_admin(uid) and not await is_verified(uid):
            await show_loader_and_edit(query, "🔒 Спершу пройдіть верифікацію.", reply_markup=await kb_main_for(uid), lang=lang); return
        key = data.split("_", 1)[1]
        await _start_form_fill(query, context, lang, key); return

    # FAQ-ответ
    if data.startswith("faq_"):
        if not is_admin(uid) and not await is_verified(uid):
            warn = "🔒 Спершу пройдіть верифікацію: натисніть «Верифікація»." if lang=="uk" else "🔒 Primero completa la verificación."
            await show_loader_and_edit(query, warn, reply_markup=await kb_main_for(uid), lang=lang); return
        token = data.split("_", 1)[1]
        key = CB_MAP.get(lang, {}).get(token)
        KB  = kb_for_lang(lang)
        info = KB.get(key) if key else None
        txt  = to_html(_clean_text(info["response"])) if info else "—"
        # показываем контент + Назад в список быстрых тем
        await show_loader_and_edit(query, txt, reply_markup=kb_back(lang, "menu_quick"), parse_mode="HTML", lang=lang); return

# ---------- свободный текст / верификация / формы ----------
async def free_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_user(update, inc_msg=1)
    lang = await get_pref_lang(update.effective_user.id)
    await update.message.chat.send_action(ChatAction.TYPING)

    # 1) Верификация шаги
    vf = context.user_data.get("verify")
    if vf:
        txt = (update.message.text or "").strip()
        step = vf.get("step", 1)
        expect_phone = vf.get("expect_phone") or ""
        email = vf.get("email") or ""
        lang = vf.get("lang") or lang

        if step == 1:
            if _phones_match(txt, expect_phone):
                try:
                    await _issue_and_send_otp(update.effective_user.id, email, lang)
                except Exception as e:
                    err = "❌ Не вдалося надіслати код на пошту. Повідомте HR." if lang=="uk" else "❌ No se pudo enviar el código al correo. Avisa a RR. HH."
                    await update.message.reply_text(err + f"\n{e}")
                    return
                vf["step"] = 3
                em_mask = _mask_email(email)
                hint = ("✉️ Надіслали код на " if lang=="uk" else "✉️ Enviamos un código a ") + f"<b>{html.escape(em_mask)}</b>\n" + \
                       ("Введіть 6 цифр. Команда для повторної відправки: /resend" if lang=="uk" else "Introduce 6 dígitos. Comando para reenviar: /resend")
                await update.message.reply_text(hint, parse_mode="HTML")
            else:
                msg = "❌ Номер не співпадає. Спробуйте ще раз." if lang=="uk" else "❌ El número no coincide. Intenta de nuevo."
                await update.message.reply_text(msg)
            return

        if step == 3:
            code = re.sub(r"\D+", "", txt)
            if len(code) != 6:
                warn = "Введіть саме 6 цифр коду." if lang=="uk" else "Introduce exactamente 6 dígitos."
                await update.message.reply_text(warn)
                return
            ok, reason = await _otp_verify(update.effective_user.id, code)
            if ok:
                await set_verified(update.effective_user.id, 1)
                context.user_data["verify"] = None
                done = "✅ Верифікацію пройдено. Доступ відкрито." if lang=="uk" else "✅ Verificación completada. Acceso concedido."
                await update.message.reply_text(done, reply_markup=await kb_main_for(update.effective_user.id))
                return
            if reason == "expired":
                msg = "⌛ Термін дії коду минув. Відправте новий: /resend" if lang=="uk" else "⌛ El código ha expirado. Reenvía con /resend"
            elif reason == "too_many":
                msg = "⛔ Забагато спроб. Запросіть новий код: /resend" if lang=="uk" else "⛔ Demasiados intentos. Pide un nuevo código: /resend"
            elif reason == "no_active":
                msg = "ℹ️ Активного коду немає. Запросіть новий: /resend" if lang=="uk" else "ℹ️ No hay código activo. Usa /resend"
            else:
                msg = "❌ Невірний код. Спробуйте ще." if lang=="uk" else "❌ Código incorrecto. Intenta de nuevo."
            await update.message.reply_text(msg)
            return

    # 2) Идёт заполнение формы?
    ff = context.user_data.get("form_fill")
    if ff:
        i = ff["i"]
        fields = ff["fields"]
        key = ff["key"]
        txt = (update.message.text or "").strip()
        ff["answers"].append({fields[i]: txt})
        ff["i"] += 1

        if ff["i"] >= len(fields):
            data_dict = {}
            for d in ff["answers"]:
                data_dict.update(d)
            await save_form_submission(update.effective_user.id, update.effective_user.username or "", key, data_dict)
            context.user_data["form_fill"] = None
            await update.message.reply_text("✅ Дані збережено. Дякуємо!" if lang=="uk" else "✅ Datos guardados. ¡Gracias!",
                                            reply_markup=await kb_main_for(update.effective_user.id))
            return
        else:
            next_field = fields[ff["i"]]
            prompt = ("✍️ <b>Вкажіть</b>: {field}" if lang=="uk" else "✍️ <b>Introduce</b>: {field}").format(field=next_field)
            await update.message.reply_text(prompt, parse_mode="HTML")
            return

    # 3) Если нет логина — трактуем как логин
    login = await get_user_login(update.effective_user.id)
    if not login:
        candidate = (update.message.text or "").strip()
        prof = await get_profile_by_login(candidate)
        if prof:
            await set_user_login(update.effective_user.id, candidate)  # verified=0
            await start_verification_flow(update, context)
            return
        else:
            await update.message.reply_text("❌ Не знайдено такий логін. Спробуйте ще раз або зверніться до HR." if lang=="uk" else "❌ No encontré este login. Intenta de nuevo o contacta RR. HH.")
            return

    # 4) Гейт
    if not is_admin(update.effective_user.id) and not await is_verified(update.effective_user.id):
        note = "🔒 Щоб отримати відповіді, пройдіть верифікацію (кнопка в меню)." if lang=="uk" else "🔒 Para ver respuestas, completa la verificación (botón en el menú)."
        await update.message.reply_text(note, reply_markup=await kb_main_for(update.effective_user.id))
        return

    # 5) Обычный FAQ-поиск
    text = update.message.text or ""
    hit = find_best_match(text, lang)
    await asyncio.sleep(0.1)
    if hit:
        # показываем ответ + Назад в быстрые темы
        await update.message.reply_text(to_html(_clean_text(hit)), parse_mode="HTML",
                                        reply_markup=kb_back(lang, "menu_quick"),
                                        disable_web_page_preview=True)
    else:
        await update.message.reply_text(TX["start_banner"][lang], parse_mode="HTML",
                                        reply_markup=await kb_main_for(update.effective_user.id),
                                        disable_web_page_preview=True)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    if context.user_data.get("form_fill"):
        context.user_data["form_fill"] = None
    if context.user_data.get("verify"):
        context.user_data["verify"] = None
    await update.message.reply_text("🚫 Заповнення скасовано." if lang=="uk" else "🚫 Formulario cancelado.",
                                    reply_markup=await kb_main_for(uid))
    return ConversationHandler.END

# ---- webapp (optional) ----
async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        raw = update.effective_message.web_app_data.data
        _ = json.loads(raw)
        await update.message.reply_text("✅ Datos recibidos desde la Mini App.", reply_markup=await kb_main_for(update.effective_user.id))
    except Exception as e:
        await update.message.reply_text(f"Error WebAppData: {e}", reply_markup=await kb_main_for(update.effective_user.id))

# ---- админки ----
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    if not is_admin(uid):
        await update.message.reply_text("⛔ Недостатньо прав (лише для адміністраторів)." if lang=="uk" else "⛔ Sin permisos (solo para administradores)."); return
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users"); total_users = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE last_seen >= datetime('now','-7 day')"); weekly = (await cur.fetchone())[0]
        cur = await db.execute("SELECT IFNULL(SUM(msg_count),0), IFNULL(SUM(click_count),0) FROM users"); msg_sum, click_sum = await cur.fetchone()
    txt = ("📊 <b>Статистика</b>" if lang=="uk" else "📊 <b>Estadísticas</b>") + "\n" + \
          ("• Користувачів всього: <b>{u}</b>\n• Активні за 7 днів: <b>{w}</b>\n• Повідомлень: <b>{m}</b>\n• Кліків: <b>{c}</b>\n"
           if lang=="uk" else
           "• Usuarios totales: <b>{u}</b>\n• Activos 7 días: <b>{w}</b>\n• Mensajes: <b>{m}</b>\n• Clicks: <b>{c}</b>\n").format(u=total_users,w=weekly,m=msg_sum,c=click_sum)
    await update.message.reply_html(txt, reply_markup=await kb_main_for(uid))

async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    if not is_admin(uid):
        await update.message.reply_text("⛔ Недостатньо прав (лише для адміністраторів)." if lang=="uk" else "⛔ Sin permisos (solo para administradores)."); return
    try:
        offset = int(context.args[0]) if len(context.args) >= 1 else 0
        limit  = int(context.args[1]) if len(context.args) >= 2 else 20
        limit  = max(1, min(limit, 100))
    except:
        offset, limit = 0, 20
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("""
            SELECT id, username, first_name, last_name, language_code, msg_count, click_count, last_seen, login
            FROM users ORDER BY last_seen DESC LIMIT ? OFFSET ?;
        """, (limit, offset))
        rows = await cur.fetchall()
    if not rows:
        await update.message.reply_text("Порожньо." if lang=="uk" else "Vacío.", reply_markup=await kb_main_for(uid)); return
    lines = []
    for uid2, username, fn, ln, tl, msgc, clk, last, login in rows:
        handle = f"@{username}" if username else ("(без username)" if lang=="uk" else "(sin username)")
        name = " ".join([x for x in [fn, ln] if x]).strip() or "—"
        login_s = login or "—"
        lines.append(f"• <b>{name}</b> {handle}\n  id: <code>{uid2}</code> | login: <code>{html.escape(login_s)}</code> | lang: {html.escape(tl or '—')} | msg: {msgc} | click: {clk} | last: {last}")
    title = ("👥 <b>Користувачі</b>\n" if lang=="uk" else "👥 <b>Usuarios</b>\n")
    nav = f"\n\n/users {offset+limit} {limit} ▶"
    await update.message.reply_html(title + "\n".join(lines) + nav, reply_markup=await kb_main_for(uid))

async def cmd_export_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    if not is_admin(uid):
        await update.message.reply_text("⛔ Недостатньо прав (лише для адміністраторів)." if lang=="uk" else "⛔ Sin permisos (solo para administradores)."); return
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute("""
            SELECT id, username, first_name, last_name, language_code, pref_lang, login, verified, is_bot, first_seen, last_seen, msg_count, click_count
            FROM users ORDER BY last_seen DESC
        """)
        rows = await cur.fetchall()
    buf = StringIO(); w = csv.writer(buf)
    w.writerow(["id","username","first_name","last_name","language_code","pref_lang","login","verified","is_bot","first_seen","last_seen","msg_count","click_count"])
    for r in rows: w.writerow(r)
    data_bytes = buf.getvalue().encode("utf-8-sig")
    bio = BytesIO(data_bytes); bio.name = "users_export.csv"
    await update.message.reply_document(document=InputFile(bio), caption="Експорт" if lang=="uk" else "Export")

async def cmd_setprofile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    if not is_admin(uid):
        await update.message.reply_text("⛔ Недостатньо прав (лише для адміністраторів)." if lang=="uk" else "⛔ Sin permisos (solo para administradores)."); return
    if len(context.args) < 2:
        await update.message.reply_text("Використання: /setprofile <login> <json>" if lang=="uk" else "Uso: /setprofile <login> <json>"); return
    login = context.args[0]
    json_str = " ".join(context.args[1:])
    try:
        data = json.loads(json_str)
    except Exception as e:
        await update.message.reply_text(f"JSON error: {e}"); return
    fields = ["full_name","position","team","email","phone","manager","vacation_left","salary_usd","extra_json"]
    payload = {k: data.get(k) for k in fields}
    if isinstance(payload.get("extra_json"), (dict, list)):
        payload["extra_json"] = json.dumps(payload["extra_json"], ensure_ascii=False)
    await upsert_profiles({login: {"login":login, **payload}})
    await update.message.reply_text(("✅ Профіль збережено: " if lang=="uk" else "✅ Perfil guardado: ") + login,
                                    reply_markup=await kb_main_for(uid))

async def cmd_import_profiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    if not is_admin(uid):
        await update.message.reply_text("⛔ Недостатньо прав (лише для адміністраторів)." if lang=="uk" else "⛔ Sin permisos (solo para administradores)."); return
    if not update.message.document:
        await update.message.reply_text("Прикріпіть CSV (login, ...)" if lang=="uk" else "Adjunta un CSV con perfiles (login, ...)."); return
    file = await context.bot.get_file(update.message.document.file_id)
    data = await file.download_as_bytearray()
    text = data.decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader); count = 0
    batch: Dict[str, dict] = {}
    for r in rows:
        login = (r.get("login") or "").strip()
        if not login: continue
        batch[login] = {
            "login": login,
            "full_name": _clean_text(r.get("full_name") or ""),
            "position":  _clean_text(r.get("position")  or ""),
            "team":      _clean_text(r.get("team") or r.get("department") or ""),
            "email":     (r.get("email") or "").strip(),
            "phone":     (r.get("phone") or "").strip(),
            "manager":   _clean_text(r.get("manager") or ""),
            "vacation_left": int((r.get("vacation_left") or "0").strip() or 0),
            "salary_usd":   int((r.get("salary_usd")   or "0").strip() or 0),
            "extra_json": None
        }
        count += 1
    await upsert_profiles(batch)
    await update.message.reply_text(("✅ Імпортовано: " if lang=="uk" else "✅ Importados: ") + str(count),
                                    reply_markup=await kb_main_for(uid))

async def cmd_dump_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("⛔ Лише для адмінів."); return
    login = " ".join(context.args).strip() or (await get_user_login(uid)) or ""
    if not login:
        await update.message.reply_text("Вкажіть логін: /dump_profile maria"); return
    p = await get_profile_by_login(login)
    if not p:
        await update.message.reply_text(f"Профіль '{login}' не знайдено."); return
    def mask_phone(s):
        d = _digits_only(s)
        return f"...{d[-6:]}" if len(d) >= 6 else d
    txt = (
        f"login: <b>{html.escape(p.get('login',''))}</b>\n"
        f"full_name: {html.escape(p.get('full_name',''))}\n"
        f"email: {html.escape(p.get('email',''))}\n"
        f"phone(raw): {html.escape(p.get('phone',''))}\n"
        f"phone(norm): {mask_phone(p.get('phone',''))}\n"
        f"position: {html.escape(p.get('position',''))}\n"
        f"team: {html.escape(p.get('team',''))}\n"
    )
    await update.message.reply_html(txt)

# ---- /refresh и автосинк ----
async def load_from_sheet_once():
    global KB_ES, KB_UK, FORMS_ES, FORMS_UK
    try:
        KB_es, KB_uk, FR_es, FR_uk, PROFILES = await fetch_sheet_configs()
        KB_ES.clear(); KB_ES.update(KB_es)
        KB_UK.clear(); KB_UK.update(KB_uk)
        FORMS_ES.clear(); FORMS_ES.update(FR_es)
        FORMS_UK.clear(); FORMS_UK.update(FR_uk)
        await upsert_profiles(PROFILES)
        log.info(f"[gsheet] loaded: KB_es={len(KB_ES)} KB_uk={len(KB_UK)} FORMS_es={len(FORMS_ES)} FORMS_uk={len(FORMS_UK)} PROFILES={len(PROFILES)}")
        return True, ""
    except Exception as e:
        log.error(f"[gsheet] load error: {e}")
        return False, str(e)

async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lang = await get_pref_lang(uid)
    if not is_admin(uid):
        await update.message.reply_text("⛔ Недостатньо прав (лише для адміністраторів)." if lang=="uk" else "⛔ Sin permisos (solo para administradores)."); return
    ok, err = await load_from_sheet_once()
    if ok:
        await update.message.reply_text("✅ Дані перезавантажено." if lang=="uk" else "✅ Datos recargados.", reply_markup=await kb_main_for(uid))
    else:
        await update.message.reply_text(("❌ Помилка завантаження: " if lang=="uk" else "❌ Error al cargar: ") + err, reply_markup=await kb_main_for(uid))

# ---------- сборка ----------
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    async def on_startup(_):
        await init_db()
        await load_from_sheet_once()
        if SYNC_INTERVAL_MIN > 0:
            async def _auto_sync_sheet():
                await asyncio.sleep(2)
                while True:
                    try:
                        await load_from_sheet_once()
                    except Exception as e:
                        log.error(f"[autosync] sheet error: {e}")
                    await asyncio.sleep(max(60, SYNC_INTERVAL_MIN*60))
            asyncio.create_task(_auto_sync_sheet())

    app.post_init = on_startup

    login_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={LOGIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_step)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="login_conv",
        persistent=False,
    )
    app.add_handler(login_conv)

    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("logout", cmd_logout))
    app.add_handler(CommandHandler("verify", cmd_verify))
    app.add_handler(CommandHandler("resend", cmd_resend))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("dump_profile", cmd_dump_profile))

    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("export_users", cmd_export_users))
    app.add_handler(CommandHandler("setprofile", cmd_setprofile))
    app.add_handler(CommandHandler("import_profiles", cmd_import_profiles))

    app.add_handler(CallbackQueryHandler(on_menu_click))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, free_text))
    app.add_handler(CommandHandler("cancel", cancel))

    return app

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("❌ BOT_TOKEN не задан. Укажи его в .env")
    log.info("Starting HR Assistant bot…")
    app = build_app()
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
