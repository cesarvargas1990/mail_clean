import os
import time
import webbrowser
from collections import defaultdict

import requests
from msal import PublicClientApplication

# ==== CONFIG ====

# Usa el client_id de tu app registrada en Azure
CLIENT_ID = "537b2720-a1e6-4f38-804f-241ec44f5163"

# Para cuentas personales está bien usar "consumers".
# Si algo te falla, puedes probar con "common" o con tu tenant ID:
# AUTHORITY = "https://login.microsoftonline.com/9ba6ecc6-733d-413a-8bac-dd4062669fa4"
AUTHORITY = "https://login.microsoftonline.com/consumers"

# Permiso de solo lectura de correo
SCOPES = ["https://graph.microsoft.com/Mail.Read"]

# Número de procesos en paralelo (ya no se usa en Outlook optimizado,
# se mantiene para compatibilidad con llamadas previas)
PROCESSES = 12
HTTP_TIMEOUT_SECONDS = 30
PAGE_SIZE = 500
REQUEST_RETRIES = 6
RETRY_BASE_SECONDS = 2

TOKEN_CACHE_FILE = "token_outlook.json"
CURRENT_TOKEN_CACHE_FILE = TOKEN_CACHE_FILE
CURRENT_ACCESS_TOKEN = None
CANCEL_MESSAGE = "Operación cancelada por el usuario."


def _safe_user_key(user_email):
    user_email = (user_email or "").strip().lower()
    return user_email or "me"


def _safe_token_file(user_email):
    user_key = _safe_user_key(user_email)
    if user_key == "me":
        return "token_outlook.json"
    clean = "".join(c if c.isalnum() else "_" for c in user_key)
    return f"token_outlook_{clean}.json"


def get_report_files(user_email):
    user_key = _safe_user_key(user_email)
    safe = "".join(c if c.isalnum() else "_" for c in user_key)
    return {
        "detail": f"detalle_correos_outlook_{safe}.txt",
        "domain": f"dominios_outlook_{safe}.txt",
    }


# ==== AUTENTICACIÓN / CLIENTE GRAPH ====

def build_app():
    """Crea la app MSAL + cache usando archivo local."""
    from msal import SerializableTokenCache

    cache = SerializableTokenCache()
    if os.path.exists(CURRENT_TOKEN_CACHE_FILE):
        with open(CURRENT_TOKEN_CACHE_FILE, "r") as f:
            cache.deserialize(f.read())

    app = PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache,
    )

    return app, cache


def save_cache(cache):
    if cache.has_state_changed:
        with open(CURRENT_TOKEN_CACHE_FILE, "w") as f:
            f.write(cache.serialize())


def ensure_not_cancelled(stop_event=None):
    if stop_event is not None and stop_event.is_set():
        raise RuntimeError(CANCEL_MESSAGE)


def open_auth_url(url, log=print):
    try:
        webbrowser.open(url)
        logger = log if callable(log) else print
        logger(f"🌐 Abriendo navegador: {url}")
    except Exception:
        pass


def get_access_token(log=print, stop_event=None):
    """
    Obtiene un access token reutilizando la cache local.
    Si no hay token válido, usa device code flow (una sola vez).
    """
    app, cache = build_app()
    # Intentar silencioso
    accounts = app.get_accounts()
    result = None

    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])

    logger = log if callable(log) else print
    ensure_not_cancelled(stop_event)

    if not result:
        # Device Code flow (similar a OAuth con código en terminal)
        flow = app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise RuntimeError("No se pudo iniciar el device flow.")
        logger("🔐 Autenticación necesaria para Outlook:")
        logger(flow.get("message", ""))
        logger("(Ingresa el código, acepta permisos y vuelve a esta ventana.)")
        open_auth_url("https://www.microsoft.com/link", log=logger)
        ensure_not_cancelled(stop_event)

        result = app.acquire_token_by_device_flow(flow)

        if "access_token" not in result:
            raise RuntimeError(f"Error obteniendo token: {result}")

        save_cache(cache)

    return result["access_token"]


def get_service(access_token=None):
    """
    Igual concepto que en tu script de Gmail:
    devuelve un 'service' que aquí es un requests.Session() con el token puesto.
    """
    token = access_token or get_access_token()
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Prefer": f"odata.maxpagesize={PAGE_SIZE}",
    })
    return session


# ==== HELPERS DE CORREOS ====

def extract_domain(email: str) -> str:
    """Extrae el dominio de un correo."""
    email = email.lower().strip()
    if "<" in email and ">" in email:
        email = email[email.find("<") + 1:email.find(">")]
    if "@" in email:
        return email.split("@")[-1]
    return "desconocido"


def update_counters_from_message(
    msg,
    from_with_attachments,
    from_without_attachments,
    to_with_attachments,
    to_without_attachments,
):
    has_attachments = bool(msg.get("hasAttachments", False))

    from_obj = msg.get("from", {}).get("emailAddress", {})
    from_addr = from_obj.get("address")
    if from_addr:
        if has_attachments:
            from_with_attachments[from_addr] += 1
        else:
            from_without_attachments[from_addr] += 1

    for rec in msg.get("toRecipients", []):
        addr = rec.get("emailAddress", {}).get("address")
        if addr:
            if has_attachments:
                to_with_attachments[addr] += 1
            else:
                to_without_attachments[addr] += 1


def should_retry_status(status_code):
    return status_code in (429, 500, 502, 503, 504)


def fetch_with_retry(service, url, logger, context, stop_event=None):
    last_error = None

    for attempt in range(1, REQUEST_RETRIES + 1):
        ensure_not_cancelled(stop_event)
        try:
            resp = service.get(url, timeout=HTTP_TIMEOUT_SECONDS)

            if should_retry_status(resp.status_code):
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = int(retry_after)
                else:
                    wait_seconds = RETRY_BASE_SECONDS * attempt
                logger(f"⚠️ {context}: HTTP {resp.status_code}. Reintento {attempt}/{REQUEST_RETRIES} en {wait_seconds}s...")
                for _ in range(wait_seconds * 10):
                    ensure_not_cancelled(stop_event)
                    time.sleep(0.1)
                continue

            if resp.status_code != 200:
                raise RuntimeError(f"{context}: HTTP {resp.status_code} - {resp.text[:250]}")

            return resp

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_error = exc
            wait_seconds = RETRY_BASE_SECONDS * attempt
            logger(f"⚠️ {context}: timeout/conexión. Reintento {attempt}/{REQUEST_RETRIES} en {wait_seconds}s...")
            for _ in range(wait_seconds * 10):
                ensure_not_cancelled(stop_event)
                time.sleep(0.1)

    if last_error:
        raise RuntimeError(f"{context}: fallo de red tras {REQUEST_RETRIES} reintentos ({last_error})")
    raise RuntimeError(f"{context}: no se pudo completar la petición tras reintentos")


def count_messages_paged(service, log=print, stop_event=None):
    """Cuenta FROM/TO directamente desde la lista paginada de mensajes."""
    logger = log if callable(log) else print
    logger("🔍 Obteniendo y procesando mensajes desde Outlook...")

    from_with_attachments = defaultdict(int)
    from_without_attachments = defaultdict(int)
    to_with_attachments = defaultdict(int)
    to_without_attachments = defaultdict(int)
    total_messages = 0
    page_count = 0

    url = f"https://graph.microsoft.com/v1.0/me/messages?$select=from,toRecipients,hasAttachments&$top={PAGE_SIZE}"

    while url:
        ensure_not_cancelled(stop_event)
        resp = fetch_with_retry(service, url, logger, "Listado de mensajes Outlook", stop_event=stop_event)

        data = resp.json()
        batch = data.get("value", [])
        page_count += 1

        for msg in batch:
            total_messages += 1
            update_counters_from_message(
                msg,
                from_with_attachments,
                from_without_attachments,
                to_with_attachments,
                to_without_attachments,
            )

        if page_count % 10 == 0:
            logger(f"⏳ Avance Outlook: {total_messages} mensajes procesados...")

        url = data.get("@odata.nextLink")

    return (
        dict(from_with_attachments),
        dict(from_without_attachments),
        dict(to_with_attachments),
        dict(to_without_attachments),
        total_messages,
    )


def write_counter_block(file_obj, title, with_attachments, without_attachments):
    file_obj.write(f"===== {title} =====\n")
    file_obj.write("--- CON ADJUNTOS ---\n")
    for item, count in sorted(with_attachments.items(), key=lambda x: x[1], reverse=True):
        file_obj.write(f"{count} → {item}\n")

    file_obj.write("\n--- SIN ADJUNTOS ---\n")
    for item, count in sorted(without_attachments.items(), key=lambda x: x[1], reverse=True):
        file_obj.write(f"{count} → {item}\n")


# ==== FLUJO PRINCIPAL ====

def process(user_email=None, processes=PROCESSES, log=print, stop_event=None, force_refresh=False, force_reauth=False):
    global CURRENT_TOKEN_CACHE_FILE, CURRENT_ACCESS_TOKEN
    user_id = (user_email or "").strip() or "me"
    logger = log if callable(log) else print
    CURRENT_TOKEN_CACHE_FILE = _safe_token_file(user_id)
    report_files = get_report_files(user_id)

    if force_reauth and os.path.exists(CURRENT_TOKEN_CACHE_FILE):
        try:
            os.remove(CURRENT_TOKEN_CACHE_FILE)
            logger("🔁 Token anterior eliminado para forzar nuevo login en Outlook.")
        except Exception:
            logger("⚠️ No se pudo eliminar el token anterior de Outlook; se intentará continuar.")

    logger("🔐 Preparando autenticación (Outlook / Hotmail)...")
    logger(f"⚙️ Modo optimizado activo (parámetro procesos={processes}).")
    # Forzamos obtener token una vez aquí para que luego en los procesos hijos
    # se pueda reutilizar la cache sin mostrar device code varias veces.
    CURRENT_ACCESS_TOKEN = get_access_token(log=logger, stop_event=stop_event)

    service = get_service(CURRENT_ACCESS_TOKEN)

    (
        from_with_attachments,
        from_without_attachments,
        to_with_attachments,
        to_without_attachments,
        total_messages,
    ) = count_messages_paged(service, log=logger, stop_event=stop_event)

    from_counter = defaultdict(int)
    to_counter = defaultdict(int)

    for sender, count in from_with_attachments.items():
        from_counter[sender] += count
    for sender, count in from_without_attachments.items():
        from_counter[sender] += count

    for rec, count in to_with_attachments.items():
        to_counter[rec] += count
    for rec, count in to_without_attachments.items():
        to_counter[rec] += count

    logger(f"📬 Total mensajes: {total_messages}")

    if not total_messages:
        logger("No se encontraron mensajes.")
        return {
            "files": [report_files["detail"], report_files["domain"]],
            "summary": {
                "source": "new_scan",
                "last_scan": None,
                "detail": {
                    "received_with_attachments": 0,
                    "received_without_attachments": 0,
                    "sent_with_attachments": 0,
                    "sent_without_attachments": 0,
                },
            },
        }

    logger("⚡ Procesamiento Outlook optimizado completado.")

    # ----- Archivo 1: detalle completo -----
    with open(report_files["detail"], "w", encoding="utf-8") as f:
        write_counter_block(
            f,
            "REMITENTES (RECIBIDOS)",
            from_with_attachments,
            from_without_attachments,
        )

        f.write("\n\n")

        write_counter_block(
            f,
            "DESTINATARIOS (ENVIADOS)",
            to_with_attachments,
            to_without_attachments,
        )

    logger(f"📄 Archivo generado: {report_files['detail']}")

    # ----- Archivo 2: dominios agrupados -----
    domain_from_with_attachments = defaultdict(int)
    domain_from_without_attachments = defaultdict(int)
    domain_to_with_attachments = defaultdict(int)
    domain_to_without_attachments = defaultdict(int)

    for sender, count in from_with_attachments.items():
        domain_from_with_attachments[extract_domain(sender)] += count

    for sender, count in from_without_attachments.items():
        domain_from_without_attachments[extract_domain(sender)] += count

    for rec, count in to_with_attachments.items():
        domain_to_with_attachments[extract_domain(rec)] += count

    for rec, count in to_without_attachments.items():
        domain_to_without_attachments[extract_domain(rec)] += count

    with open(report_files["domain"], "w", encoding="utf-8") as f:
        write_counter_block(
            f,
            "DOMINIOS REMITENTES (RECIBIDOS)",
            domain_from_with_attachments,
            domain_from_without_attachments,
        )

        f.write("\n\n")

        write_counter_block(
            f,
            "DOMINIOS DESTINATARIOS (ENVIADOS)",
            domain_to_with_attachments,
            domain_to_without_attachments,
        )

    logger(f"📄 Archivo generado: {report_files['domain']}")
    logger("\n✅ PROCESAMIENTO COMPLETO (OUTLOOK)\n")

    return {
        "files": [report_files["detail"], report_files["domain"]],
        "summary": {
            "source": "new_scan",
            "last_scan": None,
            "detail": {
                "received_with_attachments": sum(from_with_attachments.values()),
                "received_without_attachments": sum(from_without_attachments.values()),
                "sent_with_attachments": sum(to_with_attachments.values()),
                "sent_without_attachments": sum(to_without_attachments.values()),
            },
        },
    }


if __name__ == "__main__":
    process()
