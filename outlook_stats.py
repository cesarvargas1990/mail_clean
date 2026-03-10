import os
import multiprocessing as mp
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

# Número de procesos en paralelo
PROCESSES = 12
CHUNK_TARGET_SIZE = 300
HTTP_TIMEOUT_SECONDS = 30

TOKEN_CACHE_FILE = "token_outlook.json"
CURRENT_TOKEN_CACHE_FILE = TOKEN_CACHE_FILE
CURRENT_ACCESS_TOKEN = None


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


def get_access_token(log=print):
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

    if not result:
        # Device Code flow (similar a OAuth con código en terminal)
        flow = app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise RuntimeError("No se pudo iniciar el device flow.")
        logger("🔐 Autenticación necesaria para Outlook:")
        logger(flow.get("message", ""))
        logger("(Ingresa el código, acepta permisos y vuelve a esta ventana.)")

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


def list_all_message_ids(service, log=print):
    """
    Lista TODOS los IDs de mensajes usando Microsoft Graph:
    GET /me/messages?$select=id&$top=500 con paginación @odata.nextLink
    """
    logger = log if callable(log) else print
    logger("🔍 Obteniendo lista de IDs desde Outlook...")
    ids = []
    url = "https://graph.microsoft.com/v1.0/me/messages?$select=id&$top=500"

    while url:
        resp = service.get(url, timeout=HTTP_TIMEOUT_SECONDS)
        if resp.status_code != 200:
            logger(f"❌ Error al listar mensajes: {resp.status_code}")
            break

        data = resp.json()
        for m in data.get("value", []):
            ids.append(m["id"])

        url = data.get("@odata.nextLink")

    return ids


def process_chunk(ids_chunk):
    """
    Procesa un grupo de mensajes en un proceso separado,
    igual que tu script de Gmail.
    """
    service = get_service(CURRENT_ACCESS_TOKEN)
    from_local = defaultdict(int)
    to_local = defaultdict(int)

    for msg_id in ids_chunk:
        try:
            # Solo necesitamos remitente y destinatarios
            url = (
                "https://graph.microsoft.com/v1.0/me/messages/"
                f"{msg_id}?$select=from,toRecipients"
            )
            resp = service.get(url, timeout=HTTP_TIMEOUT_SECONDS)
            if resp.status_code != 200:
                continue

            msg = resp.json()

            # From
            from_obj = msg.get("from", {}).get("emailAddress", {})
            from_addr = from_obj.get("address")
            if from_addr:
                from_local[from_addr] += 1

            # To (lista)
            for rec in msg.get("toRecipients", []):
                addr = rec.get("emailAddress", {}).get("address")
                if addr:
                    to_local[addr] += 1

        except Exception:
            # No rompemos el proceso por un correo raro
            continue

    return dict(from_local), dict(to_local)


def merge_dicts(a, b):
    for k, v in b.items():
        a[k] = a.get(k, 0) + v


def init_worker(token_cache_file, access_token):
    global CURRENT_TOKEN_CACHE_FILE, CURRENT_ACCESS_TOKEN
    CURRENT_TOKEN_CACHE_FILE = token_cache_file
    CURRENT_ACCESS_TOKEN = access_token


def write_counter_block(file_obj, title, with_attachments, without_attachments):
    file_obj.write(f"===== {title} =====\n")
    file_obj.write("--- CON ADJUNTOS ---\n")
    for item, count in sorted(with_attachments.items(), key=lambda x: x[1], reverse=True):
        file_obj.write(f"{count} → {item}\n")

    file_obj.write("\n--- SIN ADJUNTOS ---\n")
    for item, count in sorted(without_attachments.items(), key=lambda x: x[1], reverse=True):
        file_obj.write(f"{count} → {item}\n")


# ==== FLUJO PRINCIPAL ====

def process(user_email=None, processes=PROCESSES, log=print):
    global CURRENT_TOKEN_CACHE_FILE, CURRENT_ACCESS_TOKEN
    user_id = (user_email or "").strip() or "me"
    logger = log if callable(log) else print
    CURRENT_TOKEN_CACHE_FILE = _safe_token_file(user_id)
    report_files = get_report_files(user_id)

    logger("🔐 Preparando autenticación (Outlook / Hotmail)...")
    # Forzamos obtener token una vez aquí para que luego en los procesos hijos
    # se pueda reutilizar la cache sin mostrar device code varias veces.
    CURRENT_ACCESS_TOKEN = get_access_token(log=logger)

    service = get_service(CURRENT_ACCESS_TOKEN)

    ids = list_all_message_ids(service, log=logger)
    logger(f"📬 Total mensajes: {len(ids)}")

    if not ids:
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

    # Dividir IDs en chunks para multiproceso
    chunk_size = max(1, CHUNK_TARGET_SIZE)
    chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    logger(f"⚡ Procesando en paralelo con {processes} procesos ({len(chunks)} bloques)...")

    results = []
    with mp.Pool(processes, initializer=init_worker, initargs=(CURRENT_TOKEN_CACHE_FILE, CURRENT_ACCESS_TOKEN)) as pool:
        total_chunks = len(chunks)
        done_chunks = 0
        for result in pool.imap_unordered(process_chunk, chunks):
            results.append(result)
            done_chunks += 1
            logger(f"⏳ Avance Outlook: {done_chunks}/{total_chunks} bloques")

    from_counter = {}
    to_counter = {}

    for f, t in results:
        merge_dicts(from_counter, f)
        merge_dicts(to_counter, t)

    # ----- Archivo 1: detalle completo -----
    with open(report_files["detail"], "w", encoding="utf-8") as f:
        write_counter_block(
            f,
            "REMITENTES (RECIBIDOS)",
            {},
            from_counter,
        )

        f.write("\n\n")

        write_counter_block(
            f,
            "DESTINATARIOS (ENVIADOS)",
            {},
            to_counter,
        )

    logger(f"📄 Archivo generado: {report_files['detail']}")

    # ----- Archivo 2: dominios agrupados -----
    domain_from_map = defaultdict(int)
    domain_to_map = defaultdict(int)

    for sender, count in from_counter.items():
        domain_from_map[extract_domain(sender)] += count

    for rec, count in to_counter.items():
        domain_to_map[extract_domain(rec)] += count

    with open(report_files["domain"], "w", encoding="utf-8") as f:
        write_counter_block(
            f,
            "DOMINIOS REMITENTES (RECIBIDOS)",
            {},
            domain_from_map,
        )

        f.write("\n\n")

        write_counter_block(
            f,
            "DOMINIOS DESTINATARIOS (ENVIADOS)",
            {},
            domain_to_map,
        )

    logger(f"📄 Archivo generado: {report_files['domain']}")
    logger("\n✅ PROCESAMIENTO COMPLETO (OUTLOOK)\n")

    return {
        "files": [report_files["detail"], report_files["domain"]],
        "summary": {
            "source": "new_scan",
            "last_scan": None,
            "detail": {
                "received_with_attachments": 0,
                "received_without_attachments": sum(from_counter.values()),
                "sent_with_attachments": 0,
                "sent_without_attachments": sum(to_counter.values()),
            },
        },
    }


if __name__ == "__main__":
    process()
