import os
import json
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime, timedelta
from glob import glob
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
PROCESSES = 12
TOKEN_GMAIL_DEFAULT = "token_gmail.json"
DETAIL_REPORT_FILE = "detalle_correos.txt"
DOMAIN_REPORT_FILE = "dominios.txt"
SCAN_STATE_FILE = "gmail_scan_state.json"
MAX_SCAN_AGE_DAYS = 2
CURRENT_USER_ID = "me"
CURRENT_TOKEN_FILE = TOKEN_GMAIL_DEFAULT
CREDENTIAL_CANDIDATES = ["credentials.json", "client_secret.json"]


def _safe_token_file(user_email):
    user_email = (user_email or "").strip().lower()
    if not user_email or user_email == "me":
        return TOKEN_GMAIL_DEFAULT
    clean = "".join(c if c.isalnum() else "_" for c in user_email)
    return f"token_gmail_{clean}.json"


def _safe_user_key(user_email):
    user_email = (user_email or "").strip().lower()
    return user_email or "me"


def get_report_files(user_email):
    user_key = _safe_user_key(user_email)
    safe = "".join(c if c.isalnum() else "_" for c in user_key)
    return {
        "detail": f"detalle_correos_{safe}.txt",
        "domain": f"dominios_{safe}.txt",
    }


def load_scan_state():
    if not os.path.exists(SCAN_STATE_FILE):
        return {}

    try:
        with open(SCAN_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def save_scan_state(state):
    with open(SCAN_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def reports_exist(report_files):
    return os.path.exists(report_files["detail"]) and os.path.exists(report_files["domain"])


def should_refresh_scan(user_id, token_file, report_files):
    if not reports_exist(report_files):
        return True, "No existen reportes previos."

    if not os.path.exists(token_file):
        return True, "No hay credenciales guardadas para este correo."

    state = load_scan_state()
    user_key = _safe_user_key(user_id)
    user_state = state.get(user_key, {})
    last_scan = user_state.get("last_scan")

    if not last_scan:
        return True, "No hay fecha de último escaneo registrada."

    try:
        last_scan_dt = datetime.fromisoformat(last_scan)
    except Exception:
        return True, "La fecha de último escaneo es inválida."

    if datetime.now() - last_scan_dt > timedelta(days=MAX_SCAN_AGE_DAYS):
        return True, "La sesión superó los 2 días; se requiere nuevo login."

    return False, "Escaneo reciente; se usarán archivos existentes."


def update_last_scan(user_id, report_files):
    state = load_scan_state()
    user_key = _safe_user_key(user_id)
    state[user_key] = {
        "last_scan": datetime.now().isoformat(timespec="seconds"),
        "files": [report_files["detail"], report_files["domain"]],
    }
    save_scan_state(state)


def get_last_scan(user_id):
    state = load_scan_state()
    user_state = state.get(_safe_user_key(user_id), {})
    return user_state.get("last_scan")


def find_client_secrets_file():
    for path in CREDENTIAL_CANDIDATES:
        if os.path.exists(path):
            return path

    extra_candidates = sorted(glob("client_secret*.json"))
    if extra_candidates:
        return extra_candidates[0]

    return None


def get_service(token_file=TOKEN_GMAIL_DEFAULT):
    """Crea un cliente Gmail API aislado por proceso."""
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    else:
        client_secrets_file = find_client_secrets_file()
        if not client_secrets_file:
            raise RuntimeError(
                "No se encontró archivo de credenciales OAuth. "
                "Coloca credentials.json o client_secret.json en la carpeta del proyecto."
            )

        flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(token_file, "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def extract_domain(email):
    """Extrae el dominio de un correo."""
    email = email.lower().strip()
    if "<" in email and ">" in email:
        # formato "Nombre <correo@dominio>"
        email = email[email.find("<") + 1:email.find(">")]
    if "@" in email:
        return email.split("@")[-1]
    return "desconocido"


def message_has_attachments(payload):
    if not payload:
        return False

    body = payload.get("body", {})
    filename = payload.get("filename", "")

    if filename and body.get("attachmentId"):
        return True

    for part in payload.get("parts", []):
        if message_has_attachments(part):
            return True

    return False


def update_message_party_counters(
    header_name,
    header_value,
    has_attachments,
    from_local,
    to_local,
    from_with_attachments_local,
    from_without_attachments_local,
    to_with_attachments_local,
    to_without_attachments_local,
):
    if header_name == "From":
        from_local[header_value] += 1
        if has_attachments:
            from_with_attachments_local[header_value] += 1
        else:
            from_without_attachments_local[header_value] += 1
        return

    if header_name == "To":
        to_local[header_value] += 1
        if has_attachments:
            to_with_attachments_local[header_value] += 1
        else:
            to_without_attachments_local[header_value] += 1


def process_chunk(ids_chunk):
    """Procesa un grupo de mensajes en un proceso independiente."""
    service = get_service(CURRENT_TOKEN_FILE)
    from_local = defaultdict(int)
    to_local = defaultdict(int)
    from_with_attachments_local = defaultdict(int)
    from_without_attachments_local = defaultdict(int)
    to_with_attachments_local = defaultdict(int)
    to_without_attachments_local = defaultdict(int)

    for msg_id in ids_chunk:
        try:
            msg = service.users().messages().get(
                userId=CURRENT_USER_ID,
                id=msg_id,
                format="full"
            ).execute()

            payload = msg.get("payload", {})
            headers = payload.get("headers", [])
            has_attachments = message_has_attachments(payload)

            for h in headers:
                name = h["name"]
                value = h["value"]
                update_message_party_counters(
                    name,
                    value,
                    has_attachments,
                    from_local,
                    to_local,
                    from_with_attachments_local,
                    from_without_attachments_local,
                    to_with_attachments_local,
                    to_without_attachments_local,
                )

        except Exception:
            pass

    return (
        dict(from_local),
        dict(to_local),
        dict(from_with_attachments_local),
        dict(from_without_attachments_local),
        dict(to_with_attachments_local),
        dict(to_without_attachments_local),
    )


def merge_dicts(a, b):
    for k, v in b.items():
        a[k] = a.get(k, 0) + v


def init_worker(user_id, token_file):
    global CURRENT_USER_ID, CURRENT_TOKEN_FILE
    CURRENT_USER_ID = user_id
    CURRENT_TOKEN_FILE = token_file


def list_message_ids(service, user_id):
    ids = []
    page = None
    while True:
        resp = service.users().messages().list(
            userId=user_id,
            pageToken=page,
            maxResults=500
        ).execute()

        ids.extend([m["id"] for m in resp.get("messages", [])])

        if "nextPageToken" in resp:
            page = resp["nextPageToken"]
        else:
            break
    return ids


def write_counter_block(file_obj, title, with_attachments, without_attachments):
    file_obj.write(f"===== {title} =====\n")
    file_obj.write("--- CON ADJUNTOS ---\n")
    for item, count in sorted(with_attachments.items(), key=lambda x: x[1], reverse=True):
        file_obj.write(f"{count} → {item}\n")

    file_obj.write("\n--- SIN ADJUNTOS ---\n")
    for item, count in sorted(without_attachments.items(), key=lambda x: x[1], reverse=True):
        file_obj.write(f"{count} → {item}\n")


def write_detail_report(
    from_with_attachments,
    from_without_attachments,
    to_with_attachments,
    to_without_attachments,
    path=DETAIL_REPORT_FILE,
):
    with open(path, "w", encoding="utf-8") as f:
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


def write_domain_report(
    from_with_attachments,
    from_without_attachments,
    to_with_attachments,
    to_without_attachments,
    path=DOMAIN_REPORT_FILE,
):
    domain_from_map = defaultdict(int)
    domain_to_map = defaultdict(int)
    domain_from_with_attachments = defaultdict(int)
    domain_from_without_attachments = defaultdict(int)
    domain_to_with_attachments = defaultdict(int)
    domain_to_without_attachments = defaultdict(int)

    for sender, count in from_with_attachments.items():
        domain = extract_domain(sender)
        domain_from_map[domain] += count
        domain_from_with_attachments[domain] += count

    for sender, count in from_without_attachments.items():
        domain = extract_domain(sender)
        domain_from_map[domain] += count
        domain_from_without_attachments[domain] += count

    for rec, count in to_with_attachments.items():
        domain = extract_domain(rec)
        domain_to_map[domain] += count
        domain_to_with_attachments[domain] += count

    for rec, count in to_without_attachments.items():
        domain = extract_domain(rec)
        domain_to_map[domain] += count
        domain_to_without_attachments[domain] += count

    with open(path, "w", encoding="utf-8") as f:
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


def sum_counter_lines(text):
    total = 0
    for line in text.splitlines():
        line = line.strip()
        if "→" not in line:
            continue
        raw_count = line.split("→", 1)[0].strip()
        try:
            total += int(raw_count)
        except ValueError:
            continue
    return total


def extract_sections(content, header_a, header_b):
    idx_a = content.find(header_a)
    idx_b = content.find(header_b)
    if idx_a == -1 or idx_b == -1:
        return None, None

    if idx_a < idx_b:
        section_a = content[idx_a + len(header_a):idx_b].strip()
        section_b = content[idx_b + len(header_b):].strip()
    else:
        section_b = content[idx_b + len(header_b):idx_a].strip()
        section_a = content[idx_a + len(header_a):].strip()

    return section_a, section_b


def extract_attachment_sections(block_content):
    return extract_sections(block_content, "--- CON ADJUNTOS ---", "--- SIN ADJUNTOS ---")


def parse_detail_summary(path=DETAIL_REPORT_FILE):
    if not os.path.exists(path):
        return {
            "received_with_attachments": 0,
            "received_without_attachments": 0,
            "sent_with_attachments": 0,
            "sent_without_attachments": 0,
        }

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    received_block, sent_block = extract_sections(
        content,
        "===== REMITENTES (RECIBIDOS) =====",
        "===== DESTINATARIOS (ENVIADOS) =====",
    )

    if received_block is None or sent_block is None:
        return {
            "received_with_attachments": 0,
            "received_without_attachments": 0,
            "sent_with_attachments": 0,
            "sent_without_attachments": 0,
        }

    received_with, received_without = extract_attachment_sections(received_block)
    sent_with, sent_without = extract_attachment_sections(sent_block)

    return {
        "received_with_attachments": sum_counter_lines(received_with or ""),
        "received_without_attachments": sum_counter_lines(received_without or ""),
        "sent_with_attachments": sum_counter_lines(sent_with or ""),
        "sent_without_attachments": sum_counter_lines(sent_without or ""),
    }


def build_summary(user_id, source, report_files):
    detail = parse_detail_summary(report_files["detail"])
    return {
        "source": source,
        "last_scan": get_last_scan(user_id),
        "detail": detail,
    }


def process(user_email=None, processes=PROCESSES, log=print):
    user_id = (user_email or "").strip() or "me"
    token_file = _safe_token_file(user_id)
    report_files = get_report_files(user_id)
    detail_report_file = report_files["detail"]
    domain_report_file = report_files["domain"]
    logger = log if callable(log) else print

    refresh_required, refresh_reason = should_refresh_scan(user_id, token_file, report_files)
    if not refresh_required:
        logger(f"ℹ️ {refresh_reason}")
        logger("📁 Cargando reportes existentes sin reautenticación.")
        return {
            "files": [detail_report_file, domain_report_file],
            "summary": build_summary(user_id, "cache", report_files),
        }

    logger(f"ℹ️ {refresh_reason}")
    if os.path.exists(token_file):
        try:
            os.remove(token_file)
            logger("🔁 Token anterior eliminado para forzar nuevo login.")
        except Exception:
            logger("⚠️ No se pudo eliminar el token anterior; se intentará continuar.")

    logger("🔐 Autenticando...")
    service = get_service(token_file)

    logger("🔍 Obteniendo lista de IDs...")
    ids = list_message_ids(service, user_id)

    logger(f"📬 Total mensajes: {len(ids)}")

    # dividir en chunks
    chunk_size = len(ids) // processes + 1
    chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    logger(f"⚡ Procesando en paralelo con {processes} procesos...")

    if chunks:
        with mp.Pool(processes, initializer=init_worker, initargs=(user_id, token_file)) as pool:
            results = pool.map(process_chunk, chunks)
    else:
        results = []

    from_counter = {}
    to_counter = {}
    from_with_attachments = {}
    from_without_attachments = {}
    to_with_attachments = {}
    to_without_attachments = {}

    for f, t, f_with, f_without, t_with, t_without in results:
        merge_dicts(from_counter, f)
        merge_dicts(to_counter, t)
        merge_dicts(from_with_attachments, f_with)
        merge_dicts(from_without_attachments, f_without)
        merge_dicts(to_with_attachments, t_with)
        merge_dicts(to_without_attachments, t_without)

    # -------------------------------------------
    # 📁 ARCHIVO 1: detalle completo
    # -------------------------------------------
    write_detail_report(
        from_with_attachments,
        from_without_attachments,
        to_with_attachments,
        to_without_attachments,
        detail_report_file,
    )

    logger(f"📄 Archivo generado: {detail_report_file}")

    # -------------------------------------------
    # 📁 ARCHIVO 2: dominios por recibidos/enviados
    # -------------------------------------------
    write_domain_report(
        from_with_attachments,
        from_without_attachments,
        to_with_attachments,
        to_without_attachments,
        domain_report_file,
    )

    logger(f"📄 Archivo generado: {domain_report_file}")

    update_last_scan(user_id, report_files)
    logger(f"🗓️ Fecha de último escaneo guardada en: {SCAN_STATE_FILE}")

    logger("\n✅ PROCESAMIENTO COMPLETO\n")
    return {
        "files": [detail_report_file, domain_report_file],
        "summary": build_summary(user_id, "new_scan", report_files),
    }


if __name__ == "__main__":
    process()
