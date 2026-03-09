import os
import json
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

TOKEN_CACHE_FILE = "token_outlook.json"


# ==== AUTENTICACIÓN / CLIENTE GRAPH ====

def build_app():
    """Crea la app MSAL + cache usando archivo local."""
    from msal import SerializableTokenCache

    cache = SerializableTokenCache()
    if os.path.exists(TOKEN_CACHE_FILE):
        with open(TOKEN_CACHE_FILE, "r") as f:
            cache.deserialize(f.read())

    app = PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache,
    )

    return app, cache


def save_cache(cache):
    if cache.has_state_changed:
        with open(TOKEN_CACHE_FILE, "w") as f:
            f.write(cache.serialize())


def get_access_token():
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

    if not result:
        # Device Code flow (similar a OAuth con código en terminal)
        flow = app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise RuntimeError("No se pudo iniciar el device flow.")
        print("\n🔐 Autenticación necesaria para Outlook:")
        print(flow["message"])
        print("(Copia/abre la URL que te da Microsoft, pega el código y acepta.)\n")

        result = app.acquire_token_by_device_flow(flow)

        if "access_token" not in result:
            raise RuntimeError(f"Error obteniendo token: {result}")

        save_cache(cache)

    return result["access_token"]


def get_service():
    """
    Igual concepto que en tu script de Gmail:
    devuelve un 'service' que aquí es un requests.Session() con el token puesto.
    """
    token = get_access_token()
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


def list_all_message_ids(service):
    """
    Lista TODOS los IDs de mensajes usando Microsoft Graph:
    GET /me/messages?$select=id&$top=500 con paginación @odata.nextLink
    """
    print("🔍 Obteniendo lista de IDs desde Outlook...")
    ids = []
    url = "https://graph.microsoft.com/v1.0/me/messages?$select=id&$top=500"

    while url:
        resp = service.get(url)
        if resp.status_code != 200:
            print("Error al listar mensajes:", resp.status_code, resp.text)
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
    service = get_service()
    from_local = defaultdict(int)
    to_local = defaultdict(int)

    for msg_id in ids_chunk:
        try:
            # Solo necesitamos remitente y destinatarios
            url = (
                "https://graph.microsoft.com/v1.0/me/messages/"
                f"{msg_id}?$select=from,toRecipients"
            )
            resp = service.get(url)
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


# ==== FLUJO PRINCIPAL ====

def process():
    print("🔐 Preparando autenticación (Outlook / Hotmail)...")
    # Forzamos obtener token una vez aquí para que luego en los procesos hijos
    # se pueda reutilizar la cache sin mostrar device code varias veces.
    _ = get_access_token()

    service = get_service()

    ids = list_all_message_ids(service)
    print(f"📬 Total mensajes: {len(ids)}")

    if not ids:
        print("No se encontraron mensajes.")
        return

    # Dividir IDs en chunks para multiproceso
    chunk_size = len(ids) // PROCESSES + 1
    chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    print(f"⚡ Procesando en paralelo con {PROCESSES} procesos...")

    with mp.Pool(PROCESSES) as pool:
        results = pool.map(process_chunk, chunks)

    from_counter = {}
    to_counter = {}

    for f, t in results:
        merge_dicts(from_counter, f)
        merge_dicts(to_counter, t)

    # ----- Archivo 1: detalle completo -----
    with open("detalle_correos_outlook.txt", "w", encoding="utf-8") as f:
        f.write("===== REMITENTES (FROM) =====\n")
        for sender, count in sorted(from_counter.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{count} → {sender}\n")

        f.write("\n\n===== DESTINATARIOS (TO) =====\n")
        for rec, count in sorted(to_counter.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{count} → {rec}\n")

    print("📄 Archivo generado: detalle_correos_outlook.txt")

    # ----- Archivo 2: dominios agrupados -----
    domain_map = defaultdict(int)

    for sender, count in from_counter.items():
        domain_map[extract_domain(sender)] += count

    for rec, count in to_counter.items():
        domain_map[extract_domain(rec)] += count

    with open("dominios_outlook.txt", "w", encoding="utf-8") as f:
        f.write("===== DOMINIOS AGRUPADOS (Outlook) =====\n")
        for dom, count in sorted(domain_map.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{count} → {dom}\n")

    print("📄 Archivo generado: dominios_outlook.txt")
    print("\n✅ PROCESAMIENTO COMPLETO (OUTLOOK)\n")


if __name__ == "__main__":
    process()
