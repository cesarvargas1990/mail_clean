import os
import json
import multiprocessing as mp
from collections import defaultdict
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
PROCESSES = 12


def get_service():
    """Crea un cliente Gmail API aislado por proceso."""
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
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


def process_chunk(ids_chunk):
    """Procesa un grupo de mensajes en un proceso independiente."""
    service = get_service()
    from_local = defaultdict(int)
    to_local = defaultdict(int)

    for msg_id in ids_chunk:
        try:
            msg = service.users().messages().get(
                userId="me",
                id=msg_id,
                format="metadata",
                metadataHeaders=["From", "To"]
            ).execute()

            headers = msg.get("payload", {}).get("headers", [])

            for h in headers:
                name = h["name"]
                value = h["value"]

                if name == "From":
                    from_local[value] += 1
                elif name == "To":
                    to_local[value] += 1

        except:
            pass

    return dict(from_local), dict(to_local)


def merge_dicts(a, b):
    for k, v in b.items():
        a[k] = a.get(k, 0) + v


def process():
    print("🔐 Autenticando...")
    service = get_service()

    print("🔍 Obteniendo lista de IDs...")
    ids = []
    page = None

    while True:
        resp = service.users().messages().list(
            userId="me",
            pageToken=page,
            maxResults=500
        ).execute()

        ids.extend([m["id"] for m in resp.get("messages", [])])

        if "nextPageToken" in resp:
            page = resp["nextPageToken"]
        else:
            break

    print(f"📬 Total mensajes: {len(ids)}")

    # dividir en chunks
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

    # -------------------------------------------
    # 📁 ARCHIVO 1: detalle completo
    # -------------------------------------------
    with open("detalle_correos.txt", "w", encoding="utf-8") as f:
        f.write("===== REMITENTES =====\n")
        for sender, count in sorted(from_counter.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{count} → {sender}\n")

        f.write("\n\n===== DESTINATARIOS =====\n")
        for rec, count in sorted(to_counter.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{count} → {rec}\n")

    print("📄 Archivo generado: detalle_correos.txt")

    # -------------------------------------------
    # 📁 ARCHIVO 2: dominios agrupados
    # -------------------------------------------
    domain_map = defaultdict(int)

    # agregar dominios de remitentes
    for sender, count in from_counter.items():
        domain = extract_domain(sender)
        domain_map[domain] += count

    # agregar dominios de destinatarios
    for rec, count in to_counter.items():
        domain = extract_domain(rec)
        domain_map[domain] += count

    with open("dominios.txt", "w", encoding="utf-8") as f:
        f.write("===== DOMINIOS AGRUPADOS =====\n")
        for dom, count in sorted(domain_map.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{count} → {dom}\n")

    print("📄 Archivo generado: dominios.txt")

    print("\n✅ PROCESAMIENTO COMPLETO\n")


if __name__ == "__main__":
    process()
