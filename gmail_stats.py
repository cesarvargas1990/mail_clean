import os
import json
import multiprocessing as mp
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
PROCESSES = 12  


def get_service():
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def process_chunk(ids_chunk):
    """Cada proceso usa su propio cliente Gmail aislado."""
    service = get_service()

    from_local = {}
    to_local = {}

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
                    from_local[value] = from_local.get(value, 0) + 1
                elif name == "To":
                    to_local[value] = to_local.get(value, 0) + 1

        except:
            pass

    return from_local, to_local


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

    
    chunk_size = len(ids) // PROCESSES + 1
    chunks = [ids[i:i+chunk_size] for i in range(0, len(ids), chunk_size)]

    print(f"⚡ Procesando en paralelo con {PROCESSES} procesos...")

    with mp.Pool(PROCESSES) as pool:
        results = pool.map(process_chunk, chunks)

    from_counter = {}
    to_counter = {}

    for f, t in results:
        merge_dicts(from_counter, f)
        merge_dicts(to_counter, t)

    print("\n📊 TOP 30 REMITENTES:")
    for sender, count in sorted(from_counter.items(), key=lambda x: x[1], reverse=True)[:100]:
        print(f"{count} → {sender}")

    print("\n📤 TOP 30 DESTINATARIOS:")
    for rec, count in sorted(to_counter.items(), key=lambda x: x[1], reverse=True)[:100]:
        print(f"{count} → {rec}")


if __name__ == "__main__":
    process()
