import os
import json
from glob import glob
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
CREDENTIAL_CANDIDATES = ["credentials.json", "client_secret.json"]


def find_client_secrets_file():
    for path in CREDENTIAL_CANDIDATES:
        if os.path.exists(path):
            return path

    extra_candidates = sorted(glob("client_secret*.json"))
    if extra_candidates:
        return extra_candidates[0]

    return None


def _safe_user_key(user_email):
    user_email = (user_email or "").strip().lower()
    return user_email or "me"


def _safe_drive_token_file(user_email):
    user_key = _safe_user_key(user_email)
    if user_key == "me":
        return "token_drive.json"
    clean = "".join(c if c.isalnum() else "_" for c in user_key)
    return f"token_drive_{clean}.json"


def _safe_drive_csv_file(user_email):
    user_key = _safe_user_key(user_email)
    if user_key == "me":
        return "drive_archivos.csv"
    clean = "".join(c if c.isalnum() else "_" for c in user_key)
    return f"drive_archivos_{clean}.csv"


def human_size(num_bytes):
    """Convierte bytes a MB o GB con 2 decimales."""
    if num_bytes is None:
        return "0 B"

    num_bytes = int(num_bytes)

    mb = num_bytes / (1024 * 1024)
    if mb < 1024:
        return f"{mb:.2f} MB"

    gb = mb / 1024
    return f"{gb:.2f} GB"


def get_service(user_email=None):
    token_path = _safe_drive_token_file(user_email)
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            client_secrets_file = find_client_secrets_file()
            if not client_secrets_file:
                raise RuntimeError(
                    "No se encontró archivo de credenciales OAuth para Drive. "
                    "Coloca credentials.json o client_secret.json en la carpeta del proyecto."
                )

            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file, SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def build_folder_map(service):
    print("📂 Descargando estructura de carpetas…")

    folder_map = {}
    page = None

    while True:
        resp = service.files().list(
            q="mimeType='application/vnd.google-apps.folder'",
            fields="nextPageToken, files(id,name,parents)",
            pageSize=500,
            pageToken=page
        ).execute()

        for f in resp.get("files", []):
            folder_map[f["id"]] = {
                "name": f["name"],
                "parents": f.get("parents", [])
            }

        page = resp.get("nextPageToken")
        if not page:
            break

    return folder_map


def resolve_path(file, folder_map, cache):
    if "parents" not in file:
        return "/"

    parent = file["parents"][0]

    if parent in cache:
        return cache[parent]

    parts = []
    while parent in folder_map:
        folder = folder_map[parent]
        parts.append(folder["name"])

        if "parents" not in folder or not folder["parents"]:
            break

        parent = folder["parents"][0]

    full_path = "/" + "/".join(reversed(parts))
    cache[file["parents"][0]] = full_path
    return full_path


def list_drive(user_email=None):
    service = get_service(user_email)
    output_file = _safe_drive_csv_file(user_email)

    print("🔍 Descargando lista de archivos…")

    files = []
    page = None

    while True:
        resp = service.files().list(
            fields="nextPageToken, files(id,name,size,mimeType,parents)",
            pageSize=1000,
            pageToken=page
        ).execute()

        files.extend(resp.get("files", []))

        page = resp.get("nextPageToken")
        if not page:
            break

    print(f"📄 Total archivos: {len(files)}")

    folder_map = build_folder_map(service)
    cache = {}

    for f in files:
        f["path"] = resolve_path(f, folder_map, cache)

    # ordenar por tamaño
    files_sorted = sorted(
        files,
        key=lambda x: int(x.get("size", 0)),
        reverse=True
    )

    # exportar con tamaños humanos
    with open(output_file, "w", encoding="utf-8") as out:
        out.write("size_bytes;size_human;full_path;ext;file_id;view_url;download_url\n")

        for f in files_sorted:
            size_bytes = f.get("size", "0")
            size_human = human_size(size_bytes)

            name = f.get("name", "")
            ext = name.split(".")[-1].lower() if "." in name else ""
            file_id = f.get("id", "")
            full_path = f"{f['path']}/{name}"
            view_url = f"https://drive.google.com/file/d/{file_id}/view"
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

            out.write(
                f"{size_bytes};{size_human};{full_path};{ext};{file_id};{view_url};{download_url}\n"
            )

    print(f"✅ Archivo generado: {output_file}")
    return output_file


if __name__ == "__main__":
    list_drive()
