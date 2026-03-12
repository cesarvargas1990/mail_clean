import os
import json
from glob import glob
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

DRIVE_READ_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DRIVE_WRITE_SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIAL_CANDIDATES = ["credentials.json", "client_secret.json"]
DEFAULT_DRIVE_TOKEN_FILE = "token_drive.json"
CANCEL_MESSAGE = "Operación cancelada por el usuario."


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
        return DEFAULT_DRIVE_TOKEN_FILE
    clean = "".join(c if c.isalnum() else "_" for c in user_key)
    return f"token_drive_{clean}.json"


def _safe_drive_csv_file(user_email):
    user_key = _safe_user_key(user_email)
    if user_key == "me":
        return "drive_archivos.csv"
    clean = "".join(c if c.isalnum() else "_" for c in user_key)
    return f"drive_archivos_{clean}.csv"


def remove_drive_token(user_email):
    token_path = _safe_drive_token_file(user_email)
    if os.path.exists(token_path):
        os.remove(token_path)


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


def load_drive_credentials(token_path, scopes):
    if os.path.exists(token_path):
        return Credentials.from_authorized_user_file(token_path, scopes)

    if token_path != DEFAULT_DRIVE_TOKEN_FILE and os.path.exists(DEFAULT_DRIVE_TOKEN_FILE):
        return Credentials.from_authorized_user_file(DEFAULT_DRIVE_TOKEN_FILE, scopes)

    return None


def run_oauth_flow(scopes):
    client_secrets_file = find_client_secrets_file()
    if not client_secrets_file:
        raise RuntimeError(
            "No se encontró archivo de credenciales OAuth para Drive. "
            "Coloca credentials.json o client_secret.json en la carpeta del proyecto."
        )

    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
    try:
        return flow.run_local_server(port=0)
    except Exception as exc:
        if "invalid_scope" in str(exc).lower():
            raise RuntimeError(
                "Google devolvió invalid_scope para Drive. "
                "Verifica que Drive API esté habilitada en tu proyecto de Google Cloud "
                "y vuelve a autorizar."
            ) from exc
        raise


def ensure_not_cancelled(stop_event=None):
    if stop_event is not None and stop_event.is_set():
        raise RuntimeError(CANCEL_MESSAGE)


def get_service(user_email=None, stop_event=None, scopes=None, force_reauth=False):
    scopes = scopes or DRIVE_READ_SCOPES
    token_path = _safe_drive_token_file(user_email)
    if force_reauth:
        remove_drive_token(user_email)
    creds = load_drive_credentials(token_path, scopes)

    if not creds or not creds.valid:
        ensure_not_cancelled(stop_event)
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as exc:
                if "invalid_scope" in str(exc).lower():
                    creds = run_oauth_flow(scopes)
                else:
                    raise
        else:
            creds = run_oauth_flow(scopes)

        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def build_folder_map(service, stop_event=None):
    print("📂 Descargando estructura de carpetas…")

    folder_map = {}
    page = None

    while True:
        ensure_not_cancelled(stop_event)
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


def list_drive(user_email=None, stop_event=None, force_reauth=False):
    service = get_service(
        user_email,
        stop_event=stop_event,
        scopes=DRIVE_READ_SCOPES,
        force_reauth=force_reauth,
    )
    output_file = _safe_drive_csv_file(user_email)

    print("🔍 Descargando lista de archivos…")

    files = []
    page = None

    while True:
        ensure_not_cancelled(stop_event)
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

    folder_map = build_folder_map(service, stop_event=stop_event)
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
            ext = name.split(".")[-1].lower() if "." in name else "sin_extension"
            file_id = f.get("id", "")
            full_path = f"{f['path']}/{name}"
            view_url = f"https://drive.google.com/file/d/{file_id}/view"
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

            out.write(
                f"{size_bytes};{size_human};{full_path};{ext};{file_id};{view_url};{download_url}\n"
            )

    print(f"✅ Archivo generado: {output_file}")
    return output_file


def delete_drive_file(file_id, user_email=None):
    if not file_id:
        raise ValueError("Se requiere file_id para eliminar en Drive.")

    service = get_service(user_email, scopes=DRIVE_WRITE_SCOPES)
    service.files().delete(fileId=file_id).execute()


if __name__ == "__main__":
    list_drive()
