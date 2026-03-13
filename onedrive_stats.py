import os
from glob import glob

import requests
import msal

from auth_browser import open_url_in_private_window

TENANT_ID = "common"
CLIENT_ID = "537b2720-a1e6-4f38-804f-241ec44f5163"
SCOPES = [
    "Files.ReadWrite",
    "User.Read",
]
CREDENTIAL_CANDIDATES = ["credentials.json", "client_secret.json"]
CANCEL_MESSAGE = "Operación cancelada por el usuario."


def _safe_user_key(user_email):
    user_email = (user_email or "").strip().lower()
    return user_email or "me"


def _safe_token_file(user_email):
    user_key = _safe_user_key(user_email)
    if user_key == "me":
        return "token_onedrive.json"
    clean = "".join(c if c.isalnum() else "_" for c in user_key)
    return f"token_onedrive_{clean}.json"


def _safe_output_file(user_email):
    user_key = _safe_user_key(user_email)
    if user_key == "me":
        return "onedrive_archivos.csv"
    clean = "".join(c if c.isalnum() else "_" for c in user_key)
    return f"onedrive_archivos_{clean}.csv"


def _normalize_email(value):
    return (value or "").strip().lower()


def remove_onedrive_token(user_email):
    token_path = _safe_token_file(user_email)
    if os.path.exists(token_path):
        os.remove(token_path)


def human_size(num_bytes):
    if num_bytes is None:
        return "0 B"
    num_bytes = int(num_bytes)
    mb = num_bytes / (1024 * 1024)
    if mb < 1024:
        return f"{mb:.2f} MB"
    gb = mb / 1024
    return f"{gb:.2f} GB"


def find_client_secrets_file():
    for path in CREDENTIAL_CANDIDATES:
        if os.path.exists(path):
            return path

    extra_candidates = sorted(glob("client_secret*.json"))
    if extra_candidates:
        return extra_candidates[0]

    return None


def ensure_not_cancelled(stop_event=None):
    if stop_event is not None and stop_event.is_set():
        raise RuntimeError(CANCEL_MESSAGE)


def open_auth_url(log=print):
    url = "https://www.microsoft.com/link"
    try:
        opened = open_url_in_private_window(url)
        mode = "en ventana privada" if opened == "private" else "en navegador del sistema"
        log(f"🌐 Abriendo navegador {mode}: {url}")
    except Exception:
        pass


def find_matching_account(accounts, user_email):
    expected = _normalize_email(user_email)
    if not expected:
        return accounts[0] if accounts else None

    for account in accounts:
        username = _normalize_email(account.get("username"))
        if username == expected:
            return account

    return None


def extract_token_username(result):
    claims = result.get("id_token_claims", {}) if isinstance(result, dict) else {}
    return _normalize_email(
        claims.get("preferred_username")
        or claims.get("email")
        or claims.get("upn")
    )


def validate_token_user(access_token, expected_email):
    expected = _normalize_email(expected_email)
    if not expected:
        return

    resp = requests.get(
        "https://graph.microsoft.com/v1.0/me?$select=userPrincipalName,mail",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"No se pudo validar la cuenta de OneDrive: {resp.status_code} {resp.text}")

    data = resp.json()
    actual = _normalize_email(data.get("userPrincipalName") or data.get("mail"))
    if actual and actual != expected:
        raise RuntimeError(
            f"La cuenta autenticada en OneDrive es {actual}, pero solicitaste {expected}. "
            "Vuelve a procesar y elige regenerar credenciales si necesitas cambiar de cuenta."
        )


def get_access_token(user_email=None, log=print, stop_event=None, force_reauth=False):
    token_path = _safe_token_file(user_email)
    if force_reauth:
        remove_onedrive_token(user_email)

    cache = msal.SerializableTokenCache()
    if os.path.exists(token_path):
        with open(token_path, "r", encoding="utf-8") as f:
            cache.deserialize(f.read())

    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority="https://login.microsoftonline.com/consumers",
        token_cache=cache,
    )

    accounts = app.get_accounts()
    result = None
    matching_account = find_matching_account(accounts, user_email)
    if matching_account:
        result = app.acquire_token_silent(SCOPES, account=matching_account)

    ensure_not_cancelled(stop_event)

    if not result:
        flow = app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise RuntimeError(f"Error iniciando device flow: {flow}")

        log("🔐 Autenticación necesaria para OneDrive:")
        log(flow.get("message", ""))
        log(
            "Si Microsoft propone otra cuenta por defecto, "
            "abre el enlace en una ventana privada o cambia de cuenta manualmente allí."
        )
        log("(Ingresa el código, acepta permisos y vuelve a esta ventana.)")
        open_auth_url(log=log)

        result = app.acquire_token_by_device_flow(flow)

        if "access_token" not in result:
            raise RuntimeError(f"Error obteniendo token OneDrive: {result}")

        token_username = extract_token_username(result)
        expected = _normalize_email(user_email)
        if expected and token_username and token_username != expected:
            raise RuntimeError(
                f"Se autenticó la cuenta {token_username}, pero solicitaste {expected}. "
                "Reintenta con la cuenta correcta."
            )

    if cache.has_state_changed:
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(cache.serialize())

    access_token = result["access_token"]
    validate_token_user(access_token, user_email)
    return access_token


def list_onedrive(user_email=None, log=print, stop_event=None, force_reauth=False):
    output_file = _safe_output_file(user_email)
    token = get_access_token(
        user_email,
        log=log,
        stop_event=stop_event,
        force_reauth=force_reauth,
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    log("🔍 Descargando lista de archivos de OneDrive...")

    url = "https://graph.microsoft.com/v1.0/me/drive/root/children?$top=200"
    files = []

    while url:
        ensure_not_cancelled(stop_event)
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Error listando OneDrive: {resp.status_code} {resp.text}")

        data = resp.json()
        for item in data.get("value", []):
            if "folder" in item:
                continue
            files.append(item)

        url = data.get("@odata.nextLink")

    files_sorted = sorted(files, key=lambda x: int(x.get("size", 0)), reverse=True)

    with open(output_file, "w", encoding="utf-8") as out:
        out.write("size_bytes;size_human;full_path;ext;file_id;view_url;download_url\n")

        for item in files_sorted:
            size_bytes = int(item.get("size", 0))
            size_human = human_size(size_bytes)
            name = item.get("name", "")
            ext = name.split(".")[-1].lower() if "." in name else "sin_extension"
            file_id = item.get("id", "")
            path = item.get("parentReference", {}).get("path", "")
            full_path = f"/{path.replace('/drive/root:', '').lstrip('/')}/{name}".replace("//", "/")
            view_url = item.get("webUrl", "")
            download_url = item.get("@microsoft.graph.downloadUrl", "")

            out.write(
                f"{size_bytes};{size_human};{full_path};{ext};{file_id};{view_url};{download_url}\n"
            )

    log(f"✅ Archivo generado: {output_file}")
    return output_file


def delete_onedrive_file(file_id, user_email=None):
    if not file_id:
        raise ValueError("Se requiere file_id para eliminar en OneDrive.")

    token = get_access_token(user_email)
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}"

    resp = requests.delete(url, headers=headers, timeout=30)
    if resp.status_code not in (204,):
        raise RuntimeError(f"Error eliminando en OneDrive: {resp.status_code} {resp.text}")


if __name__ == "__main__":
    list_onedrive()
