import json
import requests
import msal

# -----------------------------
# CONFIGURACIÓN EXACTA
# -----------------------------
TENANT_ID = "common"   # Para cuentas Outlook/Hotmail personales
CLIENT_ID = "04f0c124-f2bc-4f59-8241-bf6df9866bbd"  # Cliente oficial Microsoft

# SOLO scopes válidos para MSAL Device Flow
SCOPES = [
    "Files.ReadWrite.All",
    "Files.Read.All",
    "Files.Read",
    "User.Read",
]

# -----------------------------
# OBTENER TOKEN (device flow)
# -----------------------------
def get_token():
    app = msal.PublicClientApplication(CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}")

    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise Exception("Error iniciando device flow: " + str(flow))

    print("🔑 Ve a:", flow["verification_uri"])
    print("🧩 Código:", flow["user_code"])

    result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        print("❌ ERROR token:")
        print(result)
        exit()

    return result["access_token"]

# -----------------------------
# TEST: /me/drive/root
# -----------------------------
token = get_token()

print("\n🔍 Probando `/me/drive/root` ...")

resp = requests.get(
    "https://graph.microsoft.com/v1.0/me/drive/root",
    headers={"Authorization": f"Bearer {token}"}
)

print("\nSTATUS:", resp.status_code)
print("\nRAW RESPONSE:")
print(resp.text)

try:
    print("\nJSON FORMATEADO:")
    print(json.dumps(resp.json(), indent=2))
except:
    print("(no JSON válido)")
