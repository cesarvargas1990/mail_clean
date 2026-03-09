import json
import requests
import msal

TENANT_ID = "common"
CLIENT_ID = "537b2720-a1e6-4f38-804f-241ec4455163"

SCOPES = [
    "User.Read",
    "Files.ReadWrite",
]

def get_token():
    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )

    flow = app.initiate_device_flow(scopes=SCOPES)

    if "user_code" not in flow:
        print("Error iniciando device flow:", flow)
        exit()

    print("🔑 Ve a:", flow["verification_uri"])
    print("🧩 Código:", flow["user_code"])

    result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        print("\n❌ ERROR: No se obtuvo token")
        print(result)
        exit()

    return result["access_token"]


token = get_token()

print("\nProbando `/me/drive/root` ...")

resp = requests.get(
    "https://graph.microsoft.com/v1.0/me/drive/root",
    headers={"Authorization": f"Bearer {token}"}
)

print("\nSTATUS:", resp.status_code)
print(json.dumps(resp.json(), indent=2))
