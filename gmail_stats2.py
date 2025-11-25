import os
from collections import defaultdict

def human_size(num_bytes):
    """Convierte bytes a MB o GB."""
    if not num_bytes:
        return "0 MB"

    mb = num_bytes / (1024 * 1024)
    if mb < 1024:
        return f"{mb:.2f} MB"

    gb = mb / 1024
    return f"{gb:.2f} GB"


def process_extensions():
    input_file = "drive_archivos.csv"

    if not os.path.exists(input_file):
        print("❌ ERROR: No se encontró drive_archivos.csv")
        return

    print("🔍 Procesando extensiones…")

    ext_count = defaultdict(int)
    ext_size = defaultdict(int)

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(";")

            if len(parts) < 4:
                continue

            size_bytes = int(parts[0])
            file_path = parts[2]

            # detectar extension correctamente
            file_name = file_path.split("/")[-1]

            if "." in file_name:
                ext = file_name.split(".")[-1].lower()
            else:
                ext = "sin_extension"

            ext_count[ext] += 1
            ext_size[ext] += size_bytes

    # crear archivo final
    with open("resumen_extensiones.txt", "w", encoding="utf-8") as out:
        out.write("===== RESUMEN POR EXTENSIÓN =====\n\n")

        for ext in sorted(ext_count, key=lambda e: ext_size[e], reverse=True):
            total_size = human_size(ext_size[ext])
            count = ext_count[ext]

            out.write(f"{ext} → {count} archivos → {total_size}\n")

    print("✅ Archivo generado: resumen_extensiones.txt")


if __name__ == "__main__":
    process_extensions()
