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


def derive_summary_file(input_file):
    base = os.path.basename(input_file)
    if base.startswith("drive_archivos_") and base.endswith(".csv"):
        suffix = base[len("drive_archivos_"):-4]
        return f"resumen_extensiones_{suffix}.txt"
    if base.startswith("onedrive_archivos_") and base.endswith(".csv"):
        suffix = base[len("onedrive_archivos_"):-4]
        return f"resumen_extensiones_onedrive_{suffix}.txt"
    if base == "onedrive_archivos.csv":
        return "resumen_extensiones_onedrive.txt"
    return "resumen_extensiones.txt"


def process_extensions(input_file="drive_archivos.csv", output_file=None):
    if output_file is None:
        output_file = derive_summary_file(input_file)

    if not os.path.exists(input_file):
        print(f"❌ ERROR: No se encontró {input_file}")
        return None

    print("🔍 Procesando extensiones…")

    ext_count = defaultdict(int)
    ext_size = defaultdict(int)

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(";")

            if len(parts) < 4:
                continue

            try:
                size_bytes = int(parts[0])
            except ValueError:
                continue

            ext = (parts[3] or "").strip().lower()
            if not ext:
                ext = "sin_extension"

            ext_count[ext] += 1
            ext_size[ext] += size_bytes

    # crear archivo final
    with open(output_file, "w", encoding="utf-8") as out:
        out.write("===== RESUMEN POR EXTENSIÓN =====\n\n")

        for ext in sorted(ext_count, key=lambda e: ext_size[e], reverse=True):
            total_size = human_size(ext_size[ext])
            count = ext_count[ext]

            out.write(f"{ext} → {count} archivos → {total_size}\n")

    print(f"✅ Archivo generado: {output_file}")
    return output_file


if __name__ == "__main__":
    process_extensions()
