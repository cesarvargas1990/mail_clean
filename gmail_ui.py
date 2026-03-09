import os
import queue
import threading
import traceback
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText

from gmail_stats import process


class GmailReportApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Reporte de limpieza Gmail")
        self.root.geometry("980x680")

        self.events = queue.Queue()
        self.worker_thread = None

        main = ttk.Frame(root, padding=12)
        main.pack(fill="both", expand=True)

        header = ttk.Label(
            main,
            text=(
                "Esta herramienta analiza tu Gmail y genera reportes para facilitar\n"
                "la limpieza manual de correos (remitentes/destinatarios y dominios)."
            ),
            justify="left",
        )
        header.pack(anchor="w", pady=(0, 10))

        form = ttk.Frame(main)
        form.pack(fill="x", pady=(0, 10))

        ttk.Label(form, text="Correo Gmail:").pack(side="left")
        self.email_var = tk.StringVar()
        self.email_entry = ttk.Entry(form, textvariable=self.email_var, width=48)
        self.email_entry.pack(side="left", padx=(8, 10))

        self.run_button = ttk.Button(form, text="Generar reporte", command=self.start_report)
        self.run_button.pack(side="left")

        ttk.Label(main, text="Estado / progreso:").pack(anchor="w")
        self.log_box = ScrolledText(main, height=14, wrap="word", state="disabled")
        self.log_box.pack(fill="x", pady=(4, 12))

        ttk.Label(main, text="Archivos generados:").pack(anchor="w")
        self.notebook = ttk.Notebook(main)
        self.notebook.pack(fill="both", expand=True)

        self.root.after(120, self.consume_events)

    def append_log(self, message):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", f"{message}\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def set_running(self, running):
        if running:
            self.run_button.configure(state="disabled")
            self.email_entry.configure(state="disabled")
        else:
            self.run_button.configure(state="normal")
            self.email_entry.configure(state="normal")

    def clear_tabs(self):
        for tab_id in self.notebook.tabs():
            self.notebook.forget(tab_id)

    def build_tabs(self, files):
        self.clear_tabs()
        for path in files:
            if not os.path.exists(path):
                self.append_log(f"⚠️ No se encontró el archivo esperado: {path}")
                continue

            tab = ttk.Frame(self.notebook)
            self.notebook.add(tab, text=os.path.basename(path))

            content = ScrolledText(tab, wrap="none")
            content.pack(fill="both", expand=True)

            with open(path, "r", encoding="utf-8") as f:
                content.insert("1.0", f.read())

            content.configure(state="disabled")

    def start_report(self):
        email = self.email_var.get().strip()
        if not email:
            messagebox.showwarning("Correo requerido", "Ingresa el correo Gmail a procesar.")
            return

        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        self.clear_tabs()

        self.append_log(f"▶️ Iniciando análisis para: {email}")
        self.set_running(True)

        self.worker_thread = threading.Thread(target=self.run_report, args=(email,), daemon=True)
        self.worker_thread.start()

    def run_report(self, email):
        def emit(message):
            self.events.put(("log", message))

        try:
            files = process(user_email=email, log=emit)
            self.events.put(("done", files))
        except Exception:
            self.events.put(("error", traceback.format_exc()))

    def consume_events(self):
        try:
            while True:
                event, payload = self.events.get_nowait()
                if event == "log":
                    self.append_log(payload)
                elif event == "done":
                    self.set_running(False)
                    self.build_tabs(payload)
                    self.append_log("✅ Reporte finalizado y cargado en pestañas.")
                elif event == "error":
                    self.set_running(False)
                    self.append_log("❌ Error durante el procesamiento:")
                    self.append_log(payload)
        except queue.Empty:
            pass

        self.root.after(120, self.consume_events)


if __name__ == "__main__":
    app_root = tk.Tk()
    GmailReportApp(app_root)
    app_root.mainloop()
