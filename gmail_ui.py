import os
import io
import queue
import threading
import re
import webbrowser
from contextlib import redirect_stdout
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText

from gmail_stats import process as process_gmail
from outlook_stats import process as process_outlook
from drive_stats import list_drive
from drive_stats2 import process_extensions
from onedrive_stats import list_onedrive


class GmailReportApp:
    VERTICAL_NOTEBOOK_STYLE = "Vertical.TNotebook"
    REQUIRED_EMAIL_TITLE = "Correo requerido"
    URL_PATTERN = re.compile(r"https?://[^\s;]+")
    DRIVE_LIST_FILE = "drive_archivos.csv"
    DRIVE_SUMMARY_FILE = "resumen_extensiones.txt"

    def __init__(self, root):
        self.root = root
        self.root.title("Reportes de limpieza: Correos y Drive")
        self.root.geometry("980x680")

        self.style = ttk.Style(self.root)
        self.style.configure(self.VERTICAL_NOTEBOOK_STYLE, tabposition="wn")
        self.style.configure(f"{self.VERTICAL_NOTEBOOK_STYLE}.Tab", padding=(10, 6))

        self.events = queue.Queue()
        self.mail_worker_thread = None
        self.drive_worker_thread = None

        main = ttk.Frame(root, padding=8)
        main.pack(fill="both", expand=True)

        header = ttk.Label(
            main,
            text=(
                "Esta herramienta genera reportes para limpieza manual de\n"
                "correos (Gmail) y archivos en Google Drive."
            ),
            justify="left",
        )
        header.pack(anchor="w", pady=(0, 6))

        self.top_notebook = ttk.Notebook(main)
        self.top_notebook.pack(fill="both", expand=True)

        self.mail_page = ttk.Frame(self.top_notebook)
        self.drive_page = ttk.Frame(self.top_notebook)
        self.top_notebook.add(self.mail_page, text="Correos")
        self.top_notebook.add(self.drive_page, text="Drive")

        self.build_mail_ui(self.mail_page)
        self.build_drive_ui(self.drive_page)

        self.root.after(120, self.consume_events)

    def build_mail_ui(self, parent):
        form = ttk.Frame(parent)
        form.pack(fill="x", pady=(0, 6))

        ttk.Label(form, text="Proveedor:").pack(side="left")
        self.mail_provider_var = tk.StringVar(value="Gmail")
        self.mail_provider_combo = ttk.Combobox(
            form,
            textvariable=self.mail_provider_var,
            values=["Gmail", "Outlook"],
            width=10,
            state="readonly",
        )
        self.mail_provider_combo.pack(side="left", padx=(8, 10))

        ttk.Label(form, text="Correo:").pack(side="left")
        self.email_var = tk.StringVar()
        self.email_entry = ttk.Entry(form, textvariable=self.email_var, width=48)
        self.email_entry.pack(side="left", padx=(8, 10))

        self.mail_provider_combo.bind("<<ComboboxSelected>>", self.on_provider_change)

        self.run_button = ttk.Button(form, text="Generar reporte", command=self.start_report)
        self.run_button.pack(side="left")

        ttk.Label(parent, text="Estado / progreso:").pack(anchor="w")
        self.log_box = ScrolledText(parent, height=8, wrap="word", state="disabled")
        self.log_box.pack(fill="x", pady=(2, 6))

        ttk.Label(parent, text="Resumen:").pack(anchor="w")
        self.summary_box = ScrolledText(parent, height=5, wrap="word", state="disabled")
        self.summary_box.pack(fill="x", pady=(2, 6))
        self.summary_box.tag_configure("summary_cached", foreground="#0B7A3E")
        self.summary_box.tag_configure("summary_new", foreground="#0B5394")

        ttk.Label(parent, text="Archivos generados:").pack(anchor="w")
        self.mail_notebook = ttk.Notebook(parent, style=self.VERTICAL_NOTEBOOK_STYLE)
        self.mail_notebook.pack(fill="both", expand=True)

    def build_drive_ui(self, parent):
        form = ttk.Frame(parent)
        form.pack(fill="x", pady=(0, 6))

        ttk.Label(form, text="Proveedor:").pack(side="left")
        self.drive_provider_var = tk.StringVar(value="Google Drive")
        self.drive_provider_combo = ttk.Combobox(
            form,
            textvariable=self.drive_provider_var,
            values=["Google Drive", "OneDrive"],
            width=13,
            state="readonly",
        )
        self.drive_provider_combo.pack(side="left", padx=(8, 10))

        ttk.Label(form, text="Correo Drive:").pack(side="left")
        self.drive_email_var = tk.StringVar()
        self.drive_email_entry = ttk.Entry(form, textvariable=self.drive_email_var, width=38)
        self.drive_email_entry.pack(side="left", padx=(8, 10))

        self.drive_run_button = ttk.Button(form, text="Generar listado Drive", command=self.start_drive_report)
        self.drive_run_button.pack(side="left")
        self.drive_open_button = ttk.Button(form, text="Abrir último Drive", command=self.open_last_drive_report)
        self.drive_open_button.pack(side="left", padx=(8, 0))

        ttk.Label(parent, text="Estado Drive:").pack(anchor="w")
        self.drive_log_box = ScrolledText(parent, height=8, wrap="word", state="disabled")
        self.drive_log_box.pack(fill="x", pady=(2, 6))

        ttk.Label(parent, text="Archivos Drive generados:").pack(anchor="w")
        self.drive_notebook = ttk.Notebook(parent)
        self.drive_notebook.pack(fill="both", expand=True)

    def append_log(self, message):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", f"{message}\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def append_drive_log(self, message):
        self.drive_log_box.configure(state="normal")
        self.drive_log_box.insert("end", f"{message}\n")
        self.drive_log_box.see("end")
        self.drive_log_box.configure(state="disabled")

    def set_running(self, running):
        if running:
            self.run_button.configure(state="disabled")
            self.email_entry.configure(state="disabled")
            self.mail_provider_combo.configure(state="disabled")
        else:
            self.run_button.configure(state="normal")
            self.email_entry.configure(state="normal")
            self.mail_provider_combo.configure(state="readonly")

    def on_provider_change(self, _event=None):
        provider = self.mail_provider_var.get()
        if provider == "Outlook":
            self.append_log("ℹ️ Proveedor seleccionado: Outlook")
        else:
            self.append_log("ℹ️ Proveedor seleccionado: Gmail")

    def clear_tabs(self):
        for tab_id in self.mail_notebook.tabs():
            self.mail_notebook.forget(tab_id)

    def clear_drive_tabs(self):
        for tab_id in self.drive_notebook.tabs():
            self.drive_notebook.forget(tab_id)

    @staticmethod
    def safe_key(email):
        email = (email or "").strip().lower()
        if not email:
            return None
        return "".join(c if c.isalnum() else "_" for c in email)

    def get_drive_output_files(self, drive_email):
        safe = self.safe_key(drive_email)
        provider = self.drive_provider_var.get()
        if provider == "OneDrive":
            return [
                f"onedrive_archivos_{safe}.csv",
                f"resumen_extensiones_onedrive_{safe}.txt",
            ]
        return [f"drive_archivos_{safe}.csv", f"resumen_extensiones_{safe}.txt"]

    def set_summary(self, summary):
        source_tag = None
        if not summary:
            text = "Resumen no disponible."
        else:
            source = summary.get("source", "")
            last_scan = summary.get("last_scan") or "No registrado"
            detail = summary.get("detail", {})

            source_label = "Nuevo escaneo" if source == "new_scan" else "Cargado desde escaneo previo"
            source_tag = "summary_new" if source == "new_scan" else "summary_cached"
            rec_with = detail.get("received_with_attachments", 0)
            rec_without = detail.get("received_without_attachments", 0)
            sent_with = detail.get("sent_with_attachments", 0)
            sent_without = detail.get("sent_without_attachments", 0)
            total = rec_with + rec_without + sent_with + sent_without

            text = (
                f"Origen: {source_label}\n"
                f"Último escaneo: {last_scan}\n"
                f"Recibidos: con adjuntos={rec_with} | sin adjuntos={rec_without}\n"
                f"Enviados: con adjuntos={sent_with} | sin adjuntos={sent_without}\n"
                f"Total registros considerados: {total}"
            )

        self.summary_box.configure(state="normal")
        self.summary_box.delete("1.0", "end")
        self.summary_box.insert("1.0", text)
        if source_tag:
            self.summary_box.tag_add(source_tag, "1.0", "1.end")
        self.summary_box.configure(state="disabled")

    @staticmethod
    def extract_sections(content, header_a, header_b):
        idx_a = content.find(header_a)
        idx_b = content.find(header_b)
        if idx_a == -1 or idx_b == -1:
            return None, None

        if idx_a < idx_b:
            section_a = content[idx_a + len(header_a):idx_b].strip()
            section_b = content[idx_b + len(header_b):].strip()
        else:
            section_b = content[idx_b + len(header_b):idx_a].strip()
            section_a = content[idx_a + len(header_a):].strip()

        return section_a, section_b

    def add_text_tab(self, parent_notebook, title, text):
        frame = ttk.Frame(parent_notebook)
        parent_notebook.add(frame, text=title)
        content = ScrolledText(frame, wrap="none")
        content.pack(fill="both", expand=True)
        content.insert("1.0", text)
        self.enable_clickable_links(content, text)
        content.configure(state="disabled")

    def enable_clickable_links(self, text_widget, full_text):
        for idx, match in enumerate(self.URL_PATTERN.finditer(full_text)):
            start = match.start()
            end = match.end()
            start_index = f"1.0+{start}c"
            end_index = f"1.0+{end}c"
            tag = f"url_{idx}"
            url = match.group(0)

            text_widget.tag_add(tag, start_index, end_index)
            text_widget.tag_config(tag, foreground="#1a73e8", underline=True)
            text_widget.tag_bind(tag, "<Button-1>", lambda _e, link=url: webbrowser.open(link))

    def add_attachment_tabs(self, parent_notebook, title, block_content):
        direction_frame = ttk.Frame(parent_notebook)
        parent_notebook.add(direction_frame, text=title)
        attachment_notebook = ttk.Notebook(direction_frame, style=self.VERTICAL_NOTEBOOK_STYLE)
        attachment_notebook.pack(fill="both", expand=True)

        con_adjuntos, sin_adjuntos = self.extract_sections(
            block_content,
            "--- CON ADJUNTOS ---",
            "--- SIN ADJUNTOS ---",
        )

        if con_adjuntos is not None and sin_adjuntos is not None:
            self.add_text_tab(attachment_notebook, "Con adjuntos", con_adjuntos)
            self.add_text_tab(attachment_notebook, "Sin adjuntos", sin_adjuntos)
        else:
            self.add_text_tab(attachment_notebook, "Completo", block_content)

    @staticmethod
    def find_report_path(files, dynamic_prefix, legacy_name):
        path = next((f for f in files if os.path.basename(f).startswith(dynamic_prefix)), None)
        if not path and legacy_name in files:
            path = legacy_name
        return path

    def render_detail_tabs(self, detalle_path):
        with open(detalle_path, "r", encoding="utf-8") as f:
            detalle_content = f.read()

        detalle_tab = ttk.Frame(self.mail_notebook)
        self.mail_notebook.add(detalle_tab, text="Detalle correos")
        detalle_notebook = ttk.Notebook(detalle_tab, style=self.VERTICAL_NOTEBOOK_STYLE)
        detalle_notebook.pack(fill="both", expand=True)

        recibidos, enviados = self.extract_sections(
            detalle_content,
            "===== REMITENTES (RECIBIDOS) =====",
            "===== DESTINATARIOS (ENVIADOS) =====",
        )

        if recibidos is not None and enviados is not None:
            self.add_attachment_tabs(detalle_notebook, "Recibidos", recibidos)
            self.add_attachment_tabs(detalle_notebook, "Enviados", enviados)
        else:
            self.add_text_tab(detalle_notebook, "Completo", detalle_content)

    def render_domain_tabs(self, dominios_path):
        with open(dominios_path, "r", encoding="utf-8") as f:
            dominios_content = f.read()

        dominios_tab = ttk.Frame(self.mail_notebook)
        self.mail_notebook.add(dominios_tab, text="Dominios")
        dominios_notebook = ttk.Notebook(dominios_tab, style=self.VERTICAL_NOTEBOOK_STYLE)
        dominios_notebook.pack(fill="both", expand=True)

        recibidos_dom, enviados_dom = self.extract_sections(
            dominios_content,
            "===== DOMINIOS REMITENTES (RECIBIDOS) =====",
            "===== DOMINIOS DESTINATARIOS (ENVIADOS) =====",
        )

        if recibidos_dom is not None and enviados_dom is not None:
            self.add_attachment_tabs(dominios_notebook, "Recibidos", recibidos_dom)
            self.add_attachment_tabs(dominios_notebook, "Enviados", enviados_dom)
        else:
            self.add_text_tab(dominios_notebook, "Completo", dominios_content)

    def build_tabs(self, files):
        self.clear_tabs()

        detalle_path = self.find_report_path(files, "detalle_correos_", "detalle_correos.txt")
        dominios_path = self.find_report_path(files, "dominios_", "dominios.txt")

        if detalle_path and os.path.exists(detalle_path):
            self.render_detail_tabs(detalle_path)
        else:
            self.append_log("⚠️ No se encontró el archivo esperado de detalle_correos.")

        if dominios_path and os.path.exists(dominios_path):
            self.render_domain_tabs(dominios_path)
        else:
            self.append_log("⚠️ No se encontró el archivo esperado de dominios.")

    def capture_drive_script_output(self, func):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            func()
        output = buffer.getvalue().strip()
        if output:
            for line in output.splitlines():
                self.events.put(("drive_log", line))

    def render_drive_tabs(self, files):
        self.clear_drive_tabs()
        for path in files:
            if not os.path.exists(path):
                self.append_drive_log(f"⚠️ No se encontró el archivo esperado: {path}")
                continue

            tab_title = os.path.basename(path)
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            self.add_text_tab(self.drive_notebook, tab_title, content)

    def start_drive_report(self):
        drive_email = self.drive_email_var.get().strip()
        provider = self.drive_provider_var.get()
        if not drive_email:
            messagebox.showwarning(self.REQUIRED_EMAIL_TITLE, "Ingresa el correo para Drive.")
            return

        self.drive_log_box.configure(state="normal")
        self.drive_log_box.delete("1.0", "end")
        self.drive_log_box.configure(state="disabled")
        self.clear_drive_tabs()
        self.set_drive_running(True)
        self.append_drive_log(f"▶️ Iniciando análisis de {provider} para: {drive_email}")
        self.drive_worker_thread = threading.Thread(target=self.run_drive_report, args=(drive_email, provider), daemon=True)
        self.drive_worker_thread.start()

    def set_drive_running(self, running):
        if running:
            self.drive_run_button.configure(state="disabled")
            self.drive_open_button.configure(state="disabled")
            self.drive_email_entry.configure(state="disabled")
            self.drive_provider_combo.configure(state="disabled")
        else:
            self.drive_run_button.configure(state="normal")
            self.drive_open_button.configure(state="normal")
            self.drive_email_entry.configure(state="normal")
            self.drive_provider_combo.configure(state="readonly")

    def open_last_drive_report(self):
        drive_email = self.drive_email_var.get().strip()
        if not drive_email:
            messagebox.showwarning(self.REQUIRED_EMAIL_TITLE, "Ingresa el correo para Drive.")
            return

        files = self.get_drive_output_files(drive_email)
        existing = [path for path in files if os.path.exists(path)]
        if not existing:
            self.append_drive_log(f"⚠️ No hay reportes de Drive previos para {drive_email}.")
            return

        self.render_drive_tabs(files)
        self.append_drive_log(f"ℹ️ Se cargaron los últimos archivos de {self.drive_provider_var.get()} sin reescanear.")

    def run_drive_report(self, drive_email, provider):
        try:
            if provider == "OneDrive":
                list_file = list_onedrive(drive_email)
            else:
                list_file = list_drive(drive_email)
            summary_file = process_extensions(input_file=list_file)
            files = [list_file, summary_file]
            self.events.put(("drive_done", {"files": files}))
        except Exception as exc:
            self.events.put(("drive_error", str(exc)))

    def start_report(self):
        email = self.email_var.get().strip()
        provider = self.mail_provider_var.get()
        if not email:
            messagebox.showwarning(self.REQUIRED_EMAIL_TITLE, f"Ingresa el correo {provider} a procesar.")
            return

        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        self.set_summary(None)
        self.clear_tabs()

        self.append_log(f"▶️ Iniciando análisis {provider} para: {email}")
        self.set_running(True)

        self.mail_worker_thread = threading.Thread(target=self.run_report, args=(email, provider), daemon=True)
        self.mail_worker_thread.start()

    def run_report(self, email, provider):
        def emit(message):
            self.events.put(("log", message))

        try:
            if provider == "Outlook":
                result = process_outlook(user_email=email, log=emit)
            else:
                result = process_gmail(user_email=email, log=emit)
            self.events.put(("done", result))
        except Exception as exc:
            self.events.put(("error", str(exc)))

    def handle_mail_done_event(self, payload):
        self.set_running(False)
        files = payload.get("files", []) if isinstance(payload, dict) else payload
        summary = payload.get("summary") if isinstance(payload, dict) else None
        self.build_tabs(files)
        self.set_summary(summary)
        self.append_log("✅ Reporte finalizado y cargado en pestañas.")

    def handle_drive_done_event(self, payload):
        self.set_drive_running(False)
        files = payload.get("files", []) if isinstance(payload, dict) else payload
        self.render_drive_tabs(files)
        self.append_drive_log("✅ Reporte de Drive finalizado y cargado en pestañas.")

    def handle_error_event(self, payload):
        self.set_running(False)
        self.append_log("❌ Error durante el procesamiento:")
        self.append_log(payload)

    def handle_drive_error_event(self, payload):
        self.set_drive_running(False)
        self.append_drive_log("❌ Error durante el procesamiento de Drive:")
        self.append_drive_log(payload)

    def dispatch_event(self, event, payload):
        if event == "log":
            self.append_log(payload)
        elif event == "done":
            self.handle_mail_done_event(payload)
        elif event == "drive_log":
            self.append_drive_log(payload)
        elif event == "drive_done":
            self.handle_drive_done_event(payload)
        elif event == "drive_error":
            self.handle_drive_error_event(payload)
        elif event == "error":
            self.handle_error_event(payload)

    def consume_events(self):
        try:
            while True:
                event, payload = self.events.get_nowait()
                self.dispatch_event(event, payload)
        except queue.Empty:
            pass

        self.root.after(120, self.consume_events)


if __name__ == "__main__":
    app_root = tk.Tk()
    GmailReportApp(app_root)
    app_root.mainloop()
