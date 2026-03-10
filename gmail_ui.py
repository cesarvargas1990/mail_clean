import os
import queue
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText

from gmail_stats import process


class GmailReportApp:
    VERTICAL_NOTEBOOK_STYLE = "Vertical.TNotebook"

    def __init__(self, root):
        self.root = root
        self.root.title("Reporte de limpieza Gmail")
        self.root.geometry("980x680")

        self.style = ttk.Style(self.root)
        self.style.configure(self.VERTICAL_NOTEBOOK_STYLE, tabposition="wn")
        self.style.configure(f"{self.VERTICAL_NOTEBOOK_STYLE}.Tab", padding=(10, 6))

        self.events = queue.Queue()
        self.worker_thread = None

        main = ttk.Frame(root, padding=8)
        main.pack(fill="both", expand=True)

        header = ttk.Label(
            main,
            text=(
                "Esta herramienta analiza tu Gmail y genera reportes para facilitar\n"
                "la limpieza manual de correos (recibidos/enviados y con/sin adjuntos)."
            ),
            justify="left",
        )
        header.pack(anchor="w", pady=(0, 6))

        form = ttk.Frame(main)
        form.pack(fill="x", pady=(0, 6))

        ttk.Label(form, text="Correo Gmail:").pack(side="left")
        self.email_var = tk.StringVar()
        self.email_entry = ttk.Entry(form, textvariable=self.email_var, width=48)
        self.email_entry.pack(side="left", padx=(8, 10))

        self.run_button = ttk.Button(form, text="Generar reporte", command=self.start_report)
        self.run_button.pack(side="left")

        ttk.Label(main, text="Estado / progreso:").pack(anchor="w")
        self.log_box = ScrolledText(main, height=8, wrap="word", state="disabled")
        self.log_box.pack(fill="x", pady=(2, 6))

        ttk.Label(main, text="Resumen:").pack(anchor="w")
        self.summary_box = ScrolledText(main, height=5, wrap="word", state="disabled")
        self.summary_box.pack(fill="x", pady=(2, 6))
        self.summary_box.tag_configure("summary_cached", foreground="#0B7A3E")
        self.summary_box.tag_configure("summary_new", foreground="#0B5394")

        ttk.Label(main, text="Archivos generados:").pack(anchor="w")
        self.notebook = ttk.Notebook(main, style=self.VERTICAL_NOTEBOOK_STYLE)
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
        content.configure(state="disabled")

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

    def build_tabs(self, files):
        self.clear_tabs()

        detalle_path = "detalle_correos.txt"
        dominios_path = "dominios.txt"

        if detalle_path in files and os.path.exists(detalle_path):
            with open(detalle_path, "r", encoding="utf-8") as f:
                detalle_content = f.read()

            detalle_tab = ttk.Frame(self.notebook)
            self.notebook.add(detalle_tab, text="Detalle correos")
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
        else:
            self.append_log(f"⚠️ No se encontró el archivo esperado: {detalle_path}")

        if dominios_path in files and os.path.exists(dominios_path):
            with open(dominios_path, "r", encoding="utf-8") as f:
                dominios_content = f.read()

            dominios_tab = ttk.Frame(self.notebook)
            self.notebook.add(dominios_tab, text="Dominios")
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
        else:
            self.append_log(f"⚠️ No se encontró el archivo esperado: {dominios_path}")

    def start_report(self):
        email = self.email_var.get().strip()
        if not email:
            messagebox.showwarning("Correo requerido", "Ingresa el correo Gmail a procesar.")
            return

        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        self.set_summary(None)
        self.clear_tabs()

        self.append_log(f"▶️ Iniciando análisis para: {email}")
        self.set_running(True)

        self.worker_thread = threading.Thread(target=self.run_report, args=(email,), daemon=True)
        self.worker_thread.start()

    def run_report(self, email):
        def emit(message):
            self.events.put(("log", message))

        try:
            result = process(user_email=email, log=emit)
            self.events.put(("done", result))
        except Exception as exc:
            self.events.put(("error", str(exc)))

    def consume_events(self):
        try:
            while True:
                event, payload = self.events.get_nowait()
                if event == "log":
                    self.append_log(payload)
                elif event == "done":
                    self.set_running(False)
                    files = payload.get("files", []) if isinstance(payload, dict) else payload
                    summary = payload.get("summary") if isinstance(payload, dict) else None
                    self.build_tabs(files)
                    self.set_summary(summary)
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
