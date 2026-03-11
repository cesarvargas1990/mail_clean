import os
import io
import queue
import threading
import csv
import re
import json
import webbrowser
from glob import glob
from contextlib import redirect_stdout
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText

from gmail_stats import process as process_gmail
from outlook_stats import process as process_outlook
from drive_stats import list_drive, delete_drive_file
from drive_stats2 import process_extensions
from onedrive_stats import list_onedrive, delete_onedrive_file


class GmailReportApp:
    VERTICAL_NOTEBOOK_STYLE = "Vertical.TNotebook"
    REQUIRED_EMAIL_TITLE = "Correo requerido"
    EVT_CLICK = "<Button-1>"
    EVT_ENTER = "<Enter>"
    EVT_LEAVE = "<Leave>"
    EVT_COMBO_SELECTED = "<<ComboboxSelected>>"
    URL_PATTERN = re.compile(r"https?://[^\s;]+")
    DRIVE_LIST_FILE = "drive_archivos.csv"
    DRIVE_SUMMARY_FILE = "resumen_extensiones.txt"
    GMAIL_SCAN_STATE_FILE = "gmail_scan_state.json"
    EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    CANCEL_MESSAGE = "Operación cancelada por el usuario."

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
        self.mail_stop_event = threading.Event()
        self.drive_stop_event = threading.Event()
        self.link_tag_counter = 0

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
        self.mail_email_combo_var = tk.StringVar()
        self.mail_email_input_frame = ttk.Frame(form)
        self.mail_email_input_frame.pack(side="left", padx=(8, 10))
        self.email_entry = ttk.Entry(self.mail_email_input_frame, textvariable=self.email_var, width=48)
        self.email_entry.pack(side="left")
        self.email_combo = ttk.Combobox(
            self.mail_email_input_frame,
            textvariable=self.mail_email_combo_var,
            width=24,
            state="readonly",
        )
        self.email_combo.bind(self.EVT_COMBO_SELECTED, self.on_mail_email_selected)
        self.mail_email_combo_visible = False
        self.refresh_mail_email_selector()

        self.mail_provider_combo.bind(self.EVT_COMBO_SELECTED, self.on_provider_change)

        self.run_button = ttk.Button(form, text="Generar reporte", command=self.start_report)
        self.run_button.pack(side="left")
        self.stop_button = ttk.Button(form, text="Detener", command=self.stop_report, state="disabled")
        self.stop_button.pack(side="left", padx=(8, 0))

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
        self.drive_provider_combo.bind(self.EVT_COMBO_SELECTED, self.on_drive_provider_change)

        ttk.Label(form, text="Correo Drive:").pack(side="left")
        self.drive_email_var = tk.StringVar()
        self.drive_email_combo_var = tk.StringVar()
        self.drive_email_input_frame = ttk.Frame(form)
        self.drive_email_input_frame.pack(side="left", padx=(8, 10))
        self.drive_email_entry = ttk.Entry(self.drive_email_input_frame, textvariable=self.drive_email_var, width=38)
        self.drive_email_entry.pack(side="left")
        self.drive_email_combo = ttk.Combobox(
            self.drive_email_input_frame,
            textvariable=self.drive_email_combo_var,
            width=24,
            state="readonly",
        )
        self.drive_email_combo.bind(self.EVT_COMBO_SELECTED, self.on_drive_email_selected)
        self.drive_email_combo_visible = False
        self.refresh_drive_email_selector()

        self.drive_run_button = ttk.Button(form, text="Generar listado Drive", command=self.start_drive_report)
        self.drive_run_button.pack(side="left")
        self.drive_stop_button = ttk.Button(form, text="Detener", command=self.stop_drive_report, state="disabled")
        self.drive_stop_button.pack(side="left", padx=(8, 0))
        self.drive_open_button = ttk.Button(form, text="Abrir último Drive", command=self.open_last_drive_report)
        self.drive_open_button.pack(side="left", padx=(8, 0))

        ttk.Label(parent, text="Estado Drive:").pack(anchor="w")
        self.drive_log_box = ScrolledText(parent, height=8, wrap="word", state="disabled")
        self.drive_log_box.pack(fill="x", pady=(2, 6))

        ttk.Label(parent, text="Archivos Drive generados:").pack(anchor="w")
        self.drive_notebook = ttk.Notebook(parent)
        self.drive_notebook.pack(fill="both", expand=True)

    def append_log(self, message):
        self.append_text_with_links(self.log_box, message)

    def append_drive_log(self, message):
        self.append_text_with_links(self.drive_log_box, message)

    def append_text_with_links(self, text_widget, message):
        text_widget.configure(state="normal")
        start_index = text_widget.index("end-1c")
        text_widget.insert("end", f"{message}\n")

        for match in self.URL_PATTERN.finditer(message):
            tag = f"log_url_{self.link_tag_counter}"
            self.link_tag_counter += 1
            url = match.group(0)
            tag_start = f"{start_index}+{match.start()}c"
            tag_end = f"{start_index}+{match.end()}c"
            text_widget.tag_add(tag, tag_start, tag_end)
            text_widget.tag_config(tag, foreground="#1a73e8", underline=True)
            text_widget.tag_bind(tag, self.EVT_CLICK, lambda _e, link=url: webbrowser.open(link))
            text_widget.tag_bind(tag, self.EVT_ENTER, lambda _e: text_widget.configure(cursor="hand2"))
            text_widget.tag_bind(tag, self.EVT_LEAVE, lambda _e: text_widget.configure(cursor="xterm"))

        text_widget.see("end")
        text_widget.configure(state="disabled")

    def set_running(self, running):
        if running:
            self.run_button.configure(state="disabled")
            self.stop_button.configure(state="normal")
            self.set_mail_email_input_state("disabled")
            self.mail_provider_combo.configure(state="disabled")
        else:
            self.run_button.configure(state="normal")
            self.stop_button.configure(state="disabled")
            self.set_mail_email_input_state("normal")
            self.mail_provider_combo.configure(state="readonly")

    def on_provider_change(self, _event=None):
        provider = self.mail_provider_var.get()
        if provider == "Outlook":
            self.append_log("ℹ️ Proveedor seleccionado: Outlook")
        else:
            self.append_log("ℹ️ Proveedor seleccionado: Gmail")

    def on_mail_email_selected(self, _event=None):
        selected = self.mail_email_combo_var.get().strip()
        if selected:
            self.email_var.set(selected)

    def on_drive_provider_change(self, _event=None):
        self.refresh_drive_email_selector()
        self.append_drive_log(f"ℹ️ Proveedor Drive seleccionado: {self.drive_provider_var.get()}")

    def on_drive_email_selected(self, _event=None):
        selected = self.drive_email_combo_var.get().strip()
        if selected:
            self.drive_email_var.set(selected)

    @staticmethod
    def _guess_email_from_safe_key(safe_key):
        if not safe_key:
            return ""
        parts = safe_key.split("_")
        if len(parts) >= 3:
            local = ".".join(parts[:-2]).strip(".")
            domain = f"{parts[-2]}.{parts[-1]}"
            if local and domain:
                return f"{local}@{domain}"
        return ""

    def _extract_emails_from_file(self, file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            return []
        return [email.lower() for email in self.EMAIL_REGEX.findall(content)]

    def _collect_emails_from_token_patterns(self, patterns):
        emails = set()
        for pattern in patterns:
            for path in glob(pattern):
                emails.update(self._extract_emails_from_file(path))
        return emails

    def _collect_emails_from_prefixed_files(self, pattern_prefix_pairs):
        emails = set()
        for pattern, prefix in pattern_prefix_pairs:
            for path in glob(pattern):
                name = os.path.basename(path)
                if not (name.endswith(".txt") or name.endswith(".csv")):
                    continue
                if not name.startswith(prefix):
                    continue
                safe = name[len(prefix):name.rfind(".")]
                guessed = self._safe_to_email(safe)
                if guessed:
                    emails.add(guessed.lower())
        return emails

    @staticmethod
    def _safe_to_email(safe_key):
        return GmailReportApp._guess_email_from_safe_key(safe_key)

    def get_previous_mail_emails(self):
        emails = set()

        if os.path.exists(self.GMAIL_SCAN_STATE_FILE):
            try:
                with open(self.GMAIL_SCAN_STATE_FILE, "r", encoding="utf-8") as f:
                    state = json.load(f)
                for raw_email in state.keys():
                    if isinstance(raw_email, str) and "@" in raw_email:
                        emails.add(raw_email.strip().lower())
            except Exception:
                pass

        emails.update(self._collect_emails_from_token_patterns(["token_outlook_*.json"]))
        emails.update(
            self._collect_emails_from_prefixed_files(
                [
                    ("detalle_correos_*.txt", "detalle_correos_"),
                    ("detalle_correos_outlook_*.txt", "detalle_correos_outlook_"),
                ]
            )
        )

        return sorted(emails)

    def get_previous_drive_emails(self, provider):
        if provider == "OneDrive":
            file_prefixes = [("onedrive_archivos_*.csv", "onedrive_archivos_")]
            token_patterns = ["token_onedrive_*.json"]
        else:
            file_prefixes = [("drive_archivos_*.csv", "drive_archivos_")]
            token_patterns = ["token_drive_*.json"]

        emails = set()
        emails.update(self._collect_emails_from_prefixed_files(file_prefixes))
        emails.update(self._collect_emails_from_token_patterns(token_patterns))

        return sorted(emails)

    def refresh_mail_email_selector(self):
        current = self.email_var.get().strip()
        previous = self.get_previous_mail_emails()

        self.email_combo.pack_forget()
        self.mail_email_combo_visible = False

        if previous:
            self.email_combo.configure(values=previous, state="readonly")
            self.email_combo.pack(side="left", padx=(6, 0))
            self.mail_email_combo_visible = True
            if current in previous:
                self.mail_email_combo_var.set(current)
            elif not current:
                self.email_var.set(previous[0])
                self.mail_email_combo_var.set(previous[0])
            else:
                self.mail_email_combo_var.set("")
        else:
            self.mail_email_combo_var.set("")
            if current:
                self.email_var.set(current)

    def refresh_drive_email_selector(self):
        current = self.drive_email_var.get().strip()
        provider = self.drive_provider_var.get()
        previous = self.get_previous_drive_emails(provider)

        self.drive_email_combo.pack_forget()
        self.drive_email_combo_visible = False

        if previous:
            self.drive_email_combo.configure(values=previous, state="readonly")
            self.drive_email_combo.pack(side="left", padx=(6, 0))
            self.drive_email_combo_visible = True
            if current in previous:
                self.drive_email_combo_var.set(current)
            elif not current:
                self.drive_email_var.set(previous[0])
                self.drive_email_combo_var.set(previous[0])
            else:
                self.drive_email_combo_var.set("")
        else:
            self.drive_email_combo_var.set("")
            if current:
                self.drive_email_var.set(current)

    def set_mail_email_input_state(self, state):
        self.email_entry.configure(state=state if state in ("disabled", "normal") else "normal")
        if self.mail_email_combo_visible:
            combo_state = "disabled" if state == "disabled" else "readonly"
            self.email_combo.configure(state=combo_state)

    def set_drive_email_input_state(self, state):
        self.drive_email_entry.configure(state=state if state in ("disabled", "normal") else "normal")
        if self.drive_email_combo_visible:
            combo_state = "disabled" if state == "disabled" else "readonly"
            self.drive_email_combo.configure(state=combo_state)

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

    @staticmethod
    def prepare_csv_rows(columns, data_rows):
        display_rows = []
        original_map = {}

        for idx, row in enumerate(data_rows):
            normalized = (row + [""] * (len(columns) - len(row)))[:len(columns)]
            row_map = dict(zip(columns, normalized))

            display_map = dict(row_map)
            if "view_url" in display_map:
                display_map["view_url"] = "🔗 Abrir"
            if "download_url" in display_map:
                display_map["download_url"] = "⬇ Descargar"

            display_rows.append([display_map.get(col, "") for col in columns])
            original_map[idx] = row_map

        return display_rows, original_map

    @staticmethod
    def get_column_weights(columns):
        weights = {}
        for col in columns:
            if col == "full_path":
                weights[col] = 4
            elif col in ("view_url", "download_url"):
                weights[col] = 2
            else:
                weights[col] = 1
        return weights

    @staticmethod
    def clear_link_widget(label_widget):
        label_widget.config(text="", cursor="arrow", font=("TkDefaultFont", 10))

    def set_link_widget(self, label_widget, text, url):
        if not url:
            self.clear_link_widget(label_widget)
            label_widget.unbind(self.EVT_CLICK)
            label_widget.unbind(self.EVT_ENTER)
            label_widget.unbind(self.EVT_LEAVE)
            return

        label_widget.config(text=text, cursor="hand2", font=("TkDefaultFont", 10, "underline"), fg="#1a73e8")
        label_widget.bind(self.EVT_CLICK, lambda _e, link=url: webbrowser.open(link))
        label_widget.bind(self.EVT_ENTER, lambda _e: label_widget.config(fg="#0b57d0"))
        label_widget.bind(self.EVT_LEAVE, lambda _e: label_widget.config(fg="#1a73e8"))

    @staticmethod
    def fit_tree_columns(tree, frame, columns, column_weights):
        total_width = max(tree.winfo_width(), frame.winfo_width()) - 24
        if total_width <= 0:
            return
        total_weight = sum(column_weights.values())
        for col in columns:
            proportion = column_weights[col] / total_weight
            width = max(90, int(total_width * proportion))
            tree.column(col, width=width)

    def build_link_frame(self, frame, row=2):
        link_frame = ttk.Frame(frame)
        link_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        link_frame.grid_columnconfigure(0, weight=1)
        link_frame.grid_columnconfigure(1, weight=1)

        view_link = tk.Label(link_frame, text="", fg="#1a73e8", cursor="arrow", anchor="w")
        download_link = tk.Label(link_frame, text="", fg="#1a73e8", cursor="arrow", anchor="w")
        view_link.grid(row=0, column=0, sticky="w")
        download_link.grid(row=0, column=1, sticky="w")
        return view_link, download_link

    @staticmethod
    def format_column_title(column_name):
        return " ".join(part.capitalize() for part in column_name.split("_"))

    @staticmethod
    def infer_numeric_column(tree, column, original_rows_map):
        for item_id in tree.get_children():
            raw = original_rows_map.get(item_id, {}).get(column, "")
            if raw in (None, ""):
                continue
            try:
                float(raw)
            except ValueError:
                return False
        return True

    @staticmethod
    def _sort_key(value, numeric):
        if value in (None, ""):
            return float("-inf") if numeric else ""
        if numeric:
            try:
                return float(value)
            except ValueError:
                return float("-inf")
        return str(value).lower()

    def sort_tree_column(self, tree, column, reverse, original_rows_map):
        numeric = self.infer_numeric_column(tree, column, original_rows_map)
        items = list(tree.get_children())
        items.sort(
            key=lambda item_id: self._sort_key(original_rows_map.get(item_id, {}).get(column, ""), numeric),
            reverse=reverse,
        )

        for index, item_id in enumerate(items):
            tree.move(item_id, "", index)

        title = self.format_column_title(column)
        marker = "↓" if reverse else "↑"
        tree.heading(
            column,
            text=f"{title} {marker}",
            command=lambda c=column, r=(not reverse): self.sort_tree_column(tree, c, r, original_rows_map),
        )

    def add_csv_grid_tab(self, parent_notebook, title, csv_path):
        frame = ttk.Frame(parent_notebook)
        parent_notebook.add(frame, text=title)

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            rows = list(reader)

        if not rows:
            self.add_text_tab(parent_notebook, f"{title}-vacío", "Sin datos")
            return

        columns = rows[0]
        data_rows = rows[1:]
        display_rows, original_map_by_index = self.prepare_csv_rows(columns, data_rows)
        column_weights = self.get_column_weights(columns)

        search_frame = ttk.Frame(frame)
        search_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        search_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(search_frame, text="Buscar:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=search_var)
        search_entry.grid(row=0, column=1, sticky="ew")

        tree = ttk.Treeview(frame, columns=columns, show="headings")
        y_scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        x_scroll = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        for col in columns:
            display_title = self.format_column_title(col)
            tree.heading(
                col,
                text=display_title,
                command=lambda c=col: self.sort_tree_column(tree, c, False, original_rows_map),
            )
            tree.column(col, width=120, anchor="w", stretch=False)

        tree.grid(row=1, column=0, sticky="nsew")
        y_scroll.grid(row=1, column=1, sticky="ns")
        x_scroll.grid(row=2, column=0, sticky="ew")

        view_link, download_link = self.build_link_frame(frame, row=3)

        frame.grid_rowconfigure(1, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        original_rows_map = {}
        all_row_data = []
        for idx, row in enumerate(display_rows):
            all_row_data.append(
                {
                    "display_values": row,
                    "original_values": original_map_by_index.get(idx, {}),
                    "search_blob": " ".join(value.lower() for value in row if value),
                }
            )

        def persist_rows():
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(columns)
                for row_info in all_row_data:
                    writer.writerow([row_info["original_values"].get(col, "") for col in columns])

        def fit_columns(_event=None):
            self.fit_tree_columns(tree, frame, columns, column_weights)

        def clear_link_labels():
            self.set_link_widget(view_link, "", "")
            self.set_link_widget(download_link, "", "")

        def populate_tree(filtered_rows):
            selected_original = None
            selected = tree.selection()
            if selected:
                selected_original = original_rows_map.get(selected[0], {})

            original_rows_map.clear()
            for item_id in tree.get_children():
                tree.delete(item_id)

            new_selection = None
            for row_info in filtered_rows:
                item_id = tree.insert("", "end", values=row_info["display_values"])
                original_rows_map[item_id] = row_info["original_values"]
                if selected_original and row_info["original_values"] == selected_original:
                    new_selection = item_id

            if new_selection:
                tree.selection_set(new_selection)
                tree.focus(new_selection)
                tree.see(new_selection)
            else:
                clear_link_labels()

            fit_columns()

        def apply_filter(*_args):
            search_term = search_var.get().strip().lower()
            if not search_term:
                filtered_rows = all_row_data
            else:
                filtered_rows = [row for row in all_row_data if search_term in row["search_blob"]]
            populate_tree(filtered_rows)

        def get_selected_row_data():
            selected = tree.selection()
            if not selected:
                return None
            return original_rows_map.get(selected[0], {})

        def on_select(_event=None):
            row_data = get_selected_row_data()
            if not row_data:
                clear_link_labels()
                return
            self.set_link_widget(view_link, "🔗 Abrir enlace", row_data.get("view_url", ""))
            self.set_link_widget(download_link, "⬇ Descargar archivo", row_data.get("download_url", ""))

        def open_row_url(url):
            if url and url.startswith("http"):
                webbrowser.open(url)

        def open_selected_link(_event=None):
            row_data = get_selected_row_data()
            if not row_data:
                return
            open_row_url(row_data.get("view_url") or row_data.get("download_url"))

        def download_selected_file():
            row_data = get_selected_row_data()
            if not row_data:
                return
            open_row_url(row_data.get("download_url") or row_data.get("view_url"))

        def view_selected_file():
            row_data = get_selected_row_data()
            if not row_data:
                return
            open_row_url(row_data.get("view_url") or row_data.get("download_url"))

        def delete_selected_file():
            row_data = get_selected_row_data()
            if not row_data:
                return

            full_path = row_data.get("full_path", "(sin ruta)")
            file_id = row_data.get("file_id", "")
            if not file_id:
                messagebox.showerror("Eliminar archivo", "No se encontró el identificador del archivo.")
                return

            confirmed = messagebox.askyesno(
                "Eliminar archivo",
                f"Se eliminará este archivo del proveedor remoto:\n\n{full_path}\n\n¿Deseas continuar?",
            )
            if not confirmed:
                return

            try:
                provider = self.drive_provider_var.get()
                drive_email = self.drive_email_var.get().strip() or None
                if provider == "OneDrive":
                    delete_onedrive_file(file_id, drive_email)
                else:
                    delete_drive_file(file_id, drive_email)

                for idx, row_info in enumerate(all_row_data):
                    if row_info["original_values"].get("file_id") == file_id:
                        all_row_data.pop(idx)
                        break

                persist_rows()
                apply_filter()
                self.append_drive_log(f"🗑 Archivo eliminado: {full_path}")
            except Exception as exc:
                messagebox.showerror("Eliminar archivo", str(exc))

        def update_heading_cursor(event):
            region = tree.identify_region(event.x, event.y)
            tree.configure(cursor="hand2" if region == "heading" else "")

        def reset_cursor(_event=None):
            tree.configure(cursor="")

        context_menu = tk.Menu(tree, tearoff=0)
        context_menu.add_command(label="Visualizar", command=view_selected_file)
        context_menu.add_command(label="Descargar", command=download_selected_file)
        context_menu.add_separator()
        context_menu.add_command(label="Eliminar", command=delete_selected_file)

        def show_context_menu(event):
            item_id = tree.identify_row(event.y)
            if not item_id:
                return
            tree.selection_set(item_id)
            tree.focus(item_id)
            on_select()
            context_menu.tk_popup(event.x_root, event.y_root)
            context_menu.grab_release()

        tree.bind("<<TreeviewSelect>>", on_select)
        tree.bind("<Double-1>", open_selected_link)
        tree.bind("<Button-3>", show_context_menu)
        tree.bind("<Configure>", fit_columns)
        tree.bind("<Motion>", update_heading_cursor)
        tree.bind("<Leave>", reset_cursor)
        frame.bind("<Configure>", fit_columns)
        search_var.trace_add("write", apply_filter)
        populate_tree(all_row_data)

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
            text_widget.tag_bind(tag, self.EVT_CLICK, lambda _e, link=url: webbrowser.open(link))
            text_widget.tag_bind(tag, self.EVT_ENTER, lambda _e: text_widget.configure(cursor="hand2"))
            text_widget.tag_bind(tag, self.EVT_LEAVE, lambda _e: text_widget.configure(cursor="xterm"))

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
            if path.lower().endswith(".csv"):
                self.add_csv_grid_tab(self.drive_notebook, tab_title, path)
            else:
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
        self.drive_stop_event.clear()
        self.set_drive_running(True)
        self.append_drive_log(f"▶️ Iniciando análisis de {provider} para: {drive_email}")
        self.drive_worker_thread = threading.Thread(target=self.run_drive_report, args=(drive_email, provider), daemon=True)
        self.drive_worker_thread.start()

    def set_drive_running(self, running):
        if running:
            self.drive_run_button.configure(state="disabled")
            self.drive_stop_button.configure(state="normal")
            self.drive_open_button.configure(state="disabled")
            self.set_drive_email_input_state("disabled")
            self.drive_provider_combo.configure(state="disabled")
        else:
            self.drive_run_button.configure(state="normal")
            self.drive_stop_button.configure(state="disabled")
            self.drive_open_button.configure(state="normal")
            self.set_drive_email_input_state("normal")
            self.drive_provider_combo.configure(state="readonly")

    def stop_report(self):
        if self.mail_worker_thread and self.mail_worker_thread.is_alive():
            self.mail_stop_event.set()
            self.append_log("⏹ Solicitud de detención enviada. Esperando punto seguro de cancelación...")

    def stop_drive_report(self):
        if self.drive_worker_thread and self.drive_worker_thread.is_alive():
            self.drive_stop_event.set()
            self.append_drive_log("⏹ Solicitud de detención enviada. Esperando punto seguro de cancelación...")

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
            def emit_drive(message):
                self.events.put(("drive_log", message))

            if provider == "OneDrive":
                list_file = list_onedrive(drive_email, log=emit_drive, stop_event=self.drive_stop_event)
            else:
                list_file = list_drive(drive_email, stop_event=self.drive_stop_event)
            if self.drive_stop_event.is_set():
                raise RuntimeError(self.CANCEL_MESSAGE)
            summary_file = process_extensions(input_file=list_file)
            files = [list_file, summary_file]
            self.events.put(("drive_done", {"files": files}))
        except Exception as exc:
            message = str(exc)
            if message == self.CANCEL_MESSAGE or self.drive_stop_event.is_set():
                self.events.put(("drive_cancelled", message or self.CANCEL_MESSAGE))
            else:
                self.events.put(("drive_error", message))

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
        self.mail_stop_event.clear()

        self.append_log(f"▶️ Iniciando análisis {provider} para: {email}")
        self.set_running(True)

        self.mail_worker_thread = threading.Thread(target=self.run_report, args=(email, provider), daemon=True)
        self.mail_worker_thread.start()

    def run_report(self, email, provider):
        def emit(message):
            self.events.put(("log", message))

        try:
            if provider == "Outlook":
                result = process_outlook(user_email=email, log=emit, stop_event=self.mail_stop_event)
            else:
                result = process_gmail(user_email=email, log=emit, stop_event=self.mail_stop_event)
            self.events.put(("done", result))
        except Exception as exc:
            message = str(exc)
            if message == self.CANCEL_MESSAGE or self.mail_stop_event.is_set():
                self.events.put(("cancelled", message or self.CANCEL_MESSAGE))
            else:
                self.events.put(("error", message))

    def handle_mail_done_event(self, payload):
        self.set_running(False)
        files = payload.get("files", []) if isinstance(payload, dict) else payload
        summary = payload.get("summary") if isinstance(payload, dict) else None
        self.build_tabs(files)
        self.set_summary(summary)
        self.refresh_mail_email_selector()
        self.append_log("✅ Reporte finalizado y cargado en pestañas.")

    def handle_drive_done_event(self, payload):
        self.set_drive_running(False)
        files = payload.get("files", []) if isinstance(payload, dict) else payload
        self.render_drive_tabs(files)
        self.refresh_drive_email_selector()
        self.append_drive_log("✅ Reporte de Drive finalizado y cargado en pestañas.")

    def handle_error_event(self, payload):
        self.set_running(False)
        self.append_log("❌ Error durante el procesamiento:")
        self.append_log(payload)

    def handle_cancelled_event(self, payload):
        self.set_running(False)
        self.append_log(f"⏹ {payload or self.CANCEL_MESSAGE}")

    def handle_drive_error_event(self, payload):
        self.set_drive_running(False)
        self.append_drive_log("❌ Error durante el procesamiento de Drive:")
        self.append_drive_log(payload)

    def handle_drive_cancelled_event(self, payload):
        self.set_drive_running(False)
        self.append_drive_log(f"⏹ {payload or self.CANCEL_MESSAGE}")

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
        elif event == "drive_cancelled":
            self.handle_drive_cancelled_event(payload)
        elif event == "error":
            self.handle_error_event(payload)
        elif event == "cancelled":
            self.handle_cancelled_event(payload)

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
