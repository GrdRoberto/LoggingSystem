import ttkbootstrap as tb
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Messagebox
from ttkbootstrap.widgets import DateEntry
import tkinter as tk
from tkinter import scrolledtext
from datetime import datetime, timedelta
import threading
import os
import sys
from PIL import Image, ImageTk

from database import check_connection, load_config
from data_processing import process_day
from logging_setup import configure_logging


#functio for creation the exe with resource not delete this
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS  
    except Exception:
        base_path = os.path.abspath(".") #load current folder when execute the
    return os.path.join(base_path, relative_path)

class LoggingApp(tb.Window):
    def __init__(self):
        super().__init__(themename="darkly")
        self.title("Gimatico Logging System")
        self.geometry("750x550")
        self.resizable(True, True)

        icon_path = resource_path("icon.ico")
        try:
            self.iconbitmap(icon_path)
        except Exception as e:
            print(f"Failed to load icon: {e}")


        config = load_config()
        if not config:
            self.log_directory = None
        else:
            self.log_directory = config['database']['log_directory']
            os.makedirs(self.log_directory, exist_ok=True)
            configure_logging(self.log_directory)

        self.stop_processing = False
        self.icons = {}
        self.create_widgets()
        self.load_icons()

        if not config:
            self.log_message("Configuration file is missing or invalid.", icon="error")
            self.start_btn.configure(state=tk.DISABLED)
            self.stop_btn.configure(state=tk.DISABLED)

    def create_widgets(self):
        header = tb.Label(self, text="Gimatico Logging System", font=("Helvetica", 20, "bold"), foreground="#5dade2")
        header.pack(pady=(20, 5))

        subtitle = tb.Label(self, text="by GrdRoberto", font=("Helvetica", 10, "italic"), foreground="#888888")
        subtitle.pack(pady=(0, 15))

        date_frame = tb.Frame(self)
        date_frame.pack(pady=10)

        tb.Label(date_frame, text="Start Date:", font=('Helvetica', 11)).grid(row=0, column=0, padx=10)
        self.start_date_entry = DateEntry(date_frame, bootstyle="success", dateformat="%Y-%m-%d")
        self.start_date_entry.grid(row=0, column=1, padx=5, pady=5)

        tb.Label(date_frame, text="End Date:", font=('Helvetica', 11)).grid(row=0, column=2, padx=10)
        self.end_date_entry = DateEntry(date_frame, bootstyle="success", dateformat="%Y-%m-%d")
        self.end_date_entry.grid(row=0, column=3, padx=5, pady=5)

        self.log_area = scrolledtext.ScrolledText(self, height=14, font=("Consolas", 11), wrap=tk.WORD, bg="#1e1e1e", fg="white")
        self.log_area.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        self.log_area.config(state='disabled')

        self.progress = tb.Progressbar(self, orient="horizontal", mode="indeterminate", bootstyle=SUCCESS, length=300)
        self.progress.pack(pady=10)

        btn_frame = tb.Frame(self)
        btn_frame.pack(pady=5)

        self.start_btn = tb.Button(btn_frame, text="Start Processing", command=self.start_processing, bootstyle=SUCCESS)
        self.start_btn.pack(side=tk.LEFT, padx=10)

        self.stop_btn = tb.Button(btn_frame, text="Stop", command=self.stop_processing_action, bootstyle=DANGER)
        self.stop_btn.pack(side=tk.LEFT, padx=10)
        self.stop_btn.configure(state=tk.DISABLED)

    #Load icon folder
    def load_icons(self):
        icon_size = 16
        icons_folder = resource_path("icons")

        if not os.path.exists(icons_folder):
            print("Icons folder not found:", icons_folder)
            return

        for filename in os.listdir(icons_folder):
            if filename.lower().endswith(".png"):
                name = os.path.splitext(filename)[0] 
                path = os.path.join(icons_folder, filename)
                try:
                    img = Image.open(path).resize((icon_size, icon_size), Image.LANCZOS)
                    self.icons[name] = ImageTk.PhotoImage(img)
                except Exception as e:
                    print(f"Failed to load icon '{filename}': {e}")

    def log_message(self, message, icon=None):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.log_area.config(state='normal')
        self.log_area.insert(tk.END, f"[{timestamp}] ")
        if icon and icon in self.icons:
            self.log_area.image_create(tk.END, image=self.icons[icon])
            self.log_area.insert(tk.END, " ")
        self.log_area.insert(tk.END, f"{message}\n")
        self.log_area.yview(tk.END)
        self.log_area.config(state='disabled')

    def stop_processing_action(self):
        self.progress.stop()
        self.stop_processing = True
        self.stop_btn.configure(state=tk.DISABLED)
        self.start_btn.configure(state=tk.NORMAL)
        self.log_message("Stop process requested by user.", icon="stop")

    def stop_flag(self):
        return self.stop_processing

    def start_processing(self):
        if not self.log_directory:
            self.safe_log("Cannot start processing: configuration missing.", icon="error")
            return

        start_date_str = self.start_date_entry.entry.get()
        end_date_str = self.end_date_entry.entry.get()

        if start_date_str > end_date_str:
            self.safe_log("Start date cannot be after end date.", icon="error")
            return

        self.progress.start(20)
        self.stop_processing = False
        self.start_btn.configure(state=tk.DISABLED)
        self.stop_btn.configure(state=tk.NORMAL)

        def run():
            self.safe_log("Checking server connection..", icon="server")
            connection_result = {}

            def connection_task():
                connection_result["status"] = check_connection()

            conn_thread = threading.Thread(target=connection_task)
            conn_thread.start()
            conn_thread.join(timeout=5)

            if conn_thread.is_alive():
                if self.stop_processing:
                    self.safe_log("Connection check interrupted by user.", icon="warn")
                else:
                    self.safe_log("Connection time out - server not found", icon="error")
                self.progress.stop()
                self.start_btn.configure(state=tk.NORMAL)
                self.stop_btn.configure(state=tk.DISABLED)
                return
            self.safe_log("Server connected.", icon="check")
            self.safe_log(f"Processing data from {start_date_str} to {end_date_str}..", icon="process")

            current_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

            while current_date <= end_date:
                if self.stop_processing:
                    self.safe_log("Processing aborted by user.", icon="stop")
                    break

                day_str = current_date.strftime('%Y-%m-%d')
                self.safe_log(f"Fetching data for {day_str}..", icon="fetch")

                # call process_day
                process_day(
                    current_date,
                    log_callback=self.safe_log,
                    stop_flag=self.stop_flag,
                    log_directory=self.log_directory
                )

                if self.stop_processing:
                    self.safe_log(f"Processing stopped by user during processing of {day_str}.", icon="warn")
                    break

                current_date += timedelta(days=1)

            self.after(0, self.progress.stop)
            self.after(0, lambda: self.start_btn.configure(state=tk.NORMAL))
            self.after(0, lambda: self.stop_btn.configure(state=tk.DISABLED))
            self.safe_log("All Processing complete.", icon="done")

        threading.Thread(target=run, daemon=True).start()



    def safe_log(self, message, icon=None):
        self.after(0, lambda: self.log_message(message, icon))


if __name__ == "__main__":
    app = LoggingApp()
    app.mainloop()
