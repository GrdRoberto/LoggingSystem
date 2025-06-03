
import os
import pandas as pd
import zipfile
import yaml
from database import fetch_data

def write_to_zip(dataframe, zip_file, txt_file_name, stop_flag=None, log_callback=None):
    temp_txt_file = f"{txt_file_name}.tmp"
    try:
        dataframe.to_csv(temp_txt_file, index=False, sep='\t')

        if stop_flag and stop_flag():
            if log_callback:
                log_callback("Stop received during archiving. Cleaning up temporary file.", icon="stop")
            os.remove(temp_txt_file)
            return False  # Archiving was interrupted

        with zipfile.ZipFile(zip_file, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(temp_txt_file, txt_file_name)

        return True  # Success

    finally:
        if os.path.exists(temp_txt_file):
            os.remove(temp_txt_file)

def write_to_rar(dataframe, rar_file, txt_file_name, stop_flag=None, log_callback=None):
    # Creează un ZIP cu metoda ZIP_BZIP2 și redenumește extensia în .rar
    import shutil
    temp_txt_file = f"{txt_file_name}.tmp"
    temp_zip_file = rar_file + ".tmpzip"
    try:
        dataframe.to_csv(temp_txt_file, index=False, sep='\t')

        if stop_flag and stop_flag():
            if log_callback:
                log_callback("Stop received during archiving. Cleaning up temporary file.", icon="stop")
            os.remove(temp_txt_file)
            return False

        # Creează arhivă ZIP cu compresie BZIP2
        with zipfile.ZipFile(temp_zip_file, 'w', compression=zipfile.ZIP_BZIP2) as zipf:
            zipf.write(temp_txt_file, txt_file_name)

        # Redenumește fișierul .zip temporar în .rar
        shutil.move(temp_zip_file, rar_file)

        return True
    finally:
        if os.path.exists(temp_txt_file):
            os.remove(temp_txt_file)
        if os.path.exists(temp_zip_file):
            os.remove(temp_zip_file)

def process_day(date, log_callback=None, stop_flag=None, log_directory="logs", config_path="config.yaml"):
    # Încarcă config.yaml pentru a decide modul de comprimare
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        mode = config.get("compression", {}).get("mode", "zip").lower()
    except Exception:
        mode = "zip"

    if mode == "zip":
        archive_file_name = os.path.join(log_directory, f"{date.strftime('%Y_%m_%d')}.zip")
    else:
        archive_file_name = os.path.join(log_directory, f"{date.strftime('%Y_%m_%d')}.rar")
    txt_file_name = f"{date.strftime('%Y_%m_%d')}.txt"

    if os.path.exists(archive_file_name):
        if log_callback:
            log_callback(f"Log file for {date.strftime('%Y-%m-%d')} already exists. Skipping..", icon="skip")
        return

    combined_data = []

    try:
        # FETCHING DATE
        for rows, columns in fetch_data(date):
            if stop_flag and stop_flag():
                if log_callback:
                    log_callback("Processing stopped during fetching, no archive created.", icon="warn")
                return

            df = pd.DataFrame.from_records(rows, columns=columns)
            if not df.empty:
                combined_data.append(df)

        if not combined_data:
            if log_callback:
                log_callback(f"No data found for {date.strftime('%Y-%m-%d')}.", icon="warn")
            return

        # FINAL CHECK STOP BEFORE ARCHIVE
        if stop_flag and stop_flag():
            if log_callback:
                log_callback("Processing stopped before archiving, no archive created.", icon="warn")
            return

        if log_callback:
            log_callback(f"Archiving data for {date.strftime('%Y-%m-%d')}...", icon="archive")

        final_df = pd.concat(combined_data, ignore_index=True)

        if mode == "zip":
            success = write_to_zip(final_df, archive_file_name, txt_file_name, stop_flag=stop_flag, log_callback=log_callback)
        else:
            success = write_to_rar(final_df, archive_file_name, txt_file_name, stop_flag=stop_flag, log_callback=log_callback)

        if not success:
            if log_callback:
                log_callback(f"Archiving aborted by user for {date.strftime('%Y-%m-%d')}.", icon="warn")
            # Delete incomplete file
            if os.path.exists(archive_file_name):
                os.remove(archive_file_name)
            return

        if log_callback:
            log_callback(f"Data processed and archived for {date.strftime('%Y-%m-%d')}.", icon="done")

    except ConnectionError as ce:
        if log_callback:
            log_callback(f"Connection lost during processing {date.strftime('%Y-%m-%d')}: {ce}", icon="error")
    except Exception as e:
        if log_callback:
            log_callback(f"Error processing {date.strftime('%Y-%m-%d')}: {e}", icon="x")
