import logging
import os

def configure_logging(log_directory):
    os.makedirs(log_directory, exist_ok=True)
    log_path = os.path.join(log_directory, "app.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s]: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
