import duckdb
import os
import hashlib
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IngestWatcher")

DB_PATH = "data/outputs/scalable.db"
BRONZE_DIR = "data/outputs/bronze_listens"
INPUT_DIR = "data/inputs"

def get_file_hash(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()

def process_file(file_path):
    file_name = os.path.basename(file_path)
    if not file_name.lower().endswith('.txt'):
        return

    con = duckdb.connect(DB_PATH)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS processed_files (content_hash VARCHAR PRIMARY KEY, filename VARCHAR)")
        file_hash = get_file_hash(file_path)
        
        exists = con.execute("SELECT 1 FROM processed_files WHERE content_hash = ?", [file_hash]).fetchone()
        if exists:
            return

        logger.info(f"-> Bronze: Ingesting new file: {file_name}")
        con.execute(f"""
            COPY (SELECT *, (track_metadata->>'track_name') as track_name, (track_metadata->>'artist_name') as artist_name
            FROM read_json('{file_path}', format='newline_delimited',
            columns={{'user_name': 'VARCHAR', 'listened_at': 'BIGINT', 'recording_msid': 'VARCHAR', 'track_metadata': 'JSON'}}))
            TO '{BRONZE_DIR}' (FORMAT 'PARQUET', PARTITION_BY (user_name), OVERWRITE_OR_IGNORE 1)
        """)
        con.execute("INSERT INTO processed_files VALUES (?, ?)", [file_hash, file_name])
    except Exception as e:
        logger.error(f"Failed to process {file_name}: {e}")
    finally:
        con.close()

class DataFolderHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            process_file(event.src_path)

def run_ingest():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(BRONZE_DIR, exist_ok=True)

    logger.info(f"Performing initial scan of {INPUT_DIR}...")
    if os.path.exists(INPUT_DIR):
        for f in os.listdir(INPUT_DIR):
            process_file(os.path.join(INPUT_DIR, f))

    logger.info("Initial scan complete. Waiting for new files (Ctrl+C to stop)...")
    event_handler = DataFolderHandler()
    observer = Observer()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    run_ingest()