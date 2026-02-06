import duckdb
import os
import hashlib
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Lock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IngestWatcher")

DB_PATH = os.environ.get("DB_PATH", "data/outputs/scalable.db")
BRONZE_DIR = os.environ.get("BRONZE_DIR", "data/outputs/bronze_listens")
INPUT_DIR = os.environ.get("INPUT_DIR", "data/inputs")

db_lock = Lock()

def get_file_hash(filepath):
    """Calculate SHA256 hash of file content."""
    hasher = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()
    except FileNotFoundError:
        logger.warning(f"File not found while hashing: {filepath}")
        return None
    except Exception as e:
        logger.error(f"Error hashing file {filepath}: {e}")
        return None

def process_file(file_path):
    """Process a single file and ingest into bronze layer."""
    file_name = os.path.basename(file_path)
    
    # Only process .txt files
    if not file_name.lower().endswith('.txt'):
        logger.debug(f"Skipping non-.txt file: {file_name}")
        return
    
    # Check if file exists
    if not os.path.exists(file_path):
        logger.warning(f"File not found (may have been moved): {file_name}")
        return
    
    # Wait to ensure file is fully written
    time.sleep(0.2)
    
    with db_lock:
        # Use context manager - connection closes automatically
        with duckdb.connect(DB_PATH) as con:
            try:
                # Create tracking table
                con.execute("CREATE TABLE IF NOT EXISTS processed_files (content_hash VARCHAR PRIMARY KEY, filename VARCHAR)")
                
                # Calculate file hash
                file_hash = get_file_hash(file_path)
                if file_hash is None:
                    logger.error(f"Could not read file: {file_name}")
                    return
                
                # Check if already processed
                exists = con.execute("SELECT 1 FROM processed_files WHERE content_hash = ?", [file_hash]).fetchone()
                
                if exists:
                    logger.debug(f"Skipping already processed file: {file_name}")
                    return
                
                logger.info(f"-> Bronze: Ingesting new file: {file_name}")
                
                # Ingest to bronze layer as partitioned parquet
                con.execute(f"""
                    COPY (SELECT *, 
                        (track_metadata->>'track_name') as track_name,
                        (track_metadata->>'artist_name') as artist_name
                    FROM read_json('{file_path}', 
                        format='newline_delimited',
                        columns={{'user_name': 'VARCHAR', 'listened_at': 'BIGINT', 'recording_msid': 'VARCHAR', 'track_metadata': 'JSON'}}))
                    TO '{BRONZE_DIR}' (FORMAT 'PARQUET', PARTITION_BY (user_name), OVERWRITE_OR_IGNORE 1)
                """)
                
                # Mark as processed
                con.execute("INSERT INTO processed_files VALUES (?, ?)", [file_hash, file_name])
                logger.info(f"✓ Successfully processed: {file_name}")
                
            except FileNotFoundError:
                logger.error(f"File disappeared during processing: {file_name}")
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {e}", exc_info=True)

class DataFolderHandler(FileSystemEventHandler):
    """Handle file system events with deduplication."""
    
    def __init__(self):
        super().__init__()
        self.processing = set()
    
    def on_created(self, event):
        """Handle file creation events."""
        if not event.is_directory:
            if event.src_path in self.processing:
                return
            
            self.processing.add(event.src_path)
            logger.info(f"Detected new file: {event.src_path}")
            
            try:
                process_file(event.src_path)
            finally:
                self.processing.discard(event.src_path)
    
    def on_modified(self, event):
        """Handle file modification events (usually can ignore)."""
        if not event.is_directory:
            logger.debug(f"File modified (ignoring): {event.src_path}")

def run_ingest():
    """Main ingestion loop."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(INPUT_DIR, exist_ok=True)
    
    logger.info(f"Starting ingestion watcher...")
    logger.info(f"  DB Path:     {DB_PATH}")
    logger.info(f"  Bronze Dir:  {BRONZE_DIR}")
    logger.info(f"  Input Dir:   {INPUT_DIR}")
    
    # Initial scan of existing files
    logger.info(f"Performing initial scan of {INPUT_DIR}...")
    file_count = 0
    if os.path.exists(INPUT_DIR):
        for f in os.listdir(INPUT_DIR):
            full_path = os.path.join(INPUT_DIR, f)
            if os.path.isfile(full_path):
                process_file(full_path)
                file_count += 1
    
    logger.info(f"Initial scan complete. Processed {file_count} existing file(s).")
    logger.info("Waiting for new files (Ctrl+C to stop)...")
    
    # Start watching for new files
    event_handler = DataFolderHandler()
    observer = Observer()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down file watcher...")
        observer.stop()
    
    observer.join()
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    run_ingest()