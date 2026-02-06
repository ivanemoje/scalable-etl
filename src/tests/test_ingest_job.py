import os
import json
import pytest
import duckdb
from unittest.mock import patch
from src.jobs.ingest_job import process_file, get_file_hash  # Changed from ..jobs

@pytest.fixture
def ingest_setup(tmp_path):
    """Creates a temporary environment for local testing."""
    db_path = str(tmp_path / "test_scalable.db")
    bronze_dir = str(tmp_path / "bronze")
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    
    with patch("src.jobs.ingest_job.DB_PATH", db_path), \
         patch("src.jobs.ingest_job.BRONZE_DIR", bronze_dir), \
         patch("src.jobs.ingest_job.INPUT_DIR", str(input_dir)):
        yield {
            "db": db_path,
            "bronze": bronze_dir,
            "inputs": input_dir
        }

def test_process_file_ingestion(ingest_setup):
    input_file = ingest_setup["inputs"] / "test_data.txt"
    data = {
        "user_name": "IvanEmoje",
        "listened_at": 1707110400,
        "recording_msid": "rec_123",
        "track_metadata": {"track_name": "Testing", "artist_name": "AI"}
    }
    with open(input_file, "w") as f:
        f.write(json.dumps(data) + "\n")

    process_file(str(input_file))

    with duckdb.connect(ingest_setup["db"]) as con:
        hashes = con.execute("SELECT filename FROM processed_files").fetchall()
        assert len(hashes) == 1
        assert hashes[0][0] == "test_data.txt"

    partition_folder = os.path.join(ingest_setup["bronze"], "user_name=IvanEmoje")
    assert os.path.exists(partition_folder)
    assert any(f.endswith(".parquet") for f in os.listdir(partition_folder))

def test_deduplication_logic(ingest_setup):
    input_file = ingest_setup["inputs"] / "duplicate.txt"
    content = {"user_name": "user1", "listened_at": 1, "track_metadata": {}}
    
    with open(input_file, "w") as f:
        f.write(json.dumps(content))

    process_file(str(input_file))
    process_file(str(input_file))

    with duckdb.connect(ingest_setup["db"]) as con:
        count = con.execute("SELECT COUNT(*) FROM processed_files").fetchone()[0]
        assert count == 1

def test_get_file_hash_consistency(tmp_path):
    f = tmp_path / "hash_test.txt"
    f.write_text("consistent content")
    
    hash1 = get_file_hash(str(f))
    hash2 = get_file_hash(str(f))
    
    assert hash1 == hash2
    assert len(hash1) == 64