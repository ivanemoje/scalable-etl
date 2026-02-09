"""
Tests for ingest_job
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os


@pytest.fixture
def mock_env(monkeypatch):
    """Set up mock environment variables"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        bronze_dir = os.path.join(tmpdir, "bronze")
        input_dir = os.path.join(tmpdir, "input")
        
        os.makedirs(bronze_dir, exist_ok=True)
        os.makedirs(input_dir, exist_ok=True)
        
        monkeypatch.setenv("DB_PATH", db_path)
        monkeypatch.setenv("BRONZE_DIR", bronze_dir)
        monkeypatch.setenv("INPUT_DIR", input_dir)
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test123")
        monkeypatch.setenv("S3_INPUT_BUCKET", "test-bucket")
        
        yield {
            "db_path": db_path,
            "bronze_dir": bronze_dir,
            "input_dir": input_dir
        }


def test_get_s3_client():
    """Test S3 client initialization"""
    from src.jobs.ingest_job import get_s3_client
    
    with patch.dict(os.environ, {
        "S3_ENDPOINT": "http://localhost:9000",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test123"
    }):
        client = get_s3_client()
        assert client is not None


@patch('src.jobs.ingest_job.boto3.client')
def test_ensure_s3_bucket_exists_already_exists(mock_boto_client):
    """Test S3 bucket check when bucket exists"""
    from src.jobs.ingest_job import ensure_s3_bucket_exists
    
    mock_s3 = MagicMock()
    mock_s3.head_bucket.return_value = True
    
    ensure_s3_bucket_exists(mock_s3)
    mock_s3.head_bucket.assert_called_once()
    mock_s3.create_bucket.assert_not_called()


@patch('src.jobs.ingest_job.boto3.client')
def test_ensure_s3_bucket_exists_creates_bucket(mock_boto_client):
    """Test S3 bucket creation when bucket doesn't exist"""
    from src.jobs.ingest_job import ensure_s3_bucket_exists
    from botocore.exceptions import ClientError
    
    mock_s3 = MagicMock()
    mock_s3.head_bucket.side_effect = ClientError(
        {'Error': {'Code': '404'}}, 'HeadBucket'
    )
    
    ensure_s3_bucket_exists(mock_s3)
    mock_s3.create_bucket.assert_called_once()


@patch('src.jobs.ingest_job.duckdb.connect')
def test_process_file_skips_processed(mock_connect, mock_env):
    """Test that already processed files are skipped"""
    from src.jobs.ingest_job import process_file
    
    mock_con = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_con
    mock_con.execute.return_value.fetchone.return_value = True  # Already processed
    
    test_file = os.path.join(mock_env["input_dir"], "test.json")
    with open(test_file, 'w') as f:
        f.write('{"test": "data"}')
    
    process_file(test_file)
    
    # Should check if processed but not ingest
    assert mock_con.execute.call_count >= 2  # CREATE TABLE + SELECT


@patch('src.jobs.ingest_job.duckdb.connect')
def test_process_file_ignores_non_json_txt(mock_connect, mock_env):
    """Test that non-JSON/TXT files are ignored"""
    from src.jobs.ingest_job import process_file
    
    mock_con = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_con
    
    test_file = os.path.join(mock_env["input_dir"], "test.csv")
    with open(test_file, 'w') as f:
        f.write('col1,col2\n1,2')
    
    process_file(test_file)
    
    # Should not create any tables or process
    mock_con.execute.assert_not_called()


def test_download_and_process_s3_file():
    """Test S3 file download and processing"""
    from src.jobs.ingest_job import download_and_process_s3_file
    
    mock_s3 = MagicMock()
    mock_s3.download_fileobj.return_value = None
    
    with patch('src.jobs.ingest_job.process_file') as mock_process:
        with patch('tempfile.NamedTemporaryFile') as mock_temp:
            mock_temp.return_value.__enter__.return_value.name = '/tmp/test.json'
            
            download_and_process_s3_file(mock_s3, "test.json")
            
            mock_process.assert_called_once()


@patch('src.jobs.ingest_job.duckdb.connect')
def test_poll_s3_bucket_empty(mock_connect):
    """Test S3 polling with empty bucket"""
    from src.jobs.ingest_job import poll_s3_bucket
    
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {}  # No contents
    
    mock_con = MagicMock()
    
    poll_s3_bucket(mock_s3, mock_con)
    
    mock_s3.list_objects_v2.assert_called_once()


@patch('src.jobs.ingest_job.duckdb.connect')
@patch('src.jobs.ingest_job.download_and_process_s3_file')
def test_poll_s3_bucket_with_files(mock_download, mock_connect):
    """Test S3 polling with files in bucket"""
    from src.jobs.ingest_job import poll_s3_bucket
    
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'file1.json'},
            {'Key': 'file2.json'}
        ]
    }
    
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = None  # Not processed
    
    poll_s3_bucket(mock_s3, mock_con)
    
    assert mock_download.call_count == 2


def test_s3_polling_loop_exits_gracefully():
    """Test that S3 polling loop can be interrupted"""
    from src.jobs.ingest_job import s3_polling_loop
    
    mock_s3 = MagicMock()
    
    with patch('src.jobs.ingest_job.poll_s3_bucket'):
        with patch('time.sleep', side_effect=KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt):
                s3_polling_loop(mock_s3)