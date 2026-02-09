"""
Tests for transform_job
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession


@pytest.fixture
def mock_spark():
    """Mock Spark session"""
    with patch('src.jobs.transform_job.SparkSession') as mock:
        session = MagicMock()
        mock.builder.appName.return_value.config.return_value.getOrCreate.return_value = session
        yield session


def test_run_spark_transform_creates_namespaces(mock_spark):
    """Test that transform job creates necessary namespaces"""
    from src.jobs.transform_job import run_spark_transform
    
    mock_spark.sql.return_value = None
    mock_spark.read.parquet.return_value = MagicMock()
    
    with patch('os.path.exists', return_value=True):
        with patch('subprocess.run'):
            with pytest.raises(AttributeError):  # Expected since we're mocking
                run_spark_transform()
    
    # Check that namespace creation was attempted
    calls = [str(call) for call in mock_spark.sql.call_args_list]
    assert any('CREATE NAMESPACE' in str(call) for call in calls)


def test_run_spark_transform_checks_bronze_path():
    """Test that transform job checks if bronze path exists"""
    from src.jobs.transform_job import run_spark_transform
    
    with patch('src.jobs.transform_job.SparkSession'):
        with patch('os.path.exists', return_value=False):
            with pytest.raises(SystemExit) as exc_info:
                run_spark_transform()
            
            assert exc_info.value.code == 1


def test_run_spark_transform_cleans_empty_parquet():
    """Test that empty parquet files are cleaned"""
    from src.jobs.transform_job import run_spark_transform
    
    with patch('src.jobs.transform_job.SparkSession'):
        with patch('os.path.exists', return_value=True):
            with patch('subprocess.run') as mock_run:
                with pytest.raises(AttributeError):  # Expected
                    run_spark_transform()
                
                # Check that cleanup command was called
                mock_run.assert_called()
                args = mock_run.call_args[0][0]
                assert 'find' in args
                assert '-size 0' in args


def test_run_spark_transform_stops_spark_on_error():
    """Test that Spark session is stopped on error"""
    from src.jobs.transform_job import run_spark_transform
    
    mock_session = MagicMock()
    
    with patch('src.jobs.transform_job.SparkSession.builder') as mock_builder:
        mock_builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_session
        
        with patch('os.path.exists', return_value=True):
            with patch('subprocess.run'):
                mock_session.read.parquet.side_effect = Exception("Test error")
                
                with pytest.raises(SystemExit):
                    run_spark_transform()
                
                mock_session.stop.assert_called_once()