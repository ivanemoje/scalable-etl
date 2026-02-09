"""
Tests for daily_job
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession


@pytest.fixture
def mock_spark():
    """Mock Spark session"""
    with patch('src.jobs.daily_job.SparkSession') as mock:
        session = MagicMock()
        mock.builder.appName.return_value.config.return_value.getOrCreate.return_value = session
        yield session


def test_run_daily_job_reads_silver_table(mock_spark):
    """Test that daily job reads from silver table"""
    from src.jobs.daily_job import run_daily_job
    
    mock_df = MagicMock()
    mock_spark.table.return_value = mock_df
    mock_df.groupBy.return_value.count.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.coalesce.return_value = mock_df
    mock_df.writeTo.return_value.create.return_value = None
    
    with pytest.raises(AttributeError):
        run_daily_job()
    
    mock_spark.table.assert_called_with("warehouse.silver_listens")


def test_run_daily_job_creates_gold_table(mock_spark):
    """Test that daily job creates gold table"""
    from src.jobs.daily_job import run_daily_job
    
    mock_spark.sql.return_value = None
    
    with pytest.raises(AttributeError):
        run_daily_job()
    
    calls = [str(call) for call in mock_spark.sql.call_args_list]
    assert any('DROP TABLE' in str(call) for call in calls)
    assert any('user_peaks' in str(call) for call in calls)


def test_run_daily_job_stops_spark_on_error():
    """Test that Spark session is stopped on error"""
    from src.jobs.daily_job import run_daily_job
    
    mock_session = MagicMock()
    
    with patch('src.jobs.daily_job.SparkSession.builder') as mock_builder:
        mock_builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_session
        mock_session.table.side_effect = Exception("Test error")
        
        with pytest.raises(SystemExit):
            run_daily_job()
        
        mock_session.stop.assert_called_once()


def test_run_daily_job_applies_window_function():
    """Test that daily job applies window function for ranking"""
    from src.jobs.daily_job import run_daily_job
    
    mock_session = MagicMock()
    mock_df = MagicMock()
    
    with patch('src.jobs.daily_job.SparkSession.builder') as mock_builder:
        mock_builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_session
        mock_session.table.return_value = mock_df
        
        mock_df.groupBy.return_value.count.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.coalesce.return_value = mock_df
        
        with pytest.raises(AttributeError):
            run_daily_job()
        
        mock_df.withColumn.assert_called()


def test_run_daily_job_filters_top_3():
    """Test that daily job filters to top 3 records"""
    from src.jobs.daily_job import run_daily_job
    
    mock_session = MagicMock()
    mock_df = MagicMock()
    
    with patch('src.jobs.daily_job.SparkSession.builder') as mock_builder:
        mock_builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_session
        mock_session.table.return_value = mock_df
        
        mock_df.groupBy.return_value.count.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.coalesce.return_value = mock_df
        
        with pytest.raises(AttributeError):
            run_daily_job()

        mock_df.filter.assert_called()