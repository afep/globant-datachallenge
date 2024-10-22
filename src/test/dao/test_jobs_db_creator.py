import pandas as pd
from unittest.mock import MagicMock
from model.job import Job
from dao.jobs_db_creator import Jobs_Db_Creator  

def test_factory_orm_insert_data_with_headers():
    """Test inserting job data with headers."""

    # Mock the database connection and methods
    mock_conn = MagicMock()
    mock_conn.add = MagicMock()
    mock_conn.commit = MagicMock()

    # Create a sample DataFrame with headers
    df_data = pd.DataFrame([
        {'id': 1, 'job': 'Software Engineer'},
        {'id': 2, 'job': 'Data Scientist'}
    ])

    # Initialize the Jobs_Db_Creator with the mocked connection
    creator = Jobs_Db_Creator(mock_conn)

    # Call the method to test
    creator.factory_orm_insert_data(df_data, headers=True)

    # Verify the methods were called correctly
    assert mock_conn.add.call_count == 2  # One call per row
    assert mock_conn.commit.called  # Commit should be called

    # Check the arguments passed to the first call of `add`
    first_call_args = mock_conn.add.call_args_list[0][0][0]
    assert first_call_args.id == 1
    assert first_call_args.job == 'Software Engineer'


def test_factory_orm_insert_data_without_headers():
    """Test inserting job data without headers."""

    # Mock the database connection and methods
    mock_conn = MagicMock()
    mock_conn.add = MagicMock()
    mock_conn.commit = MagicMock()

    # Create a sample DataFrame without headers
    df_data = pd.DataFrame([
        {'column1': 3, 'column2': 'Product Manager'},
        {'column1': 4, 'column2': 'QA Engineer'}
    ])

    # Initialize the Jobs_Db_Creator with the mocked connection
    creator = Jobs_Db_Creator(mock_conn)

    # Call the method to test
    creator.factory_orm_insert_data(df_data, headers=False)

    # Verify the methods were called correctly
    assert mock_conn.add.call_count == 2
    assert mock_conn.commit.called

    # Check the arguments passed to the first call of `add`
    first_call_args = mock_conn.add.call_args_list[0][0][0]
    assert first_call_args.id == 3
    assert first_call_args.job == 'Product Manager'


def test_get_all_data():
    """Test retrieving all job data."""

    # Create mock job data to be returned by the query
    mock_jobs = [
        Job(id=1, job='Software Engineer'),
        Job(id=2, job='Data Scientist')
    ]

    # Mock the connection's query method
    mock_conn = MagicMock()
    mock_conn.query().all.return_value = mock_jobs

    # Initialize the Jobs_Db_Creator with the mocked connection
    creator = Jobs_Db_Creator(mock_conn)

    # Call the method to test
    result = creator.get_all_data()

    # Verify the results
    assert len(result) == 2
    assert result[0].id == 1
    assert result[0].job == 'Software Engineer'
    assert result[1].id == 2
    assert result[1].job == 'Data Scientist'
