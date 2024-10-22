import pandas as pd
from unittest.mock import MagicMock
from model.deparment import Department
from dao.departments_db_creator import Departments_Db_Creator  

def test_factory_orm_insert_data_with_headers():
    """Test inserting data with headers."""

    # Mock the database connection and its methods
    mock_conn = MagicMock()
    mock_conn.add = MagicMock()
    mock_conn.commit = MagicMock()

    # Create a sample DataFrame with headers
    df_data = pd.DataFrame([
        {'id': 1, 'department': 'HR'},
        {'id': 2, 'department': 'Finance'}
    ])

    # Initialize the Departments_Db_Creator with the mocked connection
    creator = Departments_Db_Creator(mock_conn)

    # Call the method to test
    creator.factory_orm_insert_data(df_data, headers=True)

    # Assertions to ensure methods were called correctly
    assert mock_conn.add.call_count == 2  # Called twice, once per row
    assert mock_conn.commit.called  # Ensure commit was called

    # Verify that the data passed to `add` was correctly formed
    first_call_args = mock_conn.add.call_args_list[0][0][0]
    assert first_call_args.id == 1
    assert first_call_args.department == 'HR'


def test_get_all_data():
    """Test retrieving all department data."""

    # Create mock data to be returned by the query
    mock_departments = [
        Department(id=1, department='HR'),
        Department(id=2, department='Finance')
    ]

    # Mock the connection's query method
    mock_conn = MagicMock()
    mock_conn.query().all.return_value = mock_departments

    # Initialize the Departments_Db_Creator with the mocked connection
    creator = Departments_Db_Creator(mock_conn)

    # Call the method to test
    result = creator.get_all_data()

    # Assertions to verify the returned data is correct
    assert len(result) == 2
    assert result[0].id == 1
    assert result[0].department == 'HR'
    assert result[1].id == 2
    assert result[1].department == 'Finance'
