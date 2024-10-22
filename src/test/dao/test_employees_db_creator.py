import pandas as pd
from unittest.mock import MagicMock
from model.employee import Employee
from dao.employees_db_creator import Employees_Db_Creator

def test_factory_orm_insert_data_with_headers():
    """Test inserting employee data with headers."""

    # Mock the database connection and its methods
    mock_conn = MagicMock()
    mock_conn.add = MagicMock()
    mock_conn.commit = MagicMock()

    # Create a sample DataFrame with headers
    df_data = pd.DataFrame([
        {
            'id': 1, 'name': 'Alice', 'datetime': '2024-10-21',
            'department_id': 101, 'job_id': 201
        },
        {
            'id': 2, 'name': 'Bob', 'datetime': '2024-10-22',
            'department_id': 102, 'job_id': 202
        }
    ])

    # Initialize the Employees_Db_Creator with the mocked connection
    creator = Employees_Db_Creator(mock_conn)

    # Call the method to test
    creator.factory_orm_insert_data(df_data, headers=True)

    # Verify the methods were called correctly
    assert mock_conn.add.call_count == 2  # One call per row
    assert mock_conn.commit.called  # Ensure commit is called

    # Check the first call arguments
    first_call_args = mock_conn.add.call_args_list[0][0][0]
    assert first_call_args.id == 1
    assert first_call_args.name == 'Alice'
    assert first_call_args.department_id == 101
    assert first_call_args.job_id == 201


def test_factory_orm_insert_data_without_headers():
    """Test inserting employee data without headers."""

    # Mock the database connection and its methods
    mock_conn = MagicMock()
    mock_conn.add = MagicMock()
    mock_conn.commit = MagicMock()

    # Create a sample DataFrame without headers
    df_data = pd.DataFrame([
        {'column1': 3, 'column2': 'Charlie', 'column3': '2024-10-23', 'column4': 103, 'column5': 203},
        {'column1': 4, 'column2': 'Dave', 'column3': '2024-10-24', 'column4': 104, 'column5': 204}
    ])

    # Initialize the Employees_Db_Creator with the mocked connection
    creator = Employees_Db_Creator(mock_conn)

    # Call the method to test
    creator.factory_orm_insert_data(df_data, headers=False)

    # Verify the methods were called correctly
    assert mock_conn.add.call_count == 2  # One call per row
    assert mock_conn.commit.called  # Ensure commit is called

    # Check the first call arguments
    first_call_args = mock_conn.add.call_args_list[0][0][0]
    assert first_call_args.id == 3
    assert first_call_args.name == 'Charlie'
    assert first_call_args.department_id == 103
    assert first_call_args.job_id == 203


def test_get_all_data():
    """Test retrieving all employee data."""

    # Create mock employee data to return from query
    mock_employees = [
        Employee(id=1, name='Alice', datetime='2024-10-21', department_id=101, job_id=201),
        Employee(id=2, name='Bob', datetime='2024-10-22', department_id=102, job_id=202)
    ]

    # Mock the connection's query method
    mock_conn = MagicMock()
    mock_conn.query().all.return_value = mock_employees

    # Initialize the Employees_Db_Creator with the mocked connection
    creator = Employees_Db_Creator(mock_conn)

    # Call the method to test
    result = creator.get_all_data()

    # Verify the results
    assert len(result) == 2
    assert result[0].name == 'Alice'
    assert result[1].name == 'Bob'
    assert result[0].department_id == 101
    assert result[1].job_id == 202
