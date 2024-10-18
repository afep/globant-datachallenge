from dao.creator import Creator
from model.employee import Employee
class Employees_Db_Creator(Creator):

    def __init__(self, conn):
        print("Initialize the new instance for employees")
        self.conn = conn

    def factory_orm_insert_data(self, df_data, headers=False):
        # Add data file to the database
        for _, row in df_data.iterrows():
            if headers:
                new_employee = Employee(id=row['id'], name=row['name'], datetime=row['datetime'],
                                   department_id=row['department_id'], job_id=row['job_id'])
            else:
                new_employee = Employee(id=row['column1'], name=row['column2'], datetime=row['column3'],
                                   department_id=row['column4'], job_id=row['column5'])
            self.conn.add(new_employee)
        self.conn.commit()
    
    def get_all_data(self):
        employees_data = self.conn.query(Employee).all()
        return employees_data
