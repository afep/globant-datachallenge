from dao.creator import Creator
from model.deparment import Department
class Departments_Db_Creator(Creator):

    def __init__(self, conn):
        print("Initialize the new instance for departments")
        self.conn = conn

    def factory_orm_insert_data(self, df_data, headers=False):
        # Add data file to the database
        for _, row in df_data.iterrows():
            if headers:
                new_department = Department(id=row['id'], department=row['department'])
            else:
                new_department = Department(id=row['column1'], department=row['column2'])
            self.conn.add(new_department)
        self.conn.commit()
