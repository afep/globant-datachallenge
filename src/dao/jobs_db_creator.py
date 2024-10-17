from dao.creator import Creator
from model.job import Job
class Jobs_Db_Creator(Creator):

    def __init__(self, conn):
        print("Initialize the new instance for jobs")
        self.conn = conn

    def factory_orm_insert_data(self, df_data, headers=False):
        # Add data file to the database
        for _, row in df_data.iterrows():
            if headers:
                new_job = Job(id=row['id'], job=row['job'])
            else:
                new_job = Job(id=row['column1'], job=row['column2'])
            self.conn.add(new_job)
        self.conn.commit()

    def get_all_data(self):
        jobs_data = self.conn.query(Job).all()
        return jobs_data