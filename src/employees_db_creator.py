from creator import Creator
class Employees_Db_Creator(Creator):

    def __init__(self, conn):
        print("Initialize the new instance for employees")
        self.conn = conn

    def factory_create_table(self):
        cursor = self.conn.cursor()
        # Create table if it doesn't exists
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS data_challenge.employees (
                id INT,
                name VARCHAR(255),
                datetime VARCHAR(255),
                department_id INT,
                job_id INT
            )
            '''
        cursor.execute(create_table_query)
        self.conn.commit()

        # Close connection
        cursor.close()

    def factory_insert_data(self, df_data):
        cursor = self.conn.cursor()


        # Insert data from dataframe into database
        for _, row in df_data.iterrows():
            id = row['column1']
            name = row['column2']
            datetime = row['column3']
            department_id = row['column4']
            job_id = row['column5']
            insert_query = f"""INSERT INTO data_challenge.employees (id, name, datetime, department_id, job_id) 
                            VALUES (%s, %s, %s, %s, %s)"""
            cursor.execute(insert_query, (id, name, datetime, department_id, job_id))
        self.conn.commit()

        # Close connection
        cursor.close()
