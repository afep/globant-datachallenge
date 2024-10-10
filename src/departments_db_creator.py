from creator import Creator
class Departments_Db_Creator(Creator):

    def __init__(self, conn):
        print("Initialize the new instance for departments")
        self.conn = conn

    def factory_create_table(self):
        cursor = self.conn.cursor()
        # Create table if it doesn't exists
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS data_challenge.departments (
                id INT,
                department VARCHAR(255)
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
            insert_query = f"INSERT INTO data_challenge.departments (id, department) VALUES ('{row['column1']}', '{row['column2']}')"
            cursor.execute(insert_query)
        self.conn.commit()

        # Close connection
        cursor.close()
