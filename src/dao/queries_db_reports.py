from model.employee import Employee
from model.job import Job
from model.deparment import Department
from sqlalchemy import func, extract, DateTime
class Queries_Db_Reports():

    def __init__(self, conn):
        print("Initialize the new instance for miscelaneous queries")
        self.conn = conn

    def get_employees_by_quarter(self, param_year):
        query = (
            self.conn.query(
                Department.department.label('department'),
                Job.job.label('job'),
                func.count().filter(extract('quarter', Employee.datetime.cast(DateTime)) == 1).label('Q1'),
                func.count().filter(extract('quarter', Employee.datetime.cast(DateTime)) == 2).label('Q2'),
                func.count().filter(extract('quarter', Employee.datetime.cast(DateTime)) == 3).label('Q3'),
                func.count().filter(extract('quarter', Employee.datetime.cast(DateTime)) == 4).label('Q4')
            )
            .join(Department, Employee.department_id == Department.id)
            .join(Job, Employee.job_id == Job.id)
            .filter(extract('year', Employee.datetime.cast(DateTime)) == param_year)
            .group_by(Department.department, Job.job)
            .order_by(Department.department, Job.job)
            )
        return query
    
    def get_employee_count_by_department(self, param_year):
        query = (
            self.conn.query(
                Employee.department_id,
                func.count(Employee.id).label('employee_count')
            )
            .filter(extract('year', Employee.datetime.cast(DateTime)) == param_year)  
            .group_by(Employee.department_id)
            .subquery()
        )
        return query

    def get_employees_mean(self, param_year):
        employee_counts_subquery = self.get_employee_count_by_department(param_year)
        query = (
            self.conn.query(
                func.avg(employee_counts_subquery.c.employee_count).label('mean_hired')
            )
        )
        return query
    
    def get_departments_above_mean(self, param_year, mean_hired):
        query = (
            self.conn.query(
                Department.id,
                Department.department,
                func.count(Employee.id).label('hired')
            )
            .join(Employee, Department.id == Employee.department_id)
            .filter(extract('year', Employee.datetime.cast(DateTime)) == param_year)
            .group_by(Department.id, Department.department)
            .having(func.count(Employee.id) > mean_hired)  # Step 3: Filter by mean
            .order_by(func.count(Employee.id).desc())  # Step 4: Order by hired (descending)
        )
        return query