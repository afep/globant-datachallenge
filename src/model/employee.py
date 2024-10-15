#from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String

from service.sql_alchemy.database import Base

class Employee(Base):
    __tablename__ = 'employees'
    __table_args__ = {'schema': 'data_challenge'}
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    datetime = Column(String(255))
    department_id = Column(Integer)
    job_id = Column(Integer)