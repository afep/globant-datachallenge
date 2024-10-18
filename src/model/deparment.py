#from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String

from service.sqlalchemy.database import Base

class Department(Base):
    __tablename__ = 'departments'
    __table_args__ = {'schema': 'data_challenge'}
    id = Column(Integer, primary_key=True)
    department = Column(String(255))