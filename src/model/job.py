from sqlalchemy import Column, Integer, String

from service.sql_alchemy.database import Base

class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = {'schema': 'data_challenge'}
    id = Column(Integer, primary_key=True)
    job = Column(String(255))