from sqlalchemy import Column, Integer, Float, Date, String, CHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
my_schema = 'csv'

class Deal(Base):
    __tablename__ = 'deal_info'
    __table_args__ = {'schema': my_schema}
    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_rk = Column(Integer, nullable=False)
    deal_num = Column(String(100))
    deal_name = Column(String(100))
    deal_sum = Column(Float)
    client_rk = Column(Integer, nullable=False)
    account_rk = Column(Integer, nullable=False)
    agreement_rk = Column(Integer, nullable=False)
    deal_start_date = Column(Date)
    department_rk = Column(Integer)
    product_rk = Column(Integer)
    deal_type_cd = Column(CHAR(1))
    effective_from_date = Column(Date, nullable=False)
    effective_to_date = Column(Date, nullable=False)

class Product(Base):
    __tablename__ = 'product_info'
    __table_args__ = {'schema': my_schema}
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_rk = Column(Integer, nullable=False)
    product_name = Column(String(100))
    effective_from_date = Column(Date, nullable=False)
    effective_to_date = Column(Date, nullable=False)

class Сurrency(Base):
    __tablename__ = 'dict_currency'
    __table_args__ = {'schema': my_schema}
    currency_cd = Column(CHAR(3), primary_key=True)
    currency_name = Column(CHAR(3))
    effective_from_date = Column(Date, nullable=False)
    effective_to_date = Column(Date, nullable=False)        