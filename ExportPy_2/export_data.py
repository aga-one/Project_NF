from datetime import datetime
# from numpy import genfromtxt
import pandas as pd
import base64
from os import path
from time import time
# import psycopg2
from models import Base, my_schema, Deal, Product, Сurrency
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker
from config import USERNAME, USERPASS

datadirectory = path.join(path.dirname(path.realpath(__file__)),"data")

# with open(filename, 'r', encoding='windows-1251') as f:    
    # conn = create_engine(f'postgresql+psycopg2://{USERNAME}:{base64.b64decode(USERPASS).decode('utf-8')}@localhost/dwh', echo=True).raw_connection()
        # cursor = conn.cursor()
        # cmd = 'COPY tbl_name(col1, col2, col3) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
        # cursor.copy_expert(cmd, f)
        # conn.commit()

def Load_Data(file_name):
    # data = genfromtxt(file_name, delimiter=',', skip_header=1, dtype='str', encoding='windows-1251')
    data = pd.read_csv(file_name, delimiter=',', encoding='windows-1251')
    # return data.tolist()
    return data.values.tolist()

if __name__ == "__main__":
    t = time()

    engine = create_engine(f'postgresql+psycopg2://{USERNAME}:{base64.b64decode(USERPASS).decode('utf-8')}@localhost/dwh', echo=True)
    with engine.connect() as connection:
        if not inspect(connection).has_schema(my_schema):
            connection.execute(CreateSchema(my_schema))
            connection.commit()

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    try:
        deal_data = Load_Data(path.join(datadirectory, 'loan_holiday_info\\deal_info.csv'))
        for i in deal_data:
            record = Deal(**{
                'deal_rk' : i[0],
                'deal_num' : i[1],
                'deal_name' : i[2],
                'deal_sum' : i[3],
                'client_rk' : i[4],
                'account_rk' : i[5],
                'agreement_rk' : i[6],
                'deal_start_date' : datetime.strptime(i[7], '%Y-%m-%d').date(),
                'department_rk' : i[8],
                'product_rk' : i[9],
                'deal_type_cd' : i[10],
                'effective_from_date' : datetime.strptime(i[11], '%Y-%m-%d').date(),
                'effective_to_date' : datetime.strptime(i[12], '%Y-%m-%d').date()
            })
            s.add(record)

        product_data = Load_Data(path.join(datadirectory, 'loan_holiday_info\\product_info.csv'))
        for i in product_data:
            record = Product(**{
                'product_rk' : i[0],
                'product_name' : i[1],
                'effective_from_date' : datetime.strptime(i[2], '%Y-%m-%d').date(),
                'effective_to_date' : datetime.strptime(i[3], '%Y-%m-%d').date()
            })
            s.add(record)

        currency_data = Load_Data(path.join(datadirectory, 'dict_currency\\dict_currency.csv'))
        for i in currency_data:
            record = Сurrency(**{
                'currency_cd' : i[0],
                'currency_name' : i[1],
                'effective_from_date' : datetime.strptime(i[2], '%Y-%m-%d').date(),
                'effective_to_date' : datetime.strptime(i[3], '%Y-%m-%d').date()
            })
            s.add(record)            

        s.commit()
    except Exception as e:
        print(e)
        s.rollback()
    finally:
        s.close()
    print("Time elapsed: " + str(time() - t) + " s.")