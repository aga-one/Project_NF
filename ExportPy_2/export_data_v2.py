import csv
from datetime import datetime
import base64
from os import path
from time import time
from models import my_schema
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, Date, String, CHAR
from config import USERNAME, USERPASS

datadirectory = path.join(path.dirname(path.realpath(__file__)),"data")

if __name__ == "__main__":
    t = time()

    engine = create_engine(f'postgresql+psycopg2://{USERNAME}:{base64.b64decode(USERPASS).decode('utf-8')}@localhost/dwh', echo=True)
    metadata = MetaData()

    # currency = Table('dict_currency', metadata,
    #     Column('currency_cd', CHAR(3), primary_key=True),
    #     Column('currency_name', CHAR(3)),
    #     Column('effective_from_date', Date, nullable=True),
    #     Column('effective_to_date', Date, nullable=True),
    #     schema=my_schema,
    # )

    # metadata.drop_all(engine)
    # metadata.create_all(engine)    

    metadata.reflect(engine, only=['dict_currency'], schema=my_schema)
    currency = Table('dict_currency', metadata, autoload=True, autoload_with=engine, schema=my_schema)
    insert_query = currency.insert()
    with open(path.join(datadirectory, 'dict_currency\\dict_currency.csv'), 'r', encoding='windows-1251') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        next(csv_reader, None)  # skip the headers
        with engine.connect() as connection:
            connection.execute(
                insert_query,
                [{'currency_cd': row[0], 
                'currency_name': row[1],
                'effective_from_date' : datetime.strptime(row[2], '%Y-%m-%d').date(),
                'effective_to_date' : datetime.strptime(row[3], '%Y-%m-%d').date()}
                    for row in csv_reader])
            connection.commit() 
    print("Time elapsed: " + str(time() - t) + " s.")