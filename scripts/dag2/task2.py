from sqlalchemy import create_engine, Column, Integer, Float, Date
from sqlalchemy.orm import declarative_base, Session
import requests
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date", dest="date")
parser.add_argument("--host", dest="host")
parser.add_argument("--dbname", dest="dbname")
parser.add_argument("--user", dest="user")
parser.add_argument("--jdbc_password", dest="jdbc_password")
parser.add_argument("--port", dest="port")
args = parser.parse_args()

v_host = str(args.host)
v_dbname = str(args.dbname)
v_user = str(args.user)
v_password = str(args.jdbc_password)
v_port = str(args.port)


name_parameters = [['Дата', 'Курс доллара', "Курс евро"]]
engine = create_engine(f"postgresql://{v_user}:{v_password}@{v_host}:{v_port}/{v_dbname}", echo=True)
base = declarative_base()

my_date = datetime.date(2023, 11, 1)
class UsdEur(base):
    __tablename__ = 'Курс_доллара_евро_за_период'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    date = Column(Date)
    rate_usd = Column(Float)
    rate_eur = Column(Float)


while my_date.month == 11:
    date_str = my_date.strftime('%Y/%m/%d')
    my_date += datetime.timedelta(days=1)
    url =f'https://www.cbr-xml-daily.ru/archive/{date_str}/daily_json.js'
    r = requests.get(url)
    try:
        name_parameters.append([date_str, r.json()['Valute']['USD']['Value'], r.json()['Valute']['EUR']['Value']])
    except:
        name_parameters.append([date_str, name_parameters[-1][1], name_parameters[-1][2]])

with Session(bind=engine) as session:
    with session.begin():
        base.metadata.create_all(bind=engine)
        for i in range(1, len(name_parameters)):
            row_db = UsdEur(date=name_parameters[i][0], rate_usd=name_parameters[i][1], rate_eur=name_parameters[i][2])
            session.add(row_db)