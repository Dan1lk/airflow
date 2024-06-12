import requests
from sqlalchemy.orm import Session, declarative_base
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, Date, String
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


cities_list = ['Казань', 'Москва', 'Санкт-Петербург', 'Чебоксары', 'Сочи', 'Владивосток', 'Екатеринбург', 'Пекин']
weather_list = [['Дата', 'Город', 'Температура', 'Скорость ветра']]
engine = create_engine(f"postgresql://{v_user}:{v_password}@{v_host}:{v_port}/{v_dbname}", echo=True)
base = declarative_base()
API_ID = 'f22418aba00b0d322fba42dc5c54e76a'


class WeatherCity:
    def get_param(self):
        for city in cities_list:
            r = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&appid={API_ID}")
            temp = r.json()['main']['temp']
            wind = r.json()['wind']['speed']
            gorod = r.json()['name']
            weather_list.append([datetime.now(), gorod, temp, wind])
        return weather_list

a = WeatherCity()
a.get_param()

class Weather_In_City(base):
    __tablename__ = 'weather_in_city'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    date = Column(Date)
    city = Column(String)
    temperature = Column(Float)
    wind_speed = Column(Float)

with Session(bind=engine) as session:
    with session.begin():
        base.metadata.create_all(bind=engine)
        for i in range(1, len(weather_list)):
                weather_str = Weather_In_City(date=weather_list[i][0], city=weather_list[i][1], temperature=weather_list[i][2], wind_speed=weather_list[i][3])
                session.add(weather_str)
