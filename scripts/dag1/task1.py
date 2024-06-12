import requests
import pandas
from sqlalchemy.orm import Session, declarative_base
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, Date, String



cities_list = ['Казань', 'Москва', 'Санкт-Петербург', 'Чебоксары', 'Сочи', 'Владивосток', 'Екатеринбург', 'Пекин']
weather_list = [['Дата', 'Город', 'Температура', 'Скорость ветра']]
engine = create_engine(f"postgresql://orders_user:IsoGleborg921@192.168.56.2:5432/orders", echo=True)
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
