from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
import requests
import pandas
from sqlalchemy.orm import Session, declarative_base
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, Date, String, TIMESTAMP
Base = declarative_base()
cities_list = ['Казань', 'Москва', 'Санкт-Петербург', 'Чебоксары', 'Сочи', 'Владивосток', 'Екатеринбург', 'Пекин']
weather_list = [['Дата', 'Город', 'Температура', 'Скорость ветра']]
API_ID = 'f22418aba00b0d322fba42dc5c54e76a'

class ForAirflowPlagin(Base):
    __tablename__ = 'for_airflow_plagin'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    text_field = Column(String(100))

class ExampleOperator(BaseOperator):
    def __init__(self,
                 postgre_conn: Connection,
                 text_field: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.text_field = text_field
        self.postgre_conn = postgre_conn
        self.SQL_ALCHEMY_DATABASE_URL = f"postgresql://{postgre_conn.login}:{postgre_conn.password}@{postgre_conn.host}:{postgre_conn.port}/{postgre_conn.schema}"

    def execute(self, context):
        engine = create_engine(self.SQL_ALCHEMY_DATABASE_URL)

        with Session(bind=engine) as session:
            with session.begin():
                Base.metadata.create_all(bind=engine)
                row = ForAirflowPlagin(text_field=self.text_field)
                session.add(row)