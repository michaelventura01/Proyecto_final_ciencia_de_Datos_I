import psycopg2
from sqlalchemy import create_engine
from config import Config
import pandas as pd

class Database_Operation:

    def __get_string(self):
        config = Config.get_config()['database']
        connection_string = f"postgresql://{config['username']}:{config['password']}@{config['server']}/{config['catalogo']}"
        return connection_string

    def obtener_reporte_sakila(self,reporte):
        engine = create_engine(self.__get_string())
        dataframe =pd.read_sql(f'select * from dwh.{reporte}', con=engine)
        return dataframe
    
    def obtener_reporte(self,reporte):
        engine = create_engine(self.__get_string())
        dataframe =pd.read_sql(f'select * from dwh.{reporte}', con=engine)
        return dataframe
    
    def ver_reportes(self):
        _config = Config.get_config()['reporte']
        return  _config.items()
    

