import psycopg2
from sqlalchemy import create_engine
from config import Config
import pandas as pd
from datetime import datetime


class Database_Operation:

    def __get_string(self):
        config = Config.get_config()['database']
        connection_string = f"mysql+mysqlconnector://{config['username']}:{config['password']}@{config['server']}/{config['catalogo']}"
        return connection_string

    def obtener_vista(self,vista):
        engine = create_engine(self.__get_string())
        dataframe =pd.read_sql_table(vista, con=engine)
        return dataframe
    
    def cargar_reporte(self,reporte,vista):
        engine = create_engine(self.__get_string())
        reporte.to_sql(f'{vista.replace('vw_','stg_')}', engine, schema='staging', if_exists='replace', index=False)
    
        
    def ejecutar_carga(self,procedimiento):
        _fecha = str(datetime.now())
        conexion = psycopg2.connect(self.__get_string())
        cursor = conexion.cursor()
        try:
            cursor.execute(f"CALL dwh.{procedimiento}()")
            conexion.commit()  # Importante: confirmar los cambios
            print(f"\t |> Tabla {procedimiento.replace('sp_cargar_','')} CARGADA en sakila_dwh CON EXITO | {_fecha}")
        except Exception as e:
            print(F"\t |> Tabla {procedimiento.replace('sp_cargar_','')} NO FUE CARGADA en sakila_dwh | {_fecha}", e)
            conexion.rollback()
        finally:
            cursor.close()
            conexion.close()