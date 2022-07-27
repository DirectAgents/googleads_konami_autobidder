import os
import urllib
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_database_config() -> dict:
    # config = {'DRIVER': 'ODBC Driver 17 for SQL Server',
    #           'SERVER': 'localhost',
    #           'PORT': '1433',
    #           'DATABASE': 'DA_Data',
    #           'UID': 'google_konami',
    #           'PWD': 'google_konami'}
    config = {'DRIVER': 'ODBC Driver 18 for SQL Server',
              'SERVER': 'daazure1.database.windows.net',
              'PORT': '1433',
              'DATABASE': 'DA_Data',
              'UID': os.environ['DATABASE_UID'],
              'PWD': os.environ['DATABASE_PWD']}
    return config


def build_database_url_encoded() -> str:
    database_config = get_database_config()
    return urllib.parse.quote_plus(
        f"DRIVER={{{database_config['DRIVER']}}};"
        f"SERVER={database_config['SERVER']},{database_config['PORT']};"
        f"DATABASE={database_config['DATABASE']};"
        f"UID={database_config['UID']};"
        f"PWD={database_config['PWD']}"
    )


def get_database_engine() -> Engine:
    database_url = build_database_url_encoded()
    return create_engine(f"mssql+pyodbc:///?odbc_connect={database_url}")
