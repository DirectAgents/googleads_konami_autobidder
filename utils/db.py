import urllib
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from utils.config import get_database_config


database_config = get_database_config()


def build_database_url_encoded() -> str:
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
