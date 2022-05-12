import yaml
from pathlib import Path


CONFIGURATION_FILE_NAME = 'configuration.yaml'
CONFIGURATION_FILE_PATH = Path(CONFIGURATION_FILE_NAME).resolve()


def get_config() -> dict:
    with open(CONFIGURATION_FILE_PATH, "r") as f:
        return yaml.safe_load(f)


def get_database_config() -> dict:
    config = get_config()
    return config['DATABASE']


def get_inputs_config() -> dict:
    config = get_config()
    return config['INPUTS']


def get_google_ads_credentials_file_path() -> Path:
    inputs_config = get_inputs_config()
    credentials_path = Path(inputs_config['GOOGLE_ADS_CREDENTIAL_FILE_NAME'])
    return credentials_path.resolve()
