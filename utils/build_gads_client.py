import os
from google.ads.googleads.client import GoogleAdsClient


def get_google_ads_client() -> GoogleAdsClient:
    error = False
    use_proto_plus = "False"
    developer_token = None
    refresh_token = None
    client_id = None
    client_secret = None
    login_customer_id = None

    try:
        developer_token = os.environ["GOOGLE_DEVELOPER_TOKEN"]
    except KeyError:
        print("Please set the environment variable: GOOGLE_DEVELOPER_TOKEN")
        error = True

    try:
        refresh_token = os.environ["GOOGLE_REFRESH_TOKEN"]
    except KeyError:
        print("Please set the environment variable: GOOGLE_REFRESH_TOKEN")
        error = True

    try:
        client_id = os.environ["GOOGLE_CLIENT_ID"]
    except KeyError:
        print("Please set the environment variable: GOOGLE_CLIENT_ID")
        error = True

    try:
        client_secret = os.environ["GOOGLE_CLIENT_SECRET"]
    except KeyError:
        print("Please set the environment variable: GOOGLE_CLIENT_SECRET")
        error = True

    try:
        login_customer_id = os.environ["GOOGLE_LOGIN_CUSTOMER_ID"]
    except KeyError:
        print("Please set the environment variable: GOOGLE_LOGIN_CUSTOMER_ID")
        error = True

    if error:
        raise Exception("You must setup all Google Ads Credentials Environment Variables")

    gads_credentials = {
        "use_proto_plus": use_proto_plus,
        "developer_token": developer_token,
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "login_customer_id": login_customer_id
    }
    return GoogleAdsClient.load_from_dict(gads_credentials)
