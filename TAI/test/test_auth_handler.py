import pytest
from GTI.auth_handler import RedshiftAuth, MySQLAuth, DynamoDBAuth, AWSAuth, BrokerAuth, OpenAIAuth, IBAuth, AlpacaAuth, BLSAuth

def test_redshift_auth():
    auth = RedshiftAuth()
    conn = auth.get_connection()
    assert conn is not None

def test_mysql_auth():
    auth = MySQLAuth()
    conn = auth.get_connection()
    assert conn is not None

def test_dynamodb_auth():
    auth = DynamoDBAuth()
    client = auth.get_client()
    assert client is not None

def test_aws_auth():
    auth = AWSAuth()
    session = auth.get_session()
    assert session is not None

def test_broker_auth():
    auth = BrokerAuth()
    headers = auth.get_headers()
    assert headers is not None

def test_openai_auth():
    auth = OpenAIAuth()
    api_key = auth.get_api_key()
    assert api_key is not None

def test_ib_auth():
    auth = IBAuth()
    ib = auth.connect()
    assert ib is not None

def test_alpaca_auth():
    auth = AlpacaAuth()
    trading_client = auth.get_trading_client()
    assert trading_client is not None

def test_bls_auth():
    auth = BLSAuth()
    api_key = auth.get_api_key()
    assert api_key is not None
