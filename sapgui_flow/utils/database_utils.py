
from io import StringIO
from urllib.parse import quote_plus
import json
from sapgui_flow.secrets.get_token import get_secret
from sqlalchemy import create_engine, event, text
import slack

def slackAlerta(msg):
    token = (get_secret("prod/Slack"))
    client = slack.WebClient(token=token)
    client.chat_postMessage(channel='alertas_engenharia',text=msg)

def sqlConnector():
    server = json.loads(get_secret("prod/BancoSQLServer"))['host']
    database = json.loads(get_secret("prod/BancoSQLServer"))['dbname']
    username = json.loads(get_secret("prod/BancoSQLServer"))['username']
    password = json.loads(get_secret("prod/BancoSQLServer"))['password']    
    conn = ('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)

    try:
        engine = create_engine(new_con, fast_executemany=True, use_insertmanyvalues=False)
        connection = engine.connect()
    except Exception as e:
        msg = f'Error while connecting to SQL Server'

        raise ConnectionError(msg)
    return connection
