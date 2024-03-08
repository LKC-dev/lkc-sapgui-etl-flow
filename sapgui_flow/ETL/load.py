import datetime
import slack
from sqlalchemy import create_engine, text 
from sapgui_flow.secrets.get_token import get_secret
from sapgui_flow.utils.database_utils import sqlConnector
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("my_logger")

# def slackAlerta(msg):
#     token = (get_secret("prod/Slack"))
#     client = slack.WebClient(token=token)
#     client.chat_postMessage(channel='alertas_engenharia',text=msg)

def load_data(df, table_name, schema, table_id_column):
    conn = sqlConnector()
    inserted_at = 'inserted_at'
    etl = f'lkc-sapgui-flow-{table_name}'
    time_now = datetime.datetime.now()
    df_data = df
    df_data.reset_index(drop=True)
    df_data[f'{inserted_at}'] = time_now
    try:
        logger.info(f'{table_name} - Started loading to SQL SERVER')
        df_data.to_sql(name=table_name, index=False, con=conn, schema=schema, if_exists='append', method='multi', chunksize=((2100//len(df_data.columns)-1)))
        logger.info(f'{table_name} - Inserted into')
        try:
            conn = sqlConnector().connect()
            logger.info(f'{table_name} - Started deduplicating')
            query = text(f"""
                        with cte as (select {table_id_column}, max(inserted_at) max_data
                        from {schema}.{table_name}
                        group by {table_id_column})
                        delete t
                        from {schema}.{table_name} t
                        inner join cte
                        on  (t.{table_id_column} = cte.{table_id_column})
                        and (t.inserted_at <> cte.max_data)
                        """)
            conn.execute(query)
            conn.commit()
            logger.info(f'{table_name} - Deduplicated')
        except Exception as e:
            logger.info(e)
            logger.error(f'{table_name} - Error deduplicating')
            slackAlerta(f"""
        URGENTE VERIFICAR:
        ERRO: Erro ao deduplicar - {schema}.{table_name}
        ETL: {etl}
        {time_now}
        """)
        conn.close()    
    except Exception as e:
        logger.info(e)
        logger.error(f"{table_name} - Error inserting SQL Server")
        slackAlerta(f"""
        URGENTE VERIFICAR:
        ERRO: Erro ao inserir SQL Server - {schema}.{table_name}
        ETL: {etl}
        {time_now}
        """)
    conn.close()
