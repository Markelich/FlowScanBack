from io import text_encoding
from statistics import pstdev

import pandas as pd
import clickhouse_connect
from sqlalchemy import create_engine, text
import time

host = '80.73.69.138'
port = 5432
db ='traffic'
user = 'default'
password = 'default'


engine = create_engine(
    url = f"clickhouse://{user}:{password}@{host}:{port}/{db}",
    # url = "postgresql+psycopg2://default:default@80.73.69.138:5432/traffic",
    echo = False,
    # pool_size=5,
    # max_overflow = 10,
)

client = clickhouse_connect.get_client(host=host, port=5432, username=user, password=password, database=db)
query = "SELECT DstIP, SUM(Pkts) AS Pkts, SUM(Octets) AS OCTETS FROM `INPUT` WHERE DstIP >= '10.1.0.0' and DstIP <= '10.2.0.0' OR DstIP >= '192.168.0.0' and DstIP <= '192.169.0.0' GROUP BY DstIP"
# query = "SELECT Start, Octets from INPUT"

def clickhouse_con():
    start = time.time()
    query_result = client.query(query)
    set = (query_result.result_set)
    df = pd.DataFrame(set)
    end = time.time()
    print("clickhouse_connect:", end - start)

# def sqlalch():
#     start = time.time()
#     with engine.connect() as conn:
#         res = conn.execute(text(query))
#         end = time.time()
#     df = pd.DataFrame(res)
#     print("sqlalchemy:", end - start)

# def pandas_sql():
#     start = time.time()
#     df = pd.read_sql(query, engine)
#     end = time.time()
#     print("pandas:", end - start)


clickhouse_con()
sqlalch()
# pandas_sql()