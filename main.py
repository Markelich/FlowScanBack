from operator import index

from fastapi import FastAPI, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 10)
pd.options.mode.chained_assignment = None


import socket
import clickhouse_connect
from starlette.config import undefined

app = FastAPI(
    title="Netflow Analiser"
)

host = '80.73.69.138'
port = 5432
db ='traffic'
user = 'default'
password = 'default'

client = clickhouse_connect.get_client(host=host, port=5432, username=user, password=password, database=db)

router = APIRouter(
    tags = ["users list"]
)






@router.get("/{hosts}/{startTime}/{finalTime}")
def get_ip_list(startTime: str, finalTime: str):
    query = f"""SELECT DstIP, SUM(Pkts) AS Pkts, SUM(Octets) AS Octets FROM `INPUT`
        WHERE (DstIP BETWEEN '10.1.0.0' and '10.2.0.0' OR DstIP BETWEEN '192.168.0.0' and '192.169.0.0')
        AND `Start` BETWEEN '{startTime}:00' AND '{finalTime}:00' GROUP BY DstIP
        ORDER BY Octets DESC"""
    # print(query)
    query_result = client.query(query)
    set = (query_result.result_set)
    df = pd.DataFrame(set, columns=['IP', 'pkts', 'octets'])
    # print(set)
    df['octetsLen'] = df['octets'].astype(str).str.len()
    df['octetsLen'] = df['octetsLen'].astype(int)

    # df['octets2'] = df['octets'] / (1024 ** (df['octetsLen'] // 3))
    octets = np.array(df['octets'])
    octetsLen = np.array(1024 ** (df['octetsLen'] // 3))

    df['octets2'] = octets / octetsLen
    # df = df[(df['Octets'] != 0) & (df['DstIPaddress'].std.match('10.1') | df['DstIPaddress'].str.match('192.168'))]
    # df['octets'] = df['octets'].apply(lambda x: round(x, 2))
    print(111)
    df = df.reset_index().to_dict('records')
    return df





@router.get("/hosts/{user_addres}/{startTime}/{finalTime}")
async def get_serveses_list(user_addres, startTime: str = '2024-08-17T15:43', finalTime: str = '2024-08-17T18:44'):
    print(user_addres)
    query = f"""SELECT SrcIP, DstIP, SUM(Pkts) AS Pkts, SUM(Octets) AS Octets FROM `INPUT`
            WHERE DstIP = '{user_addres}'
            AND `Start` BETWEEN '{startTime}:00' AND '{finalTime}:00' GROUP BY (DstIP, SrcIP)
            ORDER BY Octets DESC"""
    print(query)
    query_result = client.query(query)
    set = (query_result.result_set)
    df = pd.DataFrame(set, columns=['srcip', 'dstip', 'pkts', 'octets'])
    df['octets'] = df['octets'] / (1024 * 1024)
    df['octets'] = df['octets'].apply(lambda x: round(x, 2))
    # list_of_serveses['domain_name'] = list_of_serveses['srcip'].apply(get_domain_name)
    df = df.reset_index().to_dict('records')
    return df

@router.get("/domain_names/{server_addres}")
async def get_domain_name(server_addres):
    print(server_addres)
    try:
        domain = socket.gethostbyaddr(server_addres)[0]
    except socket.herror:
        domain = 'Unknown'
    return domain

# @router.get("/addresses_by_domain_name/{server_addres}")
# async def get_addresses(server_addres):
#     print(server_addres)
#     try:
#         domain = socket.gethostbyaddr(server_addres)[0]
#     except socket.herror:
#         domain = 'Unknown'
#     return domain


# @router.get("/curentData")
# async def get_last_data():
#     queryMainGrapf = f"""WITH
#                             (SELECT max(`Start`) - INTERVAL 32 MINUTE FROM `INPUT`) AS latest_timestamp
#                         SELECT Start, Final, Octets
#                         FROM `INPUT`
#                         WHERE `Start` >= latest_timestamp - INTERVAL 15 MINUTE AND `Start` <= latest_timestamp
#                         ORDER BY `Start` ;"""
#     # print(queryMainGrapf)
#     query_resultMainGrapf = client.query(queryMainGrapf)
#     setMainGrapf = (query_resultMainGrapf.result_set)
#
#     dfMainGrapf = pd.DataFrame(setMainGrapf, columns=['start_time', 'end_time', 'octets'])
#
#
#     dfMainGrapf['start_time'] = pd.to_datetime(dfMainGrapf['start_time'])
#     dfMainGrapf['end_time'] = pd.to_datetime(dfMainGrapf['end_time'])
#     dfMainGrapf['timediff'] = dfMainGrapf['end_time'] - dfMainGrapf['start_time']
#
#     dfMainGrapf['speed'] = dfMainGrapf['octets'] / dfMainGrapf['timediff'].dt.total_seconds()
#     dfMainGrapf = dfMainGrapf.loc[(dfMainGrapf != np.inf).all(axis=1)]
#     # print(dfMainGrapf)
#
#     time_index = pd.date_range(start=dfMainGrapf['start_time'].min(), end=dfMainGrapf['end_time'].max(), freq='s')
#     #
#     traffic_speed = pd.DataFrame(index=time_index, data={'speed': 0})
#     # print(traffic_speed)
#     #
#     for i in range(len(dfMainGrapf)):
#         traffic_speed.loc[dfMainGrapf['start_time'].iloc[i]:dfMainGrapf['end_time'].iloc[i]] += dfMainGrapf['speed'].iloc[i].astype(int)
#     traffic_speed['speed'] = traffic_speed['speed'] / 1024 / 1024 * 8
#     print(traffic_speed)
#     # traffic_speed = traffic_speed.reset_index().to_dict('records')
#     # print(traffic_speed)
#     # print(dfMainGrapf)
#     traffic_speed = traffic_speed.reset_index().to_dict('records')
#
#     return traffic_speed

origins = [
    "http://80.73.69.138:5432",
    "http://192,168.0.66:5137"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],

)

app.include_router(router)