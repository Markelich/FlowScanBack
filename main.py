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


def convert_bytes(bytes):
    if bytes >= 1024*1024*1024*1024:
        return f'{bytes / (1024*1024*1024*1024):.2f} TB'
    elif bytes >= 1024*1024*1024:
        return f'{bytes / (1024*1024*1024):.2f} GB'
    elif bytes >= 1024*1024:
        return f'{bytes / (1024*1024):.2f} MB'
    elif bytes >= 1024:
        return f'{bytes / 1024:.2f} KB'
    else:
        return f'{bytes} bytes'


# def calcPointsSpeedGraph(data):
#
#     df = pd.DataFrame(data, columns=['start_time', 'end_time', 'bytes'])
#     print(df)
#     df['start_time'] = pd.to_datetime(df['start_time'])
#     df['end_time'] = pd.to_datetime(df['end_time'])
#     df['timediff'] = df['end_time'] - df['start_time']
#     print(df)
#     df['timediff'] = df['timediff'].replace(pd.Timedelta('0 days 00:00:00.000000'), pd.Timedelta('0 days 00:00:00.001'))
#
#     df['speed'] = df['bytes'] / df['timediff'].dt.total_seconds()
#     # print(df[['start_time', 'end_time', 'speed']])
#
#     time_index = pd.date_range(start=df['start_time'].min().round('s'), end=df['end_time'].max().round('s'), freq='s')
#     traffic_speed = pd.DataFrame(index=time_index, data={'speed': 0})
#     traffic_speed.insert(0, 'timepoint', traffic_speed.index)
#     traffic_speed = traffic_speed.reset_index(drop=True)
#     traffic_speed['speed'] = traffic_speed['speed'].astype(float)
#
#     for i, row in traffic_speed.iterrows():
#         row['speed'] += (df[['bytes']].loc[(df['start_time'].dt.round('s') <= row['timepoint']) & (
#                     df['end_time'].dt.round('s') >= row['timepoint'])].sum()) / (1024 * 1024 * 1024) * 8
#         traffic_speed.loc[i, 'speed'] = row['speed'].iloc[0]
#
#     return traffic_speed

def calcPointsSpeedGraph(data):
    df = pd.DataFrame(data, columns=['start_time', 'end_time', 'bytes'])
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])

    time_index = pd.date_range(start=df['start_time'].min().round('s'), end=df['end_time'].max().round('s'), freq='s')
    traffic_speed = pd.DataFrame(index=time_index, data={'octets': 0})
    traffic_speed.insert(0, 'timepoint', traffic_speed.index)
    traffic_speed = traffic_speed.reset_index(drop=True)

    for i, row in traffic_speed.iterrows():
        row['octets'] += df[['bytes']].loc[(df['start_time'].dt.round('s') <= row['timepoint']) & (
                    df['end_time'].dt.round('s') >= row['timepoint'])].sum()
        traffic_speed.loc[i, 'octets'] = row['octets'].iloc[0]
    return traffic_speed


@router.get("/{hosts}/{startTime}/{finalTime}")
async def get_ip_list(startTime: str, finalTime: str):
    query = f"""SELECT DstIP, SUM(Octets) AS Octets FROM `INPUT`
        WHERE (DstIP BETWEEN '10.1.0.0' and '10.2.0.0' OR DstIP BETWEEN '192.168.0.0' and '192.169.0.0')
        AND `Start` BETWEEN '{startTime}:00' AND '{finalTime}:00' GROUP BY DstIP
        ORDER BY Octets DESC"""
    # print(query)
    query_result = client.query(query)
    set = (query_result.result_set)
    df = pd.DataFrame(set, columns=['IP', 'octets'])
    df['octets'] = df['octets'].apply(lambda x: convert_bytes(x))
    # print(df)
    df = df.reset_index().to_dict('records')
    return df





@router.get("/hosts/{user_addres}/{startTime}/{finalTime}")
async def get_serveses_list(user_addres, startTime: str = '2024-08-17T15:43', finalTime: str = '2024-08-17T18:44'):
    # print(user_addres)
    query = f"""SELECT SrcIP, DstIP, SUM(Octets) AS Octets FROM `INPUT`
            WHERE DstIP = '{user_addres}'
            AND `Start` BETWEEN '{startTime}:00' AND '{finalTime}:00' GROUP BY (DstIP, SrcIP)
            ORDER BY Octets DESC"""
    # print(query)
    query_result = client.query(query)
    set = (query_result.result_set)
    df = pd.DataFrame(set, columns=['srcip', 'dstip', 'octets'])
    df['octets'] = df['octets'].apply(lambda x: convert_bytes(x))
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




@router.get("/curentData")
async def get_last_data():
    queryMainGrapf = f"""WITH 
	                        (SELECT MAX(`Start`) FROM `INPUT`) AS last_time
                        SELECT `Start`, `Final`, `Octets` FROM `INPUT` i WHERE `Start` >= last_time - INTERVAL 15 MINUTE"""
    # print(queryMainGrapf)
    query_resultMainGrapf = client.query(queryMainGrapf)
    setMainGrapf = (query_resultMainGrapf.result_set)
    traffic_speed = calcPointsSpeedGraph(setMainGrapf)
    traffic_speed = traffic_speed.to_dict('records')
    # print(traffic_speed)
    return traffic_speed

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