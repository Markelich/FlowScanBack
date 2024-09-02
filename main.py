from operator import index

from fastapi import FastAPI, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

import socket
import clickhouse_connect



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
async def get_ip_list(startTime: str, finalTime: str):
    query = f"""SELECT DstIP, SUM(Pkts) AS Pkts, SUM(Octets) AS Octets FROM `INPUT`
        WHERE (DstIP BETWEEN '10.1.0.0' and '10.2.0.0' OR DstIP BETWEEN '192.168.0.0' and '192.169.0.0')
        AND `Start` BETWEEN '{startTime}:00' AND '{finalTime}:00' GROUP BY DstIP
        ORDER BY Octets DESC"""
    print(query)
    query_result = client.query(query)
    set = (query_result.result_set)
    df = pd.DataFrame(set, columns=['IP', 'pkts', 'octets'])
    # print(set)
    df['octets'] = df['octets'] / (1024 * 1024)
    df['octets'] = df['octets'].apply(lambda x: round(x, 2))
    print(df)
    df = df.reset_index().to_dict('records')
    return df

@router.get("/domain_names/{server_addres}")
async def get_domain_name(server_addres):
    try:
        domain = socket.gethostbyaddr(server_addres)[0]
    except socket.herror:
        domain = 'Unknown'
    # print(domain)
    return domain


@router.get("/hosts/{user_addres}/{startTime}/{finalTime}")
async def get_serveses_list(user_addres, startTime: str, finalTime: str):
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

@router.get("/curentData")
async def get_last_data():
    queryMainGrapf = f"""WITH 
                    (SELECT max(`Start`) FROM `INPUT`) AS latest_timestamp
                SELECT Start, Octets
                FROM `INPUT`
                WHERE `Start` >= latest_timestamp - INTERVAL 15 MINUTE
                ORDER BY `Start`;"""
    print(queryMainGrapf)
    query_resultMainGrapf = client.query(queryMainGrapf)
    setMainGrapf = (query_resultMainGrapf.result_set)

    dfMainGrapf = pd.DataFrame(setMainGrapf, columns=['timestemp', 'octets'])

    dfMainGrapf['octets'] = dfMainGrapf['octets']
    dfMainGrapf['octets'] = dfMainGrapf['octets'].apply(lambda x: round(x, 2))
    print(dfMainGrapf)
    dfMainGrapf = dfMainGrapf.reset_index().to_dict('records')
    return dfMainGrapf

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