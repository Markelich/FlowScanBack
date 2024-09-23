import clickhouse_connect
import pandas as pd
import matplotlib.pyplot as plt


pd.set_option('display.min_rows', 100)
pd.set_option('display.max_columns', 10)
pd.options.mode.chained_assignment = None

host = '80.73.69.138'
port = 5432
db ='traffic'
user = 'default'
password = 'default'

client = clickhouse_connect.get_client(host=host, port=5432, username=user, password=password, database=db)

queryMainGrapf =[
    {'start_time': '2024-09-18 22:00:11.444000+0900', 'end_time': '2024-09-18 22:00:11.956000+0900', 'bytes': 9881},
    {'start_time': '2024-09-18 22:00:12.252000+0900', 'end_time': '2024-09-18 22:00:13.008000+0900', 'bytes': 4288},
    {'start_time': '2024-09-18 22:00:12.288000+0900', 'end_time': '2024-09-18 22:00:12.800000+0900', 'bytes': 9839},
    {'start_time': '2024-09-18 22:00:13.004000+0900', 'end_time': '2024-09-18 22:00:13.004000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:13.340000+0900', 'end_time': '2024-09-18 22:00:13.848000+0900', 'bytes': 9281},
    {'start_time': '2024-09-18 22:00:13.368000+0900', 'end_time': '2024-09-18 22:00:13.860000+0900', 'bytes': 9841},
    {'start_time': '2024-09-18 22:00:13.884000+0900', 'end_time': '2024-09-18 22:00:14.300000+0900', 'bytes': 84},
    {'start_time': '2024-09-18 22:00:14.312000+0900', 'end_time': '2024-09-18 22:00:14.312000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:14.316000+0900', 'end_time': '2024-09-18 22:00:14.316000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:14.508000+0900', 'end_time': '2024-09-18 22:00:15.012000+0900', 'bytes': 9839},
    {'start_time': '2024-09-18 22:00:14.568000+0900', 'end_time': '2024-09-18 22:00:14.568000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:14.612000+0900', 'end_time': '2024-09-18 22:00:14.612000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:14.708000+0900', 'end_time': '2024-09-18 22:00:14.708000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:14.972000+0900', 'end_time': '2024-09-18 22:00:15.072000+0900', 'bytes': 223},
    {'start_time': '2024-09-18 22:00:15.200000+0900', 'end_time': '2024-09-18 22:00:15.200000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:15.212000+0900', 'end_time': '2024-09-18 22:00:15.212000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:16.680000+0900', 'end_time': '2024-09-18 22:00:16.680000+0900', 'bytes': 40},
    {'start_time': '2024-09-18 22:00:16.696000+0900', 'end_time': '2024-09-18 22:00:17.224000+0900', 'bytes': 9863},
    {'start_time': '2024-09-18 22:00:19.148000+0900', 'end_time': '2024-09-18 22:00:19.676000+0900', 'bytes': 9881},
    {'start_time': '2024-09-18 22:00:20.212000+0900', 'end_time': '2024-09-18 22:00:20.648000+0900', 'bytes': 124},
]

def calcPointsSpeedGraph(df):
    queryMainGrapf = f"""WITH 
    	                        (SELECT MAX(`Start`) FROM `INPUT`) AS last_time
                            SELECT `Start`, `Final`, `Octets` FROM `INPUT` i WHERE `Start` >= last_time - INTERVAL 15 MINUTE"""
    # print(queryMainGrapf)
    query_resultMainGrapf = client.query(queryMainGrapf)
    df = (query_resultMainGrapf.result_set)
    df = pd.DataFrame(df, columns=['start_time', 'end_time', 'bytes'])
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])

    time_index = pd.date_range(start=df['start_time'].min().round('s'), end=df['end_time'].max().round('s'), freq='s')
    traffic_speed = pd.DataFrame(index=time_index, data={'octets': 0})
    traffic_speed.insert(0, 'timepoint', traffic_speed.index)
    traffic_speed = traffic_speed.reset_index(drop=True)

    for i, row in traffic_speed.iterrows():
        row['octets'] += df[['bytes']].loc[(df['start_time'].dt.round('s') <= row['timepoint']) & (df['end_time'].dt.round('s') >= row['timepoint'])].sum()
        traffic_speed.loc[i, 'octets'] = row['octets'].iloc[0]

    return traffic_speed

traffic_speed = calcPointsSpeedGraph(queryMainGrapf)

plt.figure(figsize=(12, 6))
plt.plot(traffic_speed.timepoint, traffic_speed['octets'], label='Скорость трафика (байты/с)')
#
plt.title('Общая скорость трафика')
plt.xlabel('Время')
plt.ylabel('Скорость (байты/с)')
plt.legend()
plt.grid()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# df = pd.DataFrame(data, columns=['start_time', 'end_time', 'bytes'])
# # print(df)
# df['start_time'] = pd.to_datetime(df['start_time'])
# df['end_time'] = pd.to_datetime(df['end_time'])
# # df['timediff'] = df['end_time'] - df['start_time']
# # print(df)
# # df['timediff'] = df['timediff'].replace(pd.Timedelta('0 days 00:00:00.000000'), pd.Timedelta('0 days 00:00:00.001'))
#
# # df['speed'] = df['bytes'] / df['timediff'].dt.total_seconds()
# # print(df[['start_time', 'end_time', 'speed']])
#
# time_index = pd.date_range(start=df['start_time'].min().round('s'), end=df['end_time'].max().round('s'), freq='s')
# traffic_speed = pd.DataFrame(index=time_index, data={'octets': 0})
# traffic_speed.insert(0, 'timepoint', traffic_speed.index)
# traffic_speed = traffic_speed.reset_index(drop=True)
# # traffic_speed['octets'] = traffic_speed['octets'].astype(float)
#
# for i, row in traffic_speed.iterrows():
#     row['octets'] += (df[['bytes']].loc[(df['start_time'].dt.round('s') <= row['timepoint']) & (
#             df['end_time'].dt.round('s') >= row['timepoint'])].sum())
#     traffic_speed.loc[i, 'octets'] = row['octets'].iloc[0]
#     print(row)
# return traffic_speed
