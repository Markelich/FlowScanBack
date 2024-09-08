import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
pd.set_option('display.max_rows', 100000000)
pd.set_option('display.max_columns', 10)
pd.options.mode.chained_assignment = None

data = {
    'start_time': ['2023-10-01 00:00:02.745', '2023-10-01 00:00:02.876', '2023-10-01 00:00:03.000', '2023-10-01 00:00:05.345'],
    'end_time': ['2023-10-01 00:00:03.123', '2023-10-01 00:00:03.003', '2023-10-01 00:00:03.857', '2023-10-01 00:00:06.165'],
    'bytes': [3000000, 800000, 1000000, 2000000]
}

df = pd.DataFrame(data)



df['start_time'] = pd.to_datetime(df['start_time'])
df['end_time'] = pd.to_datetime(df['end_time'])
df['timediff'] = df['end_time'] - df['start_time']

df['speed(mb/s)'] = df['bytes']/1024/1024*8 / df['timediff'].dt.total_seconds()


time_index = pd.date_range(start=df['start_time'].min(), end=df['end_time'].max(), freq='S')
#
traffic_speed = pd.DataFrame(index=time_index, data={'speed(mb/s)': 0})
#
for i in range(len(df)):
    traffic_speed.loc[df['start_time'].iloc[i]:df['end_time'].iloc[i]] += df['speed(mb/s)'].iloc[i].astype(int)



print(df)
print(traffic_speed)

plt.figure(figsize=(12, 6))
plt.plot(traffic_speed.index, traffic_speed['speed(mb/s)'], label='Скорость трафика (байты/с)')

plt.title('Общая скорость трафика')
plt.xlabel('Время')
plt.ylabel('Скорость (байты/с)')
plt.legend()
plt.grid()
plt.xticks(rotation=45)
plt.tight_layout()
# print(traffic_speed)
plt.show()


