# import pandas as pd
# import socket
#
# # Пример dataframe с ipv4 адресами
#
#
#
#
#
# data = [
#     {
#         'srcip': '162.159.200.123',
#         'dstip': '192.168.40.67',
#         'pkts': 2,
#         'octets': 0.0,
#     },
#     {
#         'srcip': '194.58.202.20',
#         'dstip': '192.168.40.67',
#         'pkts': 1,
#         'octets': 0.0,
#     }
# ]
# df = pd.DataFrame(data)
# print(df)
# # Функция для получения доменного имени по ipv4 адресу
# def get_domain_name(ip):
#     try:
#         dn = socket.gethostbyaddr(ip)[0]
#         return dn
#     except socket.herror:
#         return 'Unknown'
#
# # Создание нового столбца с доменными именами
# df['domain_name'] = df['srcip']
#
# print(df)
# import socket
#
#
# pip3 install clickhouse-connect