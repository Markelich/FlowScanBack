# import pandas as pd
# from datetime import datetime
# import subprocess
# import re
# import time
# import os
# from io import StringIO
pd.set_option('display.max_rows', None)
pd.options.mode.chained_assignment = None
#
#
# rootDir = "/Netflow"
#
# def researchFiles(rootDir):
#     # steptime1 = time.time()
#     for i in sorted(os.listdir(rootDir)):
#         if os.path.isdir(rootDir + "/" + i):
#           researchFiles(rootDir + "/" + i)
#         if os.path.isfile(rootDir + "/" + i):
#             src_file = rootDir + "/" + i
#             folders = src_file.split('/')
#             folders[1] = 'Netflow_archive'
#             dst_file = '/'.join(folders) + ".csv"
#             if not os.path.isfile(dst_file):
#                 if not os.path.isdir(os.path.dirname(dst_file)):
#                     os.makedirs(os.path.dirname(dst_file))
#                 convert_to_csv(src_file, dst_file)
#                 # steptime2 = time.time()
#                 # print(steptime2 - steptime1)
#             else: print(dst_file, "is exist")
#
# def convert_to_csv(src_file, dst_file):
#     # startTime = time.time()
#     current_date = os.path.basename(os.path.dirname(dst_file))
#     with open(dst_file, 'w') as output_file:
#         text = subprocess.check_output(f"flow-cat {src_file} | flow-print -f5", shell=True, stderr=subprocess.STDOUT, encoding='utf-8')
#         modified_text = re.sub(" +", ",", text)
#         modified_text = re.sub(",\n", "\n", modified_text)
#         df = pd.read_csv(StringIO(modified_text))
#         df = df.loc[df['Octets'] != 0]
#         df['Start'] = pd.to_datetime(df['Start'], format='%m%d.%H:%M:%S.%f')
#         df['End'] = pd.to_datetime(df['End'], format='%m%d.%H:%M:%S.%f')
#         setYear("Start", current_date, df)
#         setYear("End", current_date, df)
#         # print(df)
#         df.to_csv(dst_file, index=False)
#         output_file.close()
#         # endTime = time.time()
#         # print(endTime - startTime, dst_file, "is writen")
#
#
#
# def setYear(timeArray, current_date, df):
#     realtime = datetime.strptime(current_date, "%Y-%m-%d")
#     realtimeTime = realtime.strftime('%m:%d')
#     if realtimeTime == '01:01':
#         df.loc[df[timeArray].dt.month == 12, timeArray] = df[timeArray].apply(lambda x: x.replace(year= realtime.year - 1))
#         df.loc[df[timeArray].dt.month == 1, timeArray] = df[timeArray].apply(lambda x: x.replace(year= realtime.year))
#     elif realtimeTime == '12:31':
#         df.loc[df[timeArray].dt.month == 12, timeArray] = df[timeArray].apply(lambda x: x.replace(year=realtime.year))
#         df.loc[df[timeArray].dt.month == 1, timeArray] = df[timeArray].apply(lambda x: x.replace(year=realtime.year + 1))
#     else:
#         df[timeArray] = df[timeArray].apply(lambda x: x.replace(year=realtime.year))
#     return df[timeArray]









researchFiles(rootDir)
