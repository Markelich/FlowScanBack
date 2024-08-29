import pandas as pd
from datetime import datetime
import subprocess
import re
import time
import os
from io import StringIO

rootDir = "/Netflow"

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


# for dirpath, _, filenames in os.walk(rootDir):
#     print(dirpath)
    # for filename in filenames:
    #     print(os.path.join(dirpath, filename))


