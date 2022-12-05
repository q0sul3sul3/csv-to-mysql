import os
import pandas as pd

from sqlalchemy import create_engine
from urllib.parse import quote
from getpass import getpass


folderpath = input('Enter Folder Path: ')
user = input('Enter User: ')
password = getpass('Enter Password: ')
host = input('Enter Host: ')
port = input('Enter Port: ')
databasename = input('Enter Database Name: ')
 
engine = create_engine('mysql+pymysql://{}:%s@{}:{}/{}?charset=utf8'.format(user, host, port, databasename) % quote(password))

# import data into database
count = 0
filenames = os.listdir(folderpath)
for i in filenames:
    df = pd.read_csv(os.path.join(folderpath, i), 
                    sep=',', 
                    quotechar='"', 
                    encoding='utf-8', 
                    skipinitialspace=True, 
                    dtype=str)
    count += 1
    df.columns = df.columns.str.strip()
    df.to_sql(name=i.lower().replace('.csv', ''), con=engine, if_exists='replace', index=False)
    print(i)
print('{} items imported.'.format(count))