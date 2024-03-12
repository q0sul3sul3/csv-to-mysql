import argparse
import os
from urllib.parse import quote

import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Import CSV files into a MySQL database.')
parser.add_argument('-u', '--username', help='Database username', required=True)
parser.add_argument('-p', '--password', help='Database password', required=True)
parser.add_argument('--host', help='Database host', default='127.0.0.1')
parser.add_argument('--port', help='Database port', default='3306')
parser.add_argument('-d', '--database', help='Database name', required=True)
parser.add_argument('--folder', help='Folder containing CSV files', default=os.getcwd())

args = parser.parse_args()
username = args.username
password = args.password
host = args.host
port = args.port
database = args.database
path = args.folder

engine = create_engine(
    f'mysql+pymysql://{args.username}:%s@{args.host}:{args.port}/{args.database}?charset=utf8'
    % quote(args.password)
)

filenames = os.listdir(path)
for filename in filenames:
    if not filename.endswith('.csv'):
        continue

    df = pd.read_csv(
        os.path.join(path, filename),
        sep=',',
        dtype=str,
        skipinitialspace=True,
        encoding='utf-8',
    )
    df.columns = df.columns.str.strip()
    df.to_sql(
        name=filename.lower().replace('.csv', ''),
        con=engine,
        if_exists='replace',
        index=False,
    )
    print(f'{filename} imported')
