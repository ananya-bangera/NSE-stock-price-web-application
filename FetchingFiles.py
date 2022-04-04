
import requests
from contextlib import closing
import csv
from urllib.request import urlopen
import requests
from bs4 import BeautifulSoup
import pandas as pd
#part1
url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

with closing(requests.get(url, stream=True)) as r:
    f = (line.decode('utf-8') for line in r.iter_lines())
    reader = csv.reader(f, delimiter=',', quotechar='"')
    for row in reader:
        print(row)



#part2
url = "https://www1.nseindia.com/ArchieveSearch?h_filetype=fobhav&date=01-04-2022&section=FO"

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Referer": "https://www1.nseindia.com/products/content/derivatives/equities/archieve_fo.htm",
}

soup = BeautifulSoup(requests.get(url, headers=headers).content, "html.parser")
csv_url = "https://www1.nseindia.com" + soup.a["href"]

print("Downloading {}...".format(csv_url))
with open(csv_url.split("/")[-1], "wb") as f_out:
    f_out.write(requests.get(csv_url, headers=headers).content)
print("Done.")
  
from zipfile import ZipFile
with ZipFile('fo01APR2022bhav.csv.zip', 'r') as zipObj:
   zipObj.extractall()


import pandas as pd
import pyodbc
import mariadb
import sys

# Import CSV
data = pd.read_csv ('fo01APR2022bhav.csv')   
df = pd.DataFrame(data)
print(df)
# driver = 'SQL Server'
# server = '**server-name**'
# db1 = 'CorpApps'
# tcon = 'yes'
# uname = 'jnichol3'
# pword = '**my-password**'
# Connect to SQL Server

try:
    conn = mariadb.connect(
        user="root@localhost",
        password="Ab@03#",
        host="192.0.2.1",
        port=3306,
        database="employees"

    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cursor = conn.cursor()

# Create Table
# cursor.execute('''
# 		CREATE TABLE products (
# 			product_id int primary key,
# 			product_name nvarchar(50),
# 			price int
# 			)
#                ''')

# Insert DataFrame to Table
# for row in df.itertuples():
#     cursor.execute('''
#                 INSERT INTO products (product_id, product_name, price)
#                 VALUES (?,?,?)
#                 ''',
#                 row.product_id, 
#                 row.product_name,
#                 row.price
#                 )
# conn.commit()

