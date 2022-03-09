# untuk menymbungkan ke database
import mysql.connector
import pandas as pd
from datetime import datetime

mydb = mysql.connector.connect(
  host="host_ip",
    port="3307",
  user="nama",
  password="pw",
    database="database",
    auth_plugin="mysql_native_password"
)

print(mydb) 

mycursor = mydb.cursor()

# untuk mebaca data csv

df = pd.read_csv("ETL Data Rasio dan Cakupan Kesehatan Medan - Sheet1 (1).csv")
df


# untuk menentukan waktu
now = datetime.now()

current_time = now
print("Current Time =", pd.to_datetime(current_time))

# menambahkan column
df.insert(loc=0, column='id', value="")
df["id_usecase"] = 50
df["id_api"] = 50
df["updated_at"] = now
df["created_at"] = now

# membaca lagii
df = df.fillna("")
df

# push to db
data = df.to_dict('records')
sql = """
    INSERT INTO latihan_staging (provinsi, kategori, kabkot, urusan, indikator, satuan, tahun, nilai, id_usecase, id_api, updated_at, created_at)
    VALUES ( %(provinsi)s, %(kategori)s, %(kabkot)s, %(urusan)s, %(indikator)s, %(satuan)s, %(tahun)s, %(nilai)s, %(id_usecase)s, %(id_api)s, %(updated_at)s, %(created_at)s   )
    """
for j in range(len(data)):
    mycursor.execute(sql,data[j])
    mydb.commit()
