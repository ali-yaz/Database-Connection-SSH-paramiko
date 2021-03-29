
#!/usr/bin/env python
# Ali Yazdizadeh, 2020
import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder
from paramiko import SSHClient
import pandas.io.sql as sqlio
import pandas as pd


#connect to a remote database using a ssh private key
#to see how to generate an ssh private key use this link
with SSHTunnelForwarder(
    ssh_address_or_host=('IP address', 22),
    ssh_private_key='/path/to/ssh_private_key', 
    ssh_username='username',
    remote_bind_address=('localhost',port)
) as server:
    server.start()
    print('connected to server!')

    with psycopg2.connect(
        host='localhost', 
        port=server.local_bind_port, 
        user='user', 
        password='password', 
        dbname='database_name') as conn:
            cur = conn.cursor()
            
            # a query to send to psql
            query = '''
            Put your SQL/PostgreSQL query here
            '''
            df = pd.read_sql(query, conn)
            print(df)
    conn.close()
    server.close()

print("finished")


