import pandas as pd
import wget

from mysql import connector

url='https://datasets.imdbws.com/name.basics.tsv.gz'
path = 'F:\\imdb_data\\'
wget.download(url,out=path)



import gzip
import shutil
with gzip.open('F:\\imdb_data\\name.basics.tsv.gz', 'rb') as f_in:
    with open('F:\\imdb_data\\name_basics.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

cnx = connector.connect(user='root',password='',host='localhost',database='lnd')
cursor =cnx.cursor()

def executeScriptsFromFile(filename):
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    
    for command in sqlCommands:
        if command.strip() != '':
                cursor.execute(command)
                
executeScriptsFromFile('local_to_mysql.sql')
cnx.commit()