import sqlite3 as lite
from pandas import *
con = lite.connect('./mldata.db')
cur = con.cursor()

cur.execute('SELECT Ticker from screens group by Ticker')

data = cur.fetchall()
try:
    cur.execute('DROP table price_data')
except:
    pass
    

for ticker in data:
    ticker = ticker[0]
    print ticker
    df = read_csv('http://real-chart.finance.yahoo.com/table.csv?s={0}&d=11&e=31&f=2014&g=d&a=0&b=2&c=2013&ignore=.csv'.format(ticker))
    df['Ticker'] = ticker
    df.to_sql('price_data', con, if_exists='append')
