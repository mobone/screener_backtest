from pandas import *
import glob

import sqlite3 as lite
con = lite.connect('./mldata.db')
cur = con.cursor()


table_names = []
df = read_csv('./data/beta.csv')
df = df.transpose()
for i in range(len(df.ix['End Year'])):
    table_names.append('q%i_%i' % (df.ix['End Quarter'][i], df.ix['End Year'][i]))

dfs = []
file_names = []
exclude = [' ', '%', '.csv', './data/']
for files in glob.glob("./data/*.csv"):
    file_name = files
    for i in exclude:
        file_name = file_name.replace(i, '')
    file_names.append(file_name)
    
    df = read_csv(files)
    df = df.transpose()
    df.index.name = 'Ticker'
    df = df.drop('End Year')
    df = df.drop('End Quarter')
    dfs.append(df)
    
print file_names

for j in dfs[0].columns:
    df = None
    print '---'
    print table_names[j]

    # get metric for each stock
    for i in range(len(dfs)):
        this_df = DataFrame(dfs[i][j])
        this_df.columns = [file_names[i]]
        if df is None:
            df = DataFrame(this_df)
            df['date'] = table_names[j]
        else:
            df = df.join(this_df)

    # get prices for each stock

    for ticker in df.index:
        cur.execute('Select * from price_data where Ticker = \'%s\'' % ticker)
        print cur.fetchone()
        print ticker
    df.to_sql('screens', con, if_exists='append')
    print df

