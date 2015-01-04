from pandas import *
import sqlite3 as lite
import time
con = lite.connect('./mldata.db')
cur = con.cursor()

dates = { 'q1_2013': ('2013-01-02', '2013-03-31'),
		  'q2_2013': ('2013-04-01', '2013-06-31'), 
		  'q3_2013': ('2013-07-01', '2013-09-30'),
		  'q4_2013': ('2013-10-01', '2013-12-31'),
		  'q1_2014': ('2014-01-02', '2014-03-31'),
		  'q2_2014': ('2014-04-01', '2014-06-30'),
		  'q3_2014': ('2014-07-01', '2014-09-30'),
		  'q4_2014': ('2014-10-01', '2014-12-31')
		}
		
q = 'SELECT Ticker from screens group by Ticker'

data = read_sql(q, con)


q = 'Select Ticker, Date, `Adj Close` from price_data'
df = read_sql(q, con, index_col = 'Ticker')



final_output = []
for ticker in data['Ticker']:
    print ticker
    for date in dates:
        cur_df =  df.ix[ticker]
        cur_df = cur_df.set_index(['Date'])
        try:
            initial = cur_df['Adj Close'][dates[date][0]]
            final = cur_df['Adj Close'][dates[date][1]]
            string = (ticker, date, (final-initial)/initial)
            final_output.append(string)
        except Exception as e:
#            print e
            pass
df = DataFrame(final_output, columns = [ 'Ticker', 'Period', 'Percent Change'] )
print df
df.to_sql('price_changes', con, if_exists='replace')
