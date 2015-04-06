import csv
import urllib2
import sqlite3

# Create sqlite database
conn = sqlite3.connect('tvshows.db')


"""
>>> all_shows.next()
['title', 'directory', 'tvrage', 'start date', 'end date', 'number of episodes', 'run time', 'network', 'country']
"""

# List of all shows
# List of all shows
def refresh_show_list(conn):

c = conn.cursor()

url = 'http://epguides.com/common/allshows.txt'
response = urllib2.urlopen(url)

cr = csv.reader(response)


header = cr.next()

print header

column_names = " varchar,".join(header)
column_names = column_names.replace(' ', '_')

query =  "DROP TABLE IF EXISTS all_shows; CREATE TABLE all_shows (" + column_names + ")"
print "creating table... \n" + query

c.execute(query)
conn.commit()

for row in cr:
    c.execute("INSERT INTO all_shows VALUES (" + ','.join(['?'] * len(row)) + ")", row)
    conn.commit()

    return cr


all_shows = refresh_show_list(conn)





# url = 'http://epguides.com/common/allshows.txt'

# response = urllib2.urlopen(url)

# cr = csv.reader(response)


# # Episode
# url2 = 'http://epguides.com/common/exportToCSV.asp?rage=18164'

# r2 = urllib2.urlopen(url2)

# bytecode = r2.read()



# begin = bytecode.find('<pre>')  # beginning position of csv file
# end = bytecode.find('</pre>')   # End position of csv file


# show_raw = bytecode[begin+7 : end]         # Get only the relevant part of the file
# show_raw = show_raw.strip()
# show = csv.reader(show_raw)

# reader = csv.reader(show_raw.split('\n'), delimiter=',')
# # for row in reader:
# #     print row


# import pandas as pd
# import io

# p = pd.read_csv(io.BytesIO(show_raw))
