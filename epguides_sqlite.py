import csv
import urllib2
import sqlite3


# List of all shows
def refresh_show_list():

    # Create sqlite database
    conn = sqlite3.connect('tvshows.db')
    conn.text_factory = str

    c = conn.cursor()

    url = 'http://epguides.com/common/allshows.txt'
    response = urllib2.urlopen(url)

    cr = csv.reader(response)

    # Get headers from the CSV file and prepare it for CREATE TABLE statement
    headers = cr.next()
    headers = [header.replace(' ', '_') for header in headers]

    column_names = [('%s varchar' % header) for header in headers]
    column_names = ',\n'.join(column_names)

    # Drop table if it exists, and recreate it
    drop_query = "DROP TABLE IF EXISTS all_shows"
    c.execute(drop_query)

    create_query = 'CREATE TABLE all_shows (' + column_names + ')'
    c.execute(create_query)

    # Import show data into the SQLite databasea
    shows = []

    for row in cr:
        if len(row) > 0:
            shows.append(tuple(row))

    c.executemany("INSERT INTO all_shows VALUES (" +
                  ','.join(['?'] * len(headers)) + ")", shows)

    conn.commit()
    conn.close()

def add_show_to_db(show_name):

    # Connect to database
    conn = sqlite3.connect("tvshows.db")
    conn.text_factory = str
    c = conn.cursor()

    # Get show's TVRage ID from all show table
    r = c.execute("SELECT tvrage FROM all_shows WHERE title = '" +
                  show_name + "'")
    show_id = r.fetchone()[0]

    # Get episode list from epguides.com
    show_url = 'http://epguides.com/common/exportToCSV.asp?rage=' + show_id
    r = urllib2.urlopen(show_url)

    # Need to strip some text before and after the data portion of file
    raw = r.read()
    begin = raw.find('<pre>')  # beginning position of csv file
    end = raw.find('</pre>')   # End position of csv fiel

    raw2 = raw[begin + 7:end].strip()
    reader = csv.reader(raw2.split('\n'), delimiter=',')

    # Get headers from the CSV file and prepare it for CREATE TABLE statement
    headers = reader.next()
    headers = [header.replace('?', '').replace(' ', '_') for header in headers]
    headers = [show_name] + headers

    column_names = [('%s varchar' % header) for header in headers]
    column_names = ',\n'.join(column_names)

    # Drop table if it exists, and recreate it
    create_query = 'CREATE TABLE IF NOT EXISTS episodes (' + column_names + ')'
    c.execute(create_query)

    c.execute("DELETE FROM episodes WHERE show_name = '" + show_name + "'")

    episodes = []

    for row in reader:
        episodes.append(tuple([show_name] + row))

    c.executemany("INSERT INTO episodes VALUES (" +
                  ','.join(['?'] * len(headers)) + ")", episodes)

    conn.commit()
    conn.close()



db = sqlite3.connect('tvshows.db')
db.text_factory = str
curs = db.cursor()

# Test show list refresh_show_list
refresh_show_list()

results = curs.execute("select * from all_shows")
results2 = results.fetchall()

print "---------------------------------"
print "Print first 3 entries from all_shows table"
print results2[:3]
print


add_show_to_db('Game of Thrones')

a = curs.execute("SELECT * FROM episodes WHERE season = 5 AND episode = 1")

print "---------------------------------"
print "Print episodes example"
print a.fetchall()
print

### Try array of shows
fav_shows = ['Game of Thrones', 'Breaking Bad', 'Mad Men', 'Louie']

for s in fav_shows:
    add_show_to_db(s)

a = curs.execute("SELECT show_name, count(distinct season) as ep_cnt FROM episodes WHERE special = 'n' GROUP BY 1")

print "---------------------------------"
print "Favourite shows and their season count"
print a.fetchall()
print


db.close()
