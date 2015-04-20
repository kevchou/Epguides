import csv
import urllib2
import sqlite3
from datetime import datetime as dt


SHOW_LIST = 'show_list_table'
EP_LIST = 'episode_list_table'


# List of all shows
def refresh_show_list():
    """This function will pull a list of all the shows available on epguides and
    store it into a local SQLite database.
    """

    # Create sqlite database
    conn = sqlite3.connect('tvshows.db')
    conn.text_factory = str
    c = conn.cursor()

    # Get CSV of all shows from epguides website
    url = 'http://epguides.com/common/allshows.txt'
    response = urllib2.urlopen(url)
    csv_data = csv.reader(response)

    # Get headers from the CSV file and prepare it for CREATE TABLE statement
    headers = csv_data.next()
    headers = [header.replace(' ', '_') for header in headers]

    column_names = [('%s varchar' % header) for header in headers]
    column_names = ',\n'.join(column_names)

    # Drop table if it exists then recreate it
    drop_query = "DROP TABLE IF EXISTS " + SHOW_LIST
    c.execute(drop_query)

    create_query = "CREATE TABLE " + SHOW_LIST + " (" + column_names + ")"
    c.execute(create_query)

    # Import show data into the SQLite databasea
    shows = []

    for row in csv_data:
        shows.append(tuple(row))

    shows = filter(None, shows)  # Remove empty strings from the show array

    # Insert into the table
    c.executemany("INSERT INTO " + SHOW_LIST + " VALUES (" +
                  ','.join(['?'] * len(headers)) + ")", shows)

    conn.commit()
    conn.close()


def add_show_to_db(show_name):
    """If the inputted show_name is in the tv list table, this function will pull
    all the episode list information from epguides and puts it into the EP_LIST
    table
    """

    # Connect to database
    conn = sqlite3.connect("tvshows.db")
    conn.text_factory = str
    c = conn.cursor()

    # Get show's TVRage ID from all show table
    r = c.execute("SELECT tvrage " +
                  "FROM " + SHOW_LIST + " " +
                  "WHERE title = ?", (show_name,) )

    show_id = r.fetchone()

    # If show cannot be found in the database, print message
    if show_id is None:
        print "'" + show_name + "' cannot be found."
        return

    show_id = show_id[0]

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
    headers = ["show_name"] + headers

    column_names = [('%s varchar' % header) for header in headers]
    column_names = ',\n'.join(column_names)

    # Drop table if it exists, and recreate it
    create_query = "CREATE TABLE IF NOT EXISTS "+EP_LIST+'('+column_names+')'
    c.execute(create_query)

    c.execute("DELETE FROM " + EP_LIST + " " +
              "WHERE show_name = ?", (show_name,))

    episodes = []

    for row in reader:
        episodes.append(tuple([show_name] + row))

    c.executemany("INSERT INTO " + EP_LIST + " VALUES (" +
                  ','.join(['?'] * len(headers)) + ")", episodes)

    conn.commit()
    conn.close()


def str_to_date(date):
    """
    Takes an input string of the form, dd/mmm/yy, and converts it to a datetime
    object
    """
    return dt.strptime(date, '%d/%b/%y')


def find_next_airdates(show_name):

    conn = sqlite3.connect("tvshows.db")
    conn.text_factory = str
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    results = c.execute("SELECT show_name, season, episode, airdate, title " +
                        "FROM " + EP_LIST + " " +
                        "WHERE show_name = ? AND special = 'n'", (show_name,))

    results = results.fetchall()

    airdates = [str_to_date(row['airdate']) for row in results]

    days_to_next_air = [(date - dt.now()).days for date in airdates]

    min_days_to_next = min([days for days in days_to_next_air if days >= 0])

    r = [a
         for a
         in results
         if (str_to_date(a['airdate']) - dt.now()).days == min_days_to_next]

    return r


class Show:

    def __init__(self, show_name):

        add_show_to_db(show_name)

        db = sqlite3.connect('tvshows.db')
        db.text_factory = str
        db.row_factory = sqlite3.Row
        curs = db.cursor()

        # Show info
        show_result = curs.execute("SELECT * " +
                                   "FROM " + SHOW_LIST + " " +
                                   "WHERE title = ?", (show_name,))

        show_result = show_result.fetchall()[0]

        self.id = show_result['tvrage']
        self.start_date = show_result['start_date']
        self.end_date = show_result['end_date']
        self.num_of_eps = show_result['number_of_episodes']
        self.run_time = show_result['run_time']
        self.network = show_result['network']

        # Episode info
        seasons_result = curs.execute("SELECT DISTINCT season " +
                                      "FROM " + EP_LIST + " " +
                                      "WHERE show_name = ?", (show_name,))

        seasons_result = seasons_result.fetchall()

        self.seasons = {}
        for s in seasons_result:
            episode_result = curs.execute("SELECT episode, airdate, title " +
                                          "FROM " + EP_LIST + " " +
                                          "WHERE show_name = ? and season = ?",
                                          (show_name, s['season']))
            episode_result = episode_result.fetchall()

            episodes = {}
            for e in episode_result:
                episodes[e['episode']] = Episode(s['season'],
                                                 e['episode'],
                                                 e['title'],
                                                 str_to_date(e['airdate']))

            self.seasons[s['season']] = episodes

    def get_next_airdate(self):
        """
        Gets next air date of show
        TODO: add stuff to do if show is over
        """
        cur_min_days = None

        for season in self.seasons:
            for episode in self.seasons[season]:

                curr_airdate = self.seasons[season][episode].airdate

                curr_days_to_ep = (curr_airdate - dt.now()).days

                hasnt_aired = (curr_days_to_ep >= 0)
                curr_ep_is_closer = (curr_days_to_ep < cur_min_days)

                if hasnt_aired and (curr_ep_is_closer or cur_min_days is None):
                    cur_min_days = curr_days_to_ep
                    next_ep = (season, episode)

        # Prints out the info for the next episode
        next_episode = self.seasons[next_ep[0]][next_ep[1]]

        print "Next episode '%s'" % next_episode['title']
        print "Airs in %s days on %s" % (cur_min_days,
                                         next_episode['airdate'])

    def __str__(self):
        return self.id


class Episode:

    def __init__(self, season, episode, title, airdate):

        self.season = season
        self.episode = episode
        self.title = title
        self.airdate = airdate

    def __str__(self):
        return "Season %s Episode %s - %s (%s)" % (self.season,
                                                   self.episode,
                                                   self.title,
                                                   self.airdate)


# Start test
db = sqlite3.connect('tvshows.db')
db.text_factory = str
db.row_factory = sqlite3.Row
curs = db.cursor()

# Try array of shows
fav_shows = ["Game of Thrones",
             "Breaking Bad",
             "Mad Men",
             "Louie",
             "Last Week Tonight with John Oliver",
             "Bob's Burgers"]

for s in fav_shows:
    add_show_to_db(s)


print "********************************************"
print "Favourite shows and their season count"
a = curs.execute("SELECT show_name, count(distinct season) as ep_cnt " +
                 "FROM " + EP_LIST + " " +
                 "WHERE special = 'n' " +
                 "GROUP BY 1")

print a.fetchall()
print

# Test if a show cannot be found
print "********************************************"
print "trying to add fake show to db:"
add_show_to_db("made up show")


print "********************************************"
print "Print next air date for a show::"

find_next_airdate("Bob's Burgers")



# TODO
# Create Classes for a show which holds an array of episodes? which are also a class?
