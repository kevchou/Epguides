import csv
import urllib2
import sqlite3
from datetime import datetime as dt

SHOW_LIST = 'show_list_table'
EP_LIST = 'episode_list_table'

# STRING CONSTANTS
SEASON_NUM = 'season'
EP_NUM = 'episode'
EP_TITLE = 'title'
EP_AIRDATE = 'airdate'


class DatabaseManager(object):

    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.conn.text_factory = str
        self.conn.row_factory = sqlite3.Row
        self.curs = self.conn.cursor()

    def query(self, *args):
        self.curs.execute(*args)
        self.conn.commit()
        return self.curs

    def querymany(self, *args):
        self.curs.executemany(*args)
        self.conn.commit()
        return self.curs

    def __del__(self):
        self.conn.close()


def refresh_show_list():
    """This function will pull a list of all the shows available on epguides and
    store it into a local SQLite database.
    """
    try:
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
        dm.query("DROP TABLE IF EXISTS " + SHOW_LIST)
        dm.query("CREATE TABLE " + SHOW_LIST + " (" + column_names + ")")

        # Import show data into the SQLite databasea
        shows = []

        for row in csv_data:
            shows.append(tuple(row))

        shows = filter(None, shows)  # Remove empty strings from the show array

        # Insert values into table
        dm.querymany("INSERT INTO " + SHOW_LIST + " VALUES (" +
                     ','.join(['?'] * len(headers)) + ")", shows)
    except:
        print "Unable to refresh shows"


def add_show_to_db(show_name):
    """If the inputted show_name is in the tv list table, this function will pull
    all the episode list information from epguides and puts it into the EP_LIST
    table
    """

    # Get show's TVRage ID from all show table
    r = dm.query("SELECT tvrage " +
                 "FROM " + SHOW_LIST + " " +
                 "WHERE title = ?", (show_name,))

    show_id_raw = r.fetchone()

    # TODO: replace this IF with try/catch?
    # If show cannot be found in the database, print message
    if show_id_raw is None:
        print "'" + show_name + "' cannot be found."
        return 0

    show_id = show_id_raw['tvrage']

    # No TV rage id found
    if show_id == '':
        print "no id."
        return 0

    try:
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
        column_names = '(' + ','.join(column_names) + ')'

        #  Create table if it exists
        dm.query("CREATE TABLE IF NOT EXISTS " + EP_LIST + column_names)

        # Delete data for current show off table
        dm.query("DELETE FROM " + EP_LIST + " " + "WHERE show_name = ?",
                 (show_name,))

        episodes = []

        for row in reader:
            episodes.append(tuple([show_name] + row))

        dm.querymany("INSERT INTO " + EP_LIST + " VALUES (" +
                     ','.join(['?'] * len(headers)) + ")", episodes)

        return 1
    except:
        print "Cannot add show"

def str_to_date(date):
    """
    Takes an input string of the form, dd/mmm/yy, and converts it to a datetime
    object
    """
    return dt.strptime(date, '%d/%b/%y')


class Show(object):
    """Show class. will hold general show info and info about every episode"""
    def __init__(self, show_name):

        # If no show info found, return nothing
        if add_show_to_db(show_name) == 0:
            return None

        # Show info
        show_result = dm.query("SELECT * " +
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
        season_raw = dm.query("SELECT DISTINCT season " +
                              "FROM " + EP_LIST + " " +
                              "WHERE show_name = ?", (show_name,))
        season_raw = season_raw.fetchall()

        # seasons will hold every single episode of the show
        self.seasons = {}

        for season in season_raw:

            # Current season string
            current_season = Season(show_name, season[SEASON_NUM])

            self.seasons[season[SEASON_NUM]] = current_season

    def get_next_airdates(self):
        """
        Gets next air date of show
        TODO: add stuff to do if show is over
        """
        # Array of unaired Episodes
        unaired_eps = []

        # Loops through every episode of the show
        for season in self.seasons:
            for episode in self.seasons[season].episodes:

                # Get number of days to each episode
                airdate = self.seasons[season].episodes[episode].airdate
                days_to_ep = (airdate - dt.now()).days

                # Check to see if each episode has aired or not
                hasnt_aired = (days_to_ep >= 0)

                # If episode hasn't aired already, append Episode to an array
                if hasnt_aired:
                    unaired_eps.append(self.seasons[season].episodes[episode])

        # Sort by airdate
        sorted_unaired = sorted(unaired_eps, key=lambda ep: ep.airdate)

        for episode in sorted_unaired:
            print episode

        return sorted_unaired

    # Do I need a getter method to get seasons?
    def season(self, seas):
        list_of_seasons = [int(s) for s in self.seasons.keys()]
        if seas in list_of_seasons:
            return self.seasons[str(s)]
        else:
            print "Enter a valid season"


class Season(object):
    """Season object will hold episodes of the season"""

    def __init__(self, show_name, season):

        season_eps_raw = dm.query("SELECT episode, airdate, title " +
                                  "FROM " + EP_LIST + " " +
                                  "WHERE special = 'n' "
                                  "  and show_name = ? and season = ?",
                                  (show_name, season))
        season_eps_raw = season_eps_raw.fetchall()

        self.episodes = {}

        for ep in season_eps_raw:
            self.episodes[ep[EP_NUM]] = Episode(season,
                                                ep[EP_NUM],
                                                ep[EP_TITLE],
                                                str_to_date(ep[EP_AIRDATE]))

    # Do I need a getter method to get episodes?
    def episode(self, ep):
        list_of_eps = [int(e) for e in self.episodes.keys()]
        if ep in list_of_eps:
            return self.episodes[str(e)]
        else:
            print "Enter a valid season"


class Episode(object):
    """Episode class that will hold the general info"""
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


class Favourite_Shows(object):

    def __init__(self):
        self.show_list = {}

    def add_show(self, show_name):
        if show_name not in self.show_list.keys():
            self.show_list[show_name] = Show(show_name)
        else:
            print "Show already added"

    def remove_show(self, show_name):
        if show_name in self.show_list.keys():
            del self.show_list[show_name]
        else:
            print "Show not added"


dm = DatabaseManager("tvshows.db")
