import csv
import urllib2
import sqlite3
from datetime import datetime as dt

# Table names
SHOW_LIST = 'show_list_table'
EP_LIST = 'episode_list_table'

# String constants
SEASON_NUM = 'season'
EP_NUM = 'episode'
EP_TITLE = 'title'
EP_AIRDATE = 'airdate'


def str_to_date(date):
    """
    Takes an input string of the form, dd/mmm/yy, and converts it to a datetime
    object
    """
    return dt.strptime(date, '%d/%b/%y')


def refresh_show_list():

    url = 'http://epguides.com/common/allshows.txt'

    try:
        # Get CSV of all shows from epguides website
        response = urllib2.urlopen(url)
        csv_data = csv.reader(response)

        # Get headers from the CSV file to prepare for CREATE TABLE statement
        headers = clean_csv_headers(csv_data.next())

        column_names = [('%s varchar' % header) for header in headers]
        column_names = ',\n'.join(column_names)

        dm.query("DROP TABLE IF EXISTS " + SHOW_LIST)
        dm.query("CREATE TABLE " + SHOW_LIST + " (" + column_names + ")")

        # Import show data into the SQLite databasea
        shows = []

        for row in csv_data:
            shows.append(tuple(row))

        shows = filter(None, shows)  # Remove empty strings from the show array

        insert_values_into_table(shows, SHOW_LIST, headers)

    except:
        print "Unable to refresh shows"


def insert_values_into_table(values, table_name, headers):
    dm.querymany("INSERT INTO " + table_name + " VALUES (" +
                 ",".join(['?'] * len(headers)) +
                 ")", values)


def get_show_tvrage_id(show_name):

    r = dm.query("SELECT tvrage " +
                 "FROM " + SHOW_LIST + " " +
                 "WHERE title = ?", (show_name,))

    try:
        show_id = r.fetchone()['tvrage']

        if show_id == '':
            print "No show id for show"
            raise Exception
        else:
            return show_id

    except:
        print "cant"


def clean_csv_headers(headers):

    headers = [header.replace('?', '') for header in headers]
    headers = [header.replace(' ', '_') for header in headers]

    return headers


def create_episodes_table():
    show_url = 'http://epguides.com/common/exportToCSV.asp?rage=1'
    raw = urllib2.urlopen(show_url).read()

    # Need to strip some text before and after the data portion of file
    begin = raw.find('<pre>')
    end = raw.find('</pre>')

    raw2 = raw[begin + 7:end].strip()
    reader = csv.reader(raw2.split('\n'), delimiter=',')

    headers = clean_csv_headers(reader.next())
    headers = ["show_name"] + headers  # Add show_name as one of the fields

    column_names = [('%s varchar' % header) for header in headers]
    column_names = '(' + ','.join(column_names) + ')'

    dm.query("CREATE TABLE IF NOT EXISTS " + EP_LIST + column_names)


def add_show_to_db(show_name):
    """pulls all the episode information from epguides then inserts
    into EP_LIST
    table
    """
    try:
        # Get episode list from epguides.com
        show_id = get_show_tvrage_id(show_name)

        show_url = 'http://epguides.com/common/exportToCSV.asp?rage=' + show_id
        raw = urllib2.urlopen(show_url).read()

        # Need to strip some text before and after the data portion of file
        begin = raw.find('<pre>')  # beginning position of csv file
        end = raw.find('</pre>')   # End position of csv fiel

        raw2 = raw[begin + 7:end].strip()
        reader = csv.reader(raw2.split('\n'), delimiter=',')

        headers = clean_csv_headers(reader.next())
        headers = ["show_name"] + headers  # Add show_name as one of the fields

        column_names = [('%s varchar' % header) for header in headers]
        column_names = '(' + ','.join(column_names) + ')'

        dm.query("CREATE TABLE IF NOT EXISTS " + EP_LIST + column_names)
        dm.query("DELETE FROM " + EP_LIST + " " + "WHERE show_name = ?",
                 (show_name,))

        # Import episode data into DB
        episodes = []

        for row in reader:
            episodes.append(tuple([show_name] + row))

        insert_values_into_table(episodes, EP_LIST, headers)

    except TypeError:
        print "Show not found in database"
    except urllib2.URLError:
        print "URL doesn't work"
    except sqlite3.InterfaceError:
        print "sqlite3 error"
    except:
        print "Can't add show for some reason"


class DatabaseManager(object):

    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.conn.text_factory = str
        self.conn.row_factory = sqlite3.Row
        self.curs = self.conn.cursor()

    def query(self, *args):
        try:
            self.curs.execute(*args)
            self.conn.commit()
            return self.curs

        except:
            print "sqlite error"
            return -1

    def querymany(self, *args):
        try:
            self.curs.executemany(*args)
            self.conn.commit()
            return self.curs

        except:
            print "sqlite error"
            return -1

    def show_exists_in_db(self, show_name):

        row_count = dm.query(
            "SELECT count(1) FROM " + EP_LIST + " WHERE show_name = ?",
            (show_name,))

        row_count = row_count.fetchone()[0]

        return True if row_count > 0 else False

    def get_show_info(self, show_name):

        show_result = dm.query(
            "SELECT * " +
            "FROM " + SHOW_LIST + " " +
            "WHERE title = ?",
            (show_name,))

        show_result = show_result.fetchall()[0]

        return show_result

    def get_episode_info(self, show_name, season):

        season_eps_raw = dm.query(
            "SELECT episode, airdate, title " +
            "FROM " + EP_LIST + " " +
            "WHERE special = 'n' "
            "  and show_name = ? and season = ?",
            (show_name, season))

        season_eps_raw = season_eps_raw.fetchall()

        return season_eps_raw

    def get_list_of_seasons_for_show(self, show_name):

        season_raw = dm.query(
            "SELECT DISTINCT season " +
            "FROM " + EP_LIST + " " +
            "WHERE show_name = ?",
            (show_name,))

        season_raw = season_raw.fetchall()

        return season_raw

    def __del__(self):
        self.conn.close()


class Show(object):
    """Show class. will hold general show info and info about every episode"""

    def __init__(self, show_name):

        if not dm.show_exists_in_db(show_name):
            add_show_to_db(show_name)

        try:
            show_result = dm.get_show_info(show_name)
            self.id = show_result['tvrage']
            self.start_date = show_result['start_date']
            self.end_date = show_result['end_date']
            self.num_of_eps = show_result['number_of_episodes']
            self.run_time = show_result['run_time']
            self.network = show_result['network']

            season_raw = dm.get_list_of_seasons_for_show(show_name)
            self.seasons = {}
            for season in season_raw:
                current_season = Season(show_name, season[SEASON_NUM])
                self.seasons[season[SEASON_NUM]] = current_season

        except IndexError:
            print "Show not in database"

    def get_next_airdates(self):

        unaired_eps = []

        for season_num, season in self.seasons.items():
            for episode_num, episode in season.episodes.items():

                days_to_ep = (episode.airdate.date() - dt.now().date()).days

                hasnt_aired = (days_to_ep >= 0)

                if hasnt_aired:
                    unaired_eps.append(episode)

        sorted_unaired_eps = sorted(unaired_eps, key=lambda ep: ep.airdate)

        return sorted_unaired_eps

    def print_next_air_ep(self):
        try:
            next_ep = self.get_next_airdates()[0]
            print next_ep.title, "airs on", next_ep.airdate.date()
        except IndexError:
            print "No next episodes"


class Season(object):
    """Season object will hold episodes of the season"""

    def __init__(self, show_name, season):

        season_eps_raw = dm.get_episode_info(show_name, season)

        self.episodes = {}

        for ep in season_eps_raw:
            self.episodes[ep[EP_NUM]] = Episode(
                season,
                ep[EP_NUM],
                ep[EP_TITLE],
                str_to_date(ep[EP_AIRDATE]))


class Episode(object):

    def __init__(self, season, episode, title, airdate):
        self.season = season
        self.episode = episode
        self.title = title
        self.airdate = airdate

    def __str__(self):
        return "Season %s Episode %s - %s" % (self.season,
                                              self.episode,
                                              self.title)


dm = DatabaseManager("tvshows.db")

# refresh_show_list()
# add_show_to_db("Bob's Burgers")


saul = Show("Better Call Saul")
bob = Show("Bob's Burgers")
got = Show("Game of Thrones")

saul.print_next_air_ep()
bob.print_next_air_ep()
got.print_next_air_ep()
