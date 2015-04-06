import pandas as pd
import csv
import urllib2
import io

# List of all shows
def refresh_show_list():

    url = 'http://epguides.com/common/allshows.txt'
    response = urllib2.urlopen(url)

    all_shows = pd.read_csv(response)

    return all_shows


def get_show(id):
    show_url = 'http://epguides.com/common/exportToCSV.asp?rage=' + id
    print show_url

    raw = urllib2.urlopen(show_url).read()

    start = raw.find('<pre>')
    end = raw.find('</pre>')

    show = raw[start + 7 : end].strip()

    return pd.read_csv(io.BytesIO(show))


# Get show tvrage id
all_shows = refresh_show_list()
lost = all_shows[all_shows.title == 'Lost']
lost_id = ''.join(lost.tvrage.values)

print lost

# Get episode list
lost_df = get_show(lost_id)

print lost_df.head()
