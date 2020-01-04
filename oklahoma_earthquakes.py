from datetime import datetime, timedelta
from threading import Timer

x = datetime.today()
y = x.replace(day=x.day, hour=23, minute=59, second=59, microsecond=0) + timedelta(days=1)
delta_t = y - x

secs = delta_t.total_seconds()


def eq_collection():
    import pandas as pd
    import pymongo
    from bs4 import BeautifulSoup as bs
    from splinter import Browser

    print('code running!')

    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php'
    browser = Browser('chrome', headless=False)

    browser.visit(url)

    browser.links.find_by_partial_text('All')[1].click()

    # read csv_file to pandas dataframe
    eq_file = "../Downloads/all_day.csv"
    eq_df = pd.read_csv(eq_file)
    eq_df.head()

    # select columns of interest
    new_eq_df = eq_df[['time', 'latitude', 'longitude', 'depth', 'mag', 'magType', 'place']].copy()

    # create new column column for state
    new_eq_df['state'] = new_eq_df['place'].str.split(',').str[1].str.strip()

    # set state to index so that the loc function can be used to find all earthquakes that happened in oklahoma
    new_eq_df = new_eq_df.set_index('state')

    # execute loc function
    new_eq_df = new_eq_df.loc['Oklahoma']

    # reset the index so that state will be pushed into the dataframe
    new_eq_df = new_eq_df.reset_index()

    # make connection to database and connect the client
    conn = 'mongodb://localhost:27017'
    client = pymongo.MongoClient(conn)

    # create database
    db = client.earthquakesDB

    # insert rows of the data frame as a dictionary to the database with column names as the key
    db.earthquakes.insert_many(new_eq_df.to_dict('records'))


t = Timer(secs, eq_collection)
t.start()