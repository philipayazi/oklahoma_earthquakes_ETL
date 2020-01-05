import schedule
import time
import pandas as pd
import pymongo
from bs4 import BeautifulSoup as bs
from splinter import Browser
import os
from pygeocoder import Geocoder
from config import gkey

def eq_collection():

    print('code running!')

    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php'
    browser = Browser('chrome', headless=False)

    browser.visit(url)

    browser.links.find_by_partial_text('All')[1].click()

    time.sleep(10)

    # read csv_file to pandas dataframe
    eq_file = "../Downloads/all_day.csv"
    eq_df = pd.read_csv(eq_file)

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

    # Reverse Geocode latitudes and longitudes to get county where earthquakes occured
    new_eq_df['county'] = new_eq_df.apply(lambda x: Geocoder(api_key=gkey).reverse_geocode(x['latitude'],x['longitude']).county,axis=1)

    if new_eq_df.isna() is True:
        new_eq_df.dropna()

    # make connection to database and connect the client
    conn = 'mongodb://localhost:27017'
    client = pymongo.MongoClient(conn)

    # create database
    db = client.earthquakesDB

    # insert rows of the data frame as a dictionary to the database with column names as the key
    db.earthquakes.insert_many(new_eq_df.to_dict('records'))

    time.sleep(5)

    if os.path.exists("../Downloads/all_day.csv"):
        os.remove("../Downloads/all_day.csv")
    else:
        print("The file does not exist")

schedule.every().day.at("23:59").do(eq_collection)

while True:
    schedule.run_pending()
    time.sleep(60)

