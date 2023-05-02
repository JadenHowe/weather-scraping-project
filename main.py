import requests
from bs4 import BeautifulSoup
from lxml import etree
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
import sys
from pytz import timezone
from foundation.forecast_scrape import Scrape_Machine
from pytz import timezone
import json
from datetime import timedelta


scraper = Scrape_Machine()

now = datetime.now(timezone('America/New_York'))
time_num = float(now.hour)
time_num += now.minute/60




recorded_data = scraper.getRecordedData()



result = {
    'Date': str(now.month) + '-' + str(now.day) + '-' + str(now.year),
    'Time': str(now.hour) + ':' + "{:02d}".format(now.minute),
    'Time Number': time_num,
    'Recorded Data': scraper.getRecordedData(),
    'CLI': scraper.getCli(),
    'Forecasts': scraper.getAllForecasts()
}

print(json.dumps(result, indent=1))

scraper.uploadToDataBase(data=result, collection='knyc_snapshots')
