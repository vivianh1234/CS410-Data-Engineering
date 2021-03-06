import urllib.request
import json
import datetime as dt
import pytz
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
#################
import pandas as pd
import numpy as np
#import seaborn as sns
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

timezone = pytz.timezone("US/Pacific")
date = dt.datetime.today()
today = timezone.localize(date)

stop_name = "StopEventData{today}.json".format(today = today)
stop_name = stop_name[:23] + ".json"

url = "http://rbi.ddns.net/getStopEvents"
html = urlopen(url)
soup = BeautifulSoup(html, 'lxml')

def get_th(table):
  #stored cleaned headers
  headers = []

  row = table.find_all('tr')[0]
  cells = row.find_all('th')
  str_cells = str(cells)
  match = re.compile('<.*?>')
  clean = (re.sub(match, '', str_cells))
  headers.append(clean)
  temp = pd.DataFrame(headers)
  temp = temp[0].str.split(',', expand=True)
  temp.insert(0, 'trip_id', 'trip_id')
  return temp

def make_df(trip_id, table):
  #used to store cleaned rows
  list_rows = []

  #clean headers to get the trip id
  match = re.compile('<.*?>')
  match2 = re.compile(r'[^\d\W]')
  clean = (re.sub(match, '', str(trip_id)))
  trip_id = (re.sub(match2, '', str(clean))).strip()

  #first row of table
  row = table.find_all('tr')[1]
  cells = row.find_all('td')
  str_cells = str(cells)
  match = re.compile('<.*?>')
  clean = (re.sub(match, '', str_cells))
  list_rows.append(clean)
  temp = pd.DataFrame(list_rows)
  temp = temp[0].str.split(',', expand=True)
  temp.insert(0, 'trip_id', trip_id)
  return temp 
  
#title = soup.find('h1')
#print(title)
df = pd.DataFrame()
trip_ids = soup.find_all('h3')
tables = soup.find_all('table')
th_df = get_th(tables[0])
#df = df.append(make_df(trip_ids[1], tables[1]), ignore_index=True)
i = 0
for id in trip_ids:
    df = df.append(make_df(id, tables[i]), ignore_index=True)
    i += 1
#Cleaning data
stop_event = pd.concat([th_df.head(1), df], ignore_index=True)
stop_event = stop_event.rename(columns=stop_event.iloc[0])
stop_event = stop_event.drop(stop_event.index[0])
#stop_event.rename(columns=(' trip_id': 'trip_id'), inplace=True)
stop_event.rename(columns={' direction': 'direction'}, inplace=True)
stop_event.rename(columns={' service_key': 'service_key'}, inplace=True)
stop_event.rename(columns={' route_number': 'route_number'}, inplace=True)
stop_event['direction'] = stop_event['direction'].str.strip(' ')
stop_event['service_key'] = stop_event['service_key'].str.strip(' ')
stop_event['route_number'] = stop_event['route_number'].str.strip(' ')

#write json to file
stop_event.to_json(stop_name, orient='records', indent=2)

#put json into list
events = stop_event.to_json(orient='records', indent=2)
events_list = json.loads(events)

#print(events_list[0])

#################

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    for n in events_list:
        record_key = "stop-data"
        record_value = json.dumps(n)
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))

