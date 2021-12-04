#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import pytz
import datetime as dt
from datetime import timedelta
import urllib.request
import re
import psycopg2
import time

timezone = pytz.timezone("US/Pacific")
date = dt.datetime.today()
today = timezone.localize(date)
data = urllib.request.urlopen("http://rbi.ddns.net/getBreadCrumbData")
name = "AssertionFailures{today}.txt".format(today = today)
name = name[:24]+".txt"

DBname = "gradualghosts_db"
DBuser = "hvivian"
DBpwd = "grubbythumbsup"
TableNameBC = "breadcrumb"
TableNameT = "trip"
cmdlist = []
skip = False

def convert_t(seconds):
  return time.strftime("%H:%M:%S", time.gmtime(int(seconds)))

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection

def load(conn, icmdlist):

	with conn.cursor() as cursor:
		print(f"Loading {len(icmdlist)} rows")
		start = time.perf_counter()
    
		for cmd in icmdlist:
			print (cmd)
			cursor.execute(cmd)

		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

#deal with breadcrumb data
def consume_bc(data):

  global skip
  skip = False

  #Data Assertions
  #assert every record has trip number
  if data['EVENT_NO_TRIP'] == "":
      skip = True 
      failures["event_no_trip_existence"].append(data)

  #assert that the stop index has a length of 9
  if len(data['EVENT_NO_STOP']) < 9:
      skip = True 
      failures["event_no_stop_length"].append(data)

  #assert that the gps longitude exists and is within the range -180 to 180
  gps_long = 0
  if data['GPS_LONGITUDE'] != "":
    gps_long = float(data['GPS_LONGITUDE'])
    if gps_long < -180 or gps_long > 180:
      skip = True 
      failures["gps_longitude_range"].append(data)
  else:
    skip = True

  #assert that the gps latitude exists and is within the range -90 to 90
  gps_lat = 0
  if data['GPS_LATITUDE'] != "":
    gps_lat = float(data['GPS_LATITUDE'])
    if gps_lat < -180 or gps_long > 180:
      skip = True 
      failures["gps_latitude_range"].append(data)
  else:
    skip = True

  #assert that the date is in the correct format
  regex = "^\d{1,2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC){1}-\d{2}$"
  match = re.search(regex, data['OPD_DATE'])
  if match == None:
      skip = True 
      failures["opd_date_format"].append(data)

  #assert that the meters is greater than zero
  meters = int(data['METERS'])
  if meters <= 0:
      skip = True 
      failures["meters_range"].append(data)

  #The ACT_TIME field should range from 0 - 86400
  act_time = int(data["ACT_TIME"])
  if act_time < 14867 or act_time > 90521:
      skip = True 
      failures["act_time_range"].append(data)
  
  #If the VELOCITY field exists, it should be a non-negative number
  if data["VELOCITY"] == '':
      skip = True
      pass
  else:
      velocity = int(data["VELOCITY"])
      if velocity < 0:
          skip = True 
          failures["velocity_nonnegative"].append(data)

  #Every direction value should be in the range 0 - 359
  if data["DIRECTION"] == '':
      skip = True
      pass
  else:
      direction = int(data["DIRECTION"])
      if direction < 0 or direction > 359:
          skip = True 
          failures["direction_range"].append(data)

  #The GPS_SATELLITES field should be a non-negative number
  if data['GPS_SATELLITES'] != "":
    gps_satellites = int(data["GPS_SATELLITES"])
    if gps_satellites < 0:
      skip = True 
      failures["gps_satellites_nonnegative"].append(data)
  else:
    skip = True

  #The GPS_HDOP field should be a non-negative number
  if data["GPS_HDOP"] != "":
    gps_hdop = float(data["GPS_HDOP"])
    if gps_hdop < 0.0:
      skip = True 
      failures["gps_hdop_nonnegative"].append(data)

  #Data Transformation
  
  #Timestamp: Convert the ACT_TIME (seconds from midnight) to postgres timestamp
  timestamp = str(data["OPD_DATE"]) + " " + str(convert_t(data["ACT_TIME"]))

  #Latitude: convert the gps_latitude string to a float
  latitude = gps_lat

  #Longitude: convert the gps_longitude string to a float
  longitude = gps_long

  #Direction: convert the direction string to an integer
  if data["DIRECTION"] == "":
    skip = True
  else:
    direction = int(data["DIRECTION"])

  #Speed: convert the velocity string into a float and convert the float from 
  #meters per second to miles per hour.
  if data["VELOCITY"] == "":
    skip = True
  else:
    speed = float(data["VELOCITY"])
    speed = speed * 2.237

  #Trip_id: convert the event_no_trip string to an integer
  trip_id = int(data["EVENT_NO_TRIP"])
      
  #Route_id: Not enough data to fill out the data. Set to NULL
  route_id = None

  #Vehicle_id: convert the vehicle_id string to an integer
  vehicle_id = int(data["VEHICLE_ID"])

  #Service_key:Not enough data to fill out the data. Set to NULL
  service_key = None

  #Direction:Not enough data to fill out the data. Set to NULL
  direction_0_1 = None


  if skip == False:
    #Create command to insert into Trip table
    cmd = f"INSERT INTO {TableNameT} (trip_id, vehicle_id)VALUES ({trip_id}, {vehicle_id}) ON CONFLICT (trip_id) DO NOTHING;"
    cmdlist.append(cmd)

    #Create command to insert into BreadCrumb table
    cmd = f"INSERT INTO {TableNameBC} VALUES ('{timestamp}', {latitude}, {longitude}, {direction}, {speed}, {trip_id});"
    cmdlist.append(cmd)
        

def consume_stop(data):

  global skip
  skip = False
  
  #Data Validation
  trip_id = int(data["trip_id"])

  #route_id
  route_number = 0
  if data["route_number"] != '':
    route_number = int(data["route_number"])
    if route_number < 0:
      skip = True
      failures["route_id_range"].append(data)

  #direction
  direction = -1
  if data["direction"] != '':
    direction = int(data["direction"])
    if direction != 0 and direction != 1: 
        skip = True 
        failures["stop_direction_range"].append(data)

  #service_key
  service_key = data["service_key"]
  if service_key != 'W' and service_key != 'S' and service_key != 'U': 
      skip = True 
      failures["stop_direction_range"].append(data)


  #Data Transformation
  if service_key == 'W':
    service_key = "Weekday"
  elif service_key == 'S':
    service_key = "Saturday"
  elif service_key == 'U':
    service_key = "Sunday"
  else:
    skip = True

  if direction == 0:
    direction = "Out"
  elif direction == 1:
    direction = "Back"
  else:
    skip = True


  if skip == False:
    cmd = f"UPDATE {TableNameT} SET route_id = '{route_number}', service_key = '{service_key}', direction = '{direction}' WHERE trip_id = {trip_id};"
    cmdlist.append(cmd)





if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest',
    })

    #dict for assertion failures
    failures = {"event_no_trip_existence":[],
            "event_no_stop_length":[],
            "gps_longitude_range":[],
            "gps_latitude_range":[],
            "opd_date_format":[],
            "meters_range":[],
            "act_time_range":[],
            "velocity_nonnegative":[],
            "direction_range":[],
            "gps_satellite_nonnegative":[],
            "gps_hdop_nonnegative":[],
            "service_key_range":[],
            "stop_direction_range":[],
            "route_id_range":[]}
 
    # Subscribe to topic
    consumer.subscribe([topic])

    #connect to db
    conn = dbconnect()

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                if len(cmdlist) > 0:
                  load(conn, cmdlist)
                  cmdlist.clear()
                
                #write to assertion failure file
                f = open(name, 'w')
                f.write(str(failures))
                f.close()

                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                # load kafka msg value into python dictionary
                data = json.loads(record_value)

                print("Consumed record with key {} and value"
                      .format(record_key, record_value))

                record_key = str(record_key)

                if record_key == "b'sensor-data'":
                  consume_bc(data)
                if record_key == "b'stop-data'":
                  consume_stop(data)

                skip = False

                if len(cmdlist) > 20:
                  load(conn, cmdlist)
                  cmdlist.clear()


    except KeyboardInterrupt:
        pass
    finally:

        # Leave group and commit final offsets
        consumer.close()
