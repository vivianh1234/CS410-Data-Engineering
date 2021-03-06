import urllib.request
import json
import datetime as dt
import pytz
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib

timezone = pytz.timezone("US/Pacific")
date = dt.datetime.today()
today = timezone.localize(date)

data = urllib.request.urlopen("http://rbi.ddns.net/getBreadCrumbData")
name = "BreadCrumbData{today}.json".format(today = today)
name = name[:24] + ".json"

#parsing JSON data
raw = data.read()

breadcrumbs = json.loads(raw)
#print(breadcrumbs[0])

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

    for n in breadcrumbs:
        record_key = "sensor-data"
        record_value = json.dumps(n)
        #print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
    

    print("{} messages were produced to topic {}!".format(delivered_records, topic))

#write to file
f = open(name, 'wb')
f.write(raw)
f.close()
