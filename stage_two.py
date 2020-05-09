import paho.mqtt.client as mqtt
import configparser
from pymongo import MongoClient

cfg = configparser.RawConfigParser()
cfg.read('settings.conf')
connection = MongoClient(cfg.get('MONGO', 'uri'))


#
# raw_db = connection['Raw']
# agg_db = connection['Agg']
# rmac_1 = raw_db["mac1"]
# rmac_2 = raw_db["mac2"]
# rmac_3 = raw_db["mac3"]
# amac_1 = agg_db["mac1"]
# amac_2 = agg_db["mac2"]
# amac_3 = agg_db["mac3"]


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected OK")


def on_log(client, userdata, level, buf):
    print("log: " + buf)


def on_message(client, userdata, msg):
    mesg = eval(msg.payload.decode("utf-8"))
    mid = mesg["data"][0]["machine_id"]
    mesg_type = mesg["message_type"]
    new_db = connection[mesg_type]
    new_machine = new_db[mid]
    new_machine.insert_one(mesg)


broker = cfg.get('MQTT', 'host')
port = int(cfg.get('MQTT', 'port'))
client = mqtt.Client("python-sub")
client.connect(broker, port, 60)
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log
print("connecting to broker", broker)
client.subscribe(cfg.get('MQTT', 'base_topic'))
print("subscribed")
client.loop_forever()
