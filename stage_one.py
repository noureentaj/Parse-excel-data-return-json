import pprint as pp
import paho.mqtt.client as mqtt
import configparser
import time

cfg = configparser.RawConfigParser()
cfg.read('settings.conf')

def publish(data):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected OK")

    broker = cfg.get('MQTT', 'host')
    port_c = int(cfg.get('MQTT', 'port'))
    client = mqtt.Client("python-pub")
    client.connect(broker, port_c, 60)
    client.on_connect = on_connect
    client.loop_start()
    print("connecting to broker", broker)
    client.publish(cfg.get('MQTT', 'base_topic'), data)
    print("published",data)
    client.loop_stop()


def read_sheet(df, mid):
    raw_data = {}
    data = {}
    columns = [x for x in df]
    parameters = columns[2:]
    for each_record in range(0, len(df)):
        for each_topic in parameters:
            data["parameter_name"] = each_topic
            data["value"] = df[each_topic][each_record]
            data["machine_id"] = mid
            data["user"] = "Noureen"
            raw_data["timestamp"] = df["Timestamp"][each_record].strftime("%H:%M:%S")
            raw_data["message_type"] = "rrraw"
            raw_data["data"] = [data]
            publish(str(raw_data))



def cal_agg(df, mid):
    columns = [x for x in df]
    parameters = columns[2:]
    agg = {}
    for each_topic in parameters:
        for aggfun in ('sum', 'max', 'min', 'avg'):
            agg["agg"] = aggfun
            agg["message_type"] = "aggg"
            agg_data = {}
            agg_data["parameter_name"] = each_topic
            if aggfun == 'sum':
                agg_data["value"] = df[each_topic].sum()
            elif aggfun == 'max':
                agg_data["value"] = df[each_topic].max()
            elif aggfun == 'min':
                agg_data["value"] = df[each_topic].min()
            elif aggfun == 'avg':
                agg_data["value"] = df[each_topic].sum() / len(df[each_topic])
            agg_data["machine_id"] = mid
            agg_data["user"] = "Noureen"
            agg["data"] = [agg_data]
            print("Trying to publish")
            publish(str(agg))
            print("Published")
