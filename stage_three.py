from flask import *
import pymongo
import configparser
from pymongo import MongoClient
import json

app = Flask(__name__)
cfg = configparser.RawConfigParser()
cfg.read('settings.conf')
connection = MongoClient(cfg.get('MONGO', 'uri'))


@app.route('/makeithappen', methods=['POST'])
def make():
    query = request.get_json()
    time = query["timestamp"]
    parameter = query["parameter_name"]
    machine_id = query["machine_id"]
    r_collection = connection.rrraw[machine_id]
    rdoc = r_collection.find({"timestamp": time,
                              "data.machine_id": machine_id,
                              "data.parameter_name": parameter},
                             {
                                 "_id": 0})  # to remove _id from mongo reponse. Because he is creepy and will give errors. You can specify what all to ignore in the second json.
    a_collection = connection.aggg[machine_id]
    adoc = a_collection.find({"timestamp": time,
                              "data.machine_id": machine_id,
                              "data.parameter_name": parameter})
    r = list(rdoc)

    return jsonify(r)


app.run(host=cfg.get('SERVICE', 'endpoint'), port=cfg.get('SERVICE', 'port'), debug=True)
