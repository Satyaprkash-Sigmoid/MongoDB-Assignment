from pymongo import MongoClient
from decimal import Decimal
from bson import ObjectId
import json

try:
    connection = MongoClient('localhost',27017)
except:
    print("Error in Connect")

db = connection['Mflix']  # Using Database
collection = db['theaters']  # Using Collection
item_list = []
with open('../jsonFile/theaters.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            my_dict["location"]["geo"]["coordinates"][0] = float(my_dict["location"]["geo"]["coordinates"][0]["$numberDouble"])
            my_dict["location"]["geo"]["coordinates"][1] = float(my_dict["location"]["geo"]["coordinates"][1]["$numberDouble"])
            item_list.append(my_dict)

collection.insert_many(item_list)

print(collection.count_documents({}))
