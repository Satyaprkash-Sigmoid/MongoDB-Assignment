from pymongo import MongoClient
from bson import ObjectId
import json
try:
    connection = MongoClient('localhost',27017)
except:
    print("Error in Connect")

db = connection['Mflix']  # Using database
collection = db['users']  # Using Collection
item_list = []
with open('../jsonFile/users.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            item_list.append(my_dict)

collection.insert_many(item_list)

print(collection.count_documents({}))