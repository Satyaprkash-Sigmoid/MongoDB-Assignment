from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import json
try:
    connection = MongoClient('localhost',27017)
except:
    print("Error in Connect")

db = connection['Mflix']  # to use database
collection = db['comments']  # to use collection
item_list = []
with open('../jsonFile/comments.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)  # Converting JSON to Python to
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            my_dict["date"]= my_dict["date"]["$date"]["$numberLong"]
            item_list.append(my_dict)
collection.insert_many(item_list)  # inserting documents to comments

print(collection.count_documents({}))
