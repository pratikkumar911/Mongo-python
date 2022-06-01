from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://pratik:{password}@tutorial.1j716.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client.test
collections = test_db.list_collection_names()
#print(collections)

def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name":"Pratik",
        "type":"Test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

#insert_test_doc()

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["a","b","c","d"]
    last_names = ["e","f","g","h"]
    ages = [21,22,23,24]
    docs =[]
    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name":first_name,
                "last_name":last_name,
                "age":age
              }
        docs.append(doc)
    person_collection.insert_many(docs)

#create_documents()

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()
    for person in people:
        printer.pprint(person)

#find_all_people()

def find_a():
    a = person_collection.find_one({"first_name":"a"})
    printer.pprint(a)

#find_a()

def count_all_people():
    count = person_collection.find().count()
    print("Number of people",count);

#count_all_people()

def get_person_by_id(person_id):
    from bson.objectid import ObjectId 

    pid = ObjectId(person_id)
    person = person_collection.find_one({"_id":pid})
    printer.pprint(person)

#get_person_by_id("629728b7c795765646839b30")

def find_age_in_range(min_age, max_age):
    query = {"$and":[
            {"age":{"$gte":min_age}},
            {"age":{"$lte":max_age}}
            ]}
    people = person_collection.find(query).sort("age");
    for person in people:
        printer.pprint(person)

#find_age_in_range(22, 24)

def project_columns():
    columns = {"_id":0,"first_name":1,"last_name":1}
    people = person_collection.find({},columns)
    for person in people:
        printer.pprint(person)

#project_columns()

def update_person(person_id):
    from bson.objectid import ObjectId

    pid = ObjectId(person_id)
    # all_updates = {
    #     "$set":{"new_field":True},
    #     "$inc":{"age":1},
    #     "$rename":{"first_name":"first","last_name":"last"}
    # }
    # person_collection.update_one({"_id":pid},all_updates)
    person_collection.update_one({"_id":pid},{"$unset":{"new_field":""}})

def replace_one(person_id):
    from bson.objectid import ObjectId

    pid = ObjectId(person_id)
    new_doc={
        "first_name":"new first name",
        "last_name":"new last name",
        "age":100
    }
    person_collection.replace_one({"_id":pid},new_doc)

#replace_one("629728b7c795765646839b31")

def delete_doc(person_id):
    from bson.objectid import ObjectId
    pid = ObjectId(person_id)
    person_collection.delete_one({"_id":pid})
#delete_doc("629728b7c795765646839b31")

#-----------------------------------------------------

address = {
    "id":"9283643q78rerh87t48",
    "street":"Bay Street",
    "number":2706,
    "city":"New York",
    "country":"United States",
    "zip":941325    
}

def add_address(person_id):
    from bson.objectid import ObjectId
    pid = ObjectId(person_id)

    person_collection.update_one({"_id":pid},{"$addToSet":{"address":address}})

add_address("629728b7c795765646839b32")