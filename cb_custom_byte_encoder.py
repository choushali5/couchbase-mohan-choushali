from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

from couchbase.transcoder import TranscoderPP
import couchbase
import pickle
import json
import time



cluster = Cluster('couchbase://localhost:8091')

authenticator = PasswordAuthenticator('Administrator', '123456')
cluster.authenticate(authenticator)
bucket = cluster.open_bucket('mohan')

class Person():
    def __init__(self):
        self.name = 'John'
        self.age = 30
        self.id = 501

    def insert_more(self,id,name):
        self.name= name
        self.age = 40
        self.id = id




person = Person()

string = "Name-"
list1 = []

ta = time.time_ns()
print("before for loop timstamp==>",ta)

for i in range(10000000):
    string += str(i)
    person.insert_more(i,string)
    #list1.insert(i,person.__dict__)
    list1.insert(i, person)
    string = "Name-"
tb= time.time_ns()
print("End of  for loop timstamp==>",tb)
print("time difference to populating list object is ==>",(tb-ta))


t1 = time.time_ns()
print("Before Upsert starting time stamp===>",t1)
print("==========>size of list object=======>",list1.__sizeof__(),type(list1))
print("==========>size of list object : pickle.dumps(list1)=======>",pickle.dumps(list1).__sizeof__(),type(pickle.dumps(list1)))
bucket.upsert("custom_transcoder_byte",pickle.dumps(list1),format=couchbase.FMT_BYTES)
t2 = time.time_ns()
print("After Upsert starting time stamp===>",t2)
print("Total time to upsert records into couchbase===>",(t2-t1))

print("")
print("==============================")
t3 = time.time_ns()

print("Before fetching get time for byte data: ",t3)

bucket.get("custom_transcoder_byte")
t4 = time.time_ns()
print("After Fectching get time stamp===>",t4)
print("Total time to get records from couchbase Byte data===>",(t4-t3))

encoded_value = bucket.get("custom_transcoder_byte")
#print("encoded value is : ",encoded_value.value)

print("---------Now Decoding Value--------")

decoded_value = pickle.loads(encoded_value.value)

#decoded_json_format = json.dumps(decoded_value)
t5 = time.time_ns()

print("Total time before fetching and after decoding in json object",(t5-t3))
