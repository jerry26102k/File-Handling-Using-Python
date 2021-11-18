import os
import time
import shutil
from os import listdir
from os.path import isfile, join
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")  # client is a reference for mongoDb server.
db = client["filesDb"]  # creating database
file_collection = db["files"]  # adding collection to the database

parent_directory_path = os.getcwd()

processing_path = os.path.join(parent_directory_path, "Processing")
try:
    os.mkdir(processing_path)  # creating processing folder.

except OSError:
    print("Processing folder already present, therefore copy not created!")

queue_path = os.path.join(parent_directory_path, "Queue")
try:
    os.mkdir(queue_path)  # creating Queue folder.
except OSError:
    print("Queue folder already present, therefore copy not created!")

processed_path = os.path.join(parent_directory_path, "Processed")
try:
    os.mkdir(processed_path)  # creating Processed folder.
except OSError:
    print("Processed folder already present, therefore copy not created!")

# creating file in processing every second and at  every 5 second transferring all
# files from processing to queue and updating data.

i = 1
while True:

    file_name = str(int(time.time())) + ".txt"  # using system time as file name so that every file name is unique.
    dictionary = {"fileName": file_name, "isProcessed": 0}
    file_collection.insert_one(dictionary)

    with open(join(processing_path, file_name), 'w') as fp:
        pass
    time.sleep(1)

    if i % 5 == 0 and not (any(isfile(join(queue_path, i)) for i in listdir(queue_path))):
        # getting list of files in processing folder.
        file_list = listdir(processing_path)
        for file in file_list:
            shutil.move(processing_path + "/" + file, queue_path + "/" + file)  # Processing -> Queue

    if any(isfile(join(queue_path, i)) for i in listdir(queue_path)):  # checking if any file exists in queue folder.
        file_to_move = os.listdir(queue_path)[0]  # transferring the very first file to processed folder.
        file_collection.update_one(
            {"fileName": file_to_move},
            {
                "$set": {"isProcessed": 1}    # mongoDb query for updating isProcessed column if the file is moving to
                                              # processed folder
              }
        )
        shutil.move(queue_path + '/' + file_to_move, processed_path + '/' + file_to_move)  # Queue -> Processed
    i += 1
