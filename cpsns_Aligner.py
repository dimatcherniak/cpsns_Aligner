"""
This program will
1. Read the JSON config file
a. MQTT:
- M: list of length M of topics to subscribe
- QoS
- ClientID
b. Time axis
- N: How many samples to collect
2. Subscribes to SEVERAL MQTT topics listed in the config file or (not recommended, not supported initially) to one topic using a wildcard 
3. Collects the payloads to form a binary data chunk N x M making sure the data are alligned (share the same time axis)
4. When ready, publishes the JSON string explaining the data
"""

"""
Idea (4-Jan-2025)
Stop storing the data and flush the already accumulated readbuffer in case of EVENTS:
EVENTS:
1. A new channel appear (if a new METADATA topic discovered)
2. A channel disappear (if there is no data for a period of time)
3. METADATA changes (the new metadata json string does not match the old one)
4. ...
"""

import numpy as np
import math
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import struct
import time
from datetime import datetime, timedelta
import argparse
import os
import sys
from enum import Enum
import json
import hashlib
from pymongo import MongoClient

CONFIG_FILE_DEFAULT = "config.json"

myDict = {}
bReadingMyDict = False
bWritingMyDict = False

json_config = {}

mqttc = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)

# Events
class DataStreamEvent(Enum):
    AllGood = 0
    NewChannelDiscovered = 1
    ChannelDisappeared = 2
    ChannelMetadataChanged = 3

currentEvent = DataStreamEvent.AllGood    

# Replaces the subtopics of the topic by the strings in the list
def replace_subtopics(topic, replacements):
    subtopics = topic.split('/')
    for i in range(min(len(subtopics), len(replacements))):
        if replacements[i]:
            subtopics[i] = replacements[i]
    return '/'.join(subtopics)

# Geherates a hshkey of a string, to make long string comparison faster
def hash_string(s):
    return hashlib.sha256(s.encode()).hexdigest()

def hash_bytes(b):
    return hashlib.sha256(b).digest()    

def on_publish(client, userdata, mid, arg1, arg2):
    print(f"Message {mid} published.")

def on_connect(mqttc, userdata, flags, rc, properties=None):
    global json_config
    print("Connected with response code %s" % rc)
    for topic in json_config["MQTT"]["TopicsToSubscribe"]:
        print(f"Subscribing to the topic {topic}...")
        mqttc.subscribe(topic, qos=json_config["MQTT"]["QoS"])

def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print(f"Subscribed to {msg}")

def on_message(client, userdata, msg):
    global json_config
    global myDict, bReadingMyDict, bWritingMyDict
    global timeAxis
    global currentEvent

    topic = msg.topic
    substrings = topic.split('/')
    bIsMetadata = True
    if substrings[-1] == "data":
        bIsMetadata = False
    elif substrings[-1] == "metadata":
        bIsMetadata = True
    else:
        raise Exception("Unknown topic: " + substrings[-1])

    myKey = '/'.join(substrings[:-1]) # Key: a string = the topic without "data" or "metadata"

    while bReadingMyDict:
        # make the thread sleep
        # print("Waiting for bReadingMyDict")
        time.sleep(0.01)
    bWritingMyDict = True

    if bIsMetadata:
        # Process JSON metadata
        # Add the key to the dictionary
        if myKey not in myDict:
            # Parse the payload
            json_metadata = json.loads(msg.payload)
            nSamples = json_metadata['Data']['Samples']
            cType = json_metadata['Data']['Type'][0]
            if cType != 'f' and cType != 'd':
                print(f"Unknow type: {cType}", file=sys.stderr)
                sys.exit(1)

            Fs = json_metadata["Analysis chain"][0]["Sampling"]

            # Check the consistency of the Fs
            bIgnoreTopic = False
            if timeAxis["Fs"] == 0:
                timeAxis["Fs"] = Fs # the first wins
            elif timeAxis["Fs"] != Fs:
                print(f"Weird: Fs of topic {topic} is {Fs} Sa/s is different from the common Fs = {timeAxis['Fs'] } Sa/s! The topic is ignored!", file=sys.stderr)
                bIgnoreTopic = True
            
            if not bIgnoreTopic:
                nSamplesToCollect = json_config["Output"]["SamplesToCollect"]
                myDict[myKey] = {
                    "SamplesInPayload": nSamples, 
                    "DataType": cType, 
                    "SampleRate": Fs, 
                    "MetadataHashTag": hash_bytes(msg.payload), 
                    "NextIndex": -1, 
                    "ReadyToFlush": False,
                    "SecondsAtReadBufferStart": -1, 
                    "NanosecondsAtReadBufferStart": -1, 
                    "MetadataJsonAsByteObject": msg.payload,
                    "LastTimeAccessed": datetime.now(),
                    "IsDead": False,
                    "readBuffer": np.full(round(2*nSamplesToCollect), np.nan, dtype=np.float32)
                    }
                # New channel:
                currentEvent = DataStreamEvent.NewChannelDiscovered
                print(f"   ---> Event: {currentEvent}")
        else:
            # check if metadata has changed
            if hash_bytes(msg.payload) != myDict[myKey]["MetadataHashTag"]:
                currentEvent = DataStreamEvent.ChannelMetadataChanged
                print(f"   ---> Event: {currentEvent}")

    else:
        if myKey in myDict:            
            # Parse the payload
            payload = msg.payload
            descriptorLength, metadataVer = struct.unpack_from('HH', payload)
            # how many samples and what's its type, float or double?
            cType = myDict[myKey]["DataType"]
            nSamples = myDict[myKey]["SamplesInPayload"]
            if nSamples == -1: # unknown or variable
                # calculate nSamples from the payload length
                payload_len = len(payload)
                nSamples = (payload_len-descriptorLength)/struct.calcsize(cType)

            strBinFormat = str(nSamples) + str(cType)  # e.g., '640f' for 640 floats
            # data
            data = np.array(struct.unpack_from(strBinFormat, payload, offset=descriptorLength))
            # time stamp
            secFromEpoch = struct.unpack_from('Q', payload, 4)[0]
            nanosec = struct.unpack_from('Q', payload, 12)[0]

            if myDict[myKey]["SecondsAtReadBufferStart"] == -1:
                # initialize the beginning of the topic-specific time axis
                myDict[myKey]["SecondsAtReadBufferStart"] = secFromEpoch
                myDict[myKey]["NanosecondsAtReadBufferStart"] = nanosec

            # Same for the global time axis
            if timeAxis["OriginSecFromEpoch"] == 0:
                timeAxis["OriginSecFromEpoch"] = secFromEpoch
                timeAxis["Nanosec"] = nanosec
                # TODO: Here is the nice spot to allocate the readBuffer, as we possess more information than before 

            # Compute the index where to copy the data
            print(f"{nSamples} samples at {secFromEpoch}:{nanosec} s.")

            # Compute the interval between the current timestamp and the timestamp at the start
            delta_sec = secFromEpoch - timeAxis["OriginSecFromEpoch"]
            delta_nsec = nanosec - timeAxis["Nanosec"]
            if delta_nsec < 0:
                delta_nsec += 1000000000
                delta_sec -= 1
            # convert to microsec
            delta_mjus = delta_sec * 1000000.0 + delta_nsec / 1000.0
            # finally, the inx
            rowInx = round(delta_mjus * (timeAxis["Fs"] / 1000000.0))
            print(f"delta = {delta_mjus} mjus. inx = {rowInx}")

            # check and write...
            bGoWrite = True
            if rowInx < 0:
                # a valid scenario: this is a delayed data chunk
                # TODO: we can consider reallocating everything to include this datachunk, if it is not superdelayed
                # for now, we just ignore it
                print(f"The delayed data from topic {msg.topic} is ignored! Debug info: rowInx = {rowInx}, delta = {delta_mjus} microsec.", file=sys.stderr)
                bGoWrite = False
            elif rowInx >= myDict[myKey]["readBuffer"].shape[0]:
                # readBuffer overrun scenario
                print(f"readBuffer overrun (1)!", file=sys.stderr)
                bGoWrite = False
            elif rowInx+nSamples >= myDict[myKey]["readBuffer"].shape[0]:
                # readBuffer overrun scenario
                print(f"readBuffer overrun (2)!", file=sys.stderr)
                bGoWrite = False
            else:
                # everything seems okay
                bGoWrite = True

            if bGoWrite:                
                myDict[myKey]["readBuffer"][rowInx:rowInx+nSamples] = data
                myDict[myKey]["NextIndex"] = rowInx+nSamples
                myDict[myKey]["LastTimeAccessed"] = datetime.now()

        else:
            print("Waiting for the metadata...")
    
    bWritingMyDict = False


def main():
    global myDict, bReadingMyDict, bWritingMyDict
    global json_config
    global timeAxis

    bDatabaseConnectionEstablished = False

    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="This program" 
                    "1. reads the JSON config file"
                    "a. MQTT:"
                    "- M: list of length M of topics to subscribe"
                    "- QoS"
                    "- ClientID"
                    "b. Time axis"
                    "- N: How many samples to collect"
                    "2. Subscribes to SEVERAL MQTT topics listed in the config file or (not recommended, not supported initially) to one topic using a wildcard "
                    "3. Collects the payloads to form a binary data chunk N x M making sure the data are alligned (share the same time axis)"
                    "4. When ready, publishes the JSON string explaining the data"
                    )
    parser.add_argument('--config', type=str, help='Specify the JSON configuration file. Defaults to ' + CONFIG_FILE_DEFAULT, default=CONFIG_FILE_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    # Name of the configuration file
    strConfigFile = args.config

    print(f"Reading configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # define the common time axis
    timeAxis = {"OriginSecFromEpoch": 0, "Nanosec": 0, "Fs": 0}

    # allocate the readbuffer 2D array (32-bit float, despite the input data type)
    nSamplesToCollect = json_config["Output"]["SamplesToCollect"]
    nChannelsToObserve = len(json_config["MQTT"]["TopicsToSubscribe"])
    print(f"nSamplesToCollect={nSamplesToCollect}, nChannelsToObserve={nChannelsToObserve}")
    # the readbuffer should be bigger than the output buffer
    # TODO: make a smart guess for how much bigger! Now hardcoded to 2
    #readBuffer = np.full((round(2*nSamplesToCollect), nChannelsToObserve), np.nan, dtype=np.float32)
    bWritingMyDict = False
    
    # MQTT stuff
    # Set username and password
    if json_config["MQTT"]["userId"] != "":
        mqttc.username_pw_set(json_config["MQTT"]["userId"], json_config["MQTT"]["password"])

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_subscribe = on_subscribe
    mqttc.on_publish = on_publish
    mqttc.connect(json_config["MQTT"]["host"], json_config["MQTT"]["port"], 60) # we subscribe to the topics in on_connect callback

    mqttc.loop_start()
    # MQTT done

    currentEvent = DataStreamEvent.AllGood

    fileCnt = 1
    verCnt = 0
    while True:

        # Check if the data needs to be flushed to the destination
        # If I have sufficient data in the readBuffer
        # Check this iterating myDict
        # First wait until it is available
        while bWritingMyDict:
            time.sleep(0.01)
        bReadingMyDict = True

        if currentEvent != DataStreamEvent.AllGood:
            print(f"======== Event {currentEvent} detected! Reset all!")
            myDict = {}
            timeAxis = {"OriginSecFromEpoch": 0, "Nanosec": 0, "Fs": 0}
            # to generate the file name
            fileCnt = 1
            verCnt += 1
            currentEvent = DataStreamEvent.AllGood
            
        for myKey in myDict:
            if myDict[myKey]["NextIndex"] > nSamplesToCollect:
                # this topic is ready
                myDict[myKey]["ReadyToFlush"] = True
            else:
                myDict[myKey]["ReadyToFlush"] = False
                # is the channel dead?
                timeSinceLastTimeAccessed = datetime.now()-myDict[myKey]["LastTimeAccessed"]
                if timeSinceLastTimeAccessed.total_seconds() > 0.5 * nSamplesToCollect / timeAxis["Fs"]: # not accessed since a half of the period
                    myDict[myKey]["IsDead"] = True
                    print(f"Channel {myKey} is declared DEAD: no data for the last {timeSinceLastTimeAccessed.total_seconds()} sec.", file=sys.stderr)
                else:
                    # give it a little time to try
                    myDict[myKey]["IsDead"] = False
        
        # Analyse the state and decide what to do...
        # Rules: 
        # Rule 1, Priority 1: at least one channel is dead --> flush ALL channels and raise ChannelDisapperared flag
        # Rule 2, Priority 2: no dead channels but not channels are ready to flush --> go next iteration
        # Rule 3, Priority 3: no dead channels, all channels are ready to flush --> flush
        # Rule 1
        bThereAreDeadChannels = False
        for myKey in myDict:
            if myDict[myKey]["IsDead"]:
                currentEvent = DataStreamEvent.ChannelDisappeared # this will restart at the next iteration
                bThereAreDeadChannels = True
                break
        # Rule 2
        bAllChannelsReady = True
        if not bThereAreDeadChannels:
            for myKey in myDict:
                if not myDict[myKey]["ReadyToFlush"]:
                    bAllChannelsReady = False
                    break
        
        if len(myDict) > 0 and (bThereAreDeadChannels or bAllChannelsReady): # see the rules above
            # What column of the array to write? I can only guarantee the alphabetical order!
            # Then in case if the measurement stopped and proceed again, it would be easier (though, not guarantied!) to continue
            keysList = sorted(list(myDict.keys()))

            # JSON describing the data file
            listMetadata = []            
            for myKey in myDict:
                # get the metadata string...
                json_metadata = json.loads(myDict[myKey]["MetadataJsonAsByteObject"])
                listMetadata.insert(keysList.index(myKey), json_metadata)
            
            # Array to be flushed:
            arrayToDump = np.full((nSamplesToCollect, len(myDict)), np.nan, dtype=np.float32)
            bTimeAxisUpdated = False
            for myKey in myDict:
                nSamplesToMove = myDict[myKey]["readBuffer"].shape[0]-nSamplesToCollect
                arrayToDump[:,keysList.index(myKey)] = myDict[myKey]["readBuffer"][0:nSamplesToMove]
                # Reshaffle the buffer
                myDict[myKey]["readBuffer"][0:nSamplesToMove] = myDict[myKey]["readBuffer"][nSamplesToMove:] # memcpy
                myDict[myKey]["readBuffer"][nSamplesToMove:].fill(np.nan)            
                # Change the global time axis origin
                if not bTimeAxisUpdated:
                    timeIntervalInSec = nSamplesToMove / timeAxis["Fs"]
                    timeIntervalinWholeSec = math.floor(timeIntervalInSec)
                    timeIntervalNanosec = round(1000000000 * (timeIntervalInSec-timeIntervalinWholeSec))
                    timeAxis["OriginSecFromEpoch"] += timeIntervalinWholeSec
                    timeAxis["Nanosec"] += timeIntervalNanosec
                    if timeAxis["Nanosec"] >= 1000000000:
                        timeAxis["Nanosec"] -= 1000000000        
                        timeAxis["OriginSecFromEpoch"] += 1
                    bTimeAxisUpdated = True

                # reset sample counter
                myDict[myKey]["NextIndex"] = 0
                
            # Flushing to the destination
            if json_config["Output"]["Destination"] == "file":
                # ------- Dump to file ---------------
                # TODO: depending of the destination, chose the right file format
                # 1. Saving the description as JSON
                jsonFileDecription = {
                    "TimeAxis": {
                        "StartSecFromEpoch": timeAxis["OriginSecFromEpoch"], 
                        "Nanosec": timeAxis["Nanosec"],
                        "TimeAtCollectionStart": f'{datetime.utcfromtimestamp(timeAxis["OriginSecFromEpoch"]) + timedelta(microseconds=timeAxis["Nanosec"] / 1000)}',
                        "Fs": timeAxis["Fs"]},                
                    "Channels": listMetadata}
                with open(f'{json_config["Output"]["DestinationFolder"]}/{json_config["Output"]["DestinationFileName"]}_{1000*verCnt+fileCnt}.json', 'w') as json_descr_file:
                    json.dump(jsonFileDecription, json_descr_file, indent=4)
                # 2. Saving the data as numpy array in binary form
                np.save(f'{json_config["Output"]["DestinationFolder"]}/{json_config["Output"]["DestinationFileName"]}_{1000*verCnt+fileCnt}.npy', arrayToDump)
                fileCnt += 1
                print("File saved!")
            elif json_config["Output"]["Destination"] == "mongodb":
                # ------- Dump to MongoDB ---------------
                if not bDatabaseConnectionEstablished:
                    bDatabaseConnectionEstablished = True
                    client = MongoClient(json_config["Output"]["DatabaseConnection"])
                    db = client[json_config["Output"]["DatabaseName"]]
                    collection = db[json_config["Output"]["DatabaseCollection"]]
                    # TODO: if the version changed, start saving to the new collection                
                # insert the data
                data = {
                    "TimeAxis": {
                        "StartSecFromEpoch": timeAxis["OriginSecFromEpoch"], 
                        "Nanosec": timeAxis["Nanosec"],
                        "TimeAtCollectionStart": datetime.utcfromtimestamp(timeAxis["OriginSecFromEpoch"]) + timedelta(microseconds=timeAxis["Nanosec"] / 1000),
                        "Fs": timeAxis["Fs"]},                
                    "Channels": listMetadata,
                    "Data": (arrayToDump.T).tolist()
                }
                collection.insert_one(data)
                print("Collection inserted!")
            elif json_config["Output"]["Destination"] == "MQTT":
                # ------- Dump to MQTT ---------------
                # Metadata
                jsonMetadata = json.dumps({
                    "TimeAxis": {
                        "StartSecFromEpoch": timeAxis["OriginSecFromEpoch"], 
                        "Nanosec": timeAxis["Nanosec"],
                        "TimeAtCollectionStart": f'{datetime.utcfromtimestamp(timeAxis["OriginSecFromEpoch"]) + timedelta(microseconds=timeAxis["Nanosec"] / 1000)}',
                        "Fs": timeAxis["Fs"]},                
                    "Channels": listMetadata}, indent=4)
                # Generate the new topic
                newTopic = replace_subtopics(next(iter(myDict)), json_config["Output"]["ModifySubtopics"])
                newTopicMetadata = newTopic + "/metadata"
                newTopicData = newTopic + "/data"
                # Publish
                mqttc.publish(newTopicMetadata, jsonMetadata, qos=json_config["MQTT"]["QoS"])
                mqttc.publish(newTopicData, arrayToDump.tobytes(), qos=json_config["MQTT"]["QoS"])
                print(f"Topics {newTopicMetadata} and {newTopic} attempted to publish!")
            else:
                print(f"Error: Unknown destination: {json_config['Output']['Destination']}", file=sys.stderr)
                sys.exit(1)

        bReadingMyDict = False

        time.sleep(0.1)
        continue

if __name__ == "__main__":
    main()