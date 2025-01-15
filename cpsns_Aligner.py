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
    #print(f"on_message: received {msg.topic}")
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
                    "NextIndex": 0, 
                    "ReadyToFlush": False,
                    "SecondsAtReadBufferStart": -1, 
                    "NanosecondsAtReadBufferStart": -1, 
                    "MetadataJsonAsByteObject": msg.payload,
                    "LastTimeAccessed": datetime.now(),
                    "IsDead": False,
                    "dataPayloads": {} # this to be a dictionary, with time stamp as a key and the data as np.array
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

            # Add an entry to the dictionary: the timestamp is the key
            myDict[myKey]["dataPayloads"][(secFromEpoch, nanosec)] = data
            print(f"{nSamples} samples at {secFromEpoch}:{nanosec} s.")

            # Initialize the global time axis. TODO: Do I need it? 
            if timeAxis["OriginSecFromEpoch"] == 0:
                timeAxis["OriginSecFromEpoch"] = secFromEpoch
                timeAxis["Nanosec"] = nanosec

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

    # define the common time axis. TODO: do I need it?
    timeAxis = {"OriginSecFromEpoch": 0, "Nanosec": 0, "Fs": 0}

    nSamplesToCollect = json_config["Output"]["SamplesToCollect"]
    nChannelsToObserve = len(json_config["MQTT"]["TopicsToSubscribe"])
    print(f"nSamplesToCollect={nSamplesToCollect}, nChannelsToObserve={nChannelsToObserve}")

    bWritingMyDict = False
    bReadingMyDict = False
    
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

        # Here it goes the logic to collect the data from the payloadData dictionary for each topic into arrayToDump
        # 1. Make a temporary time axis (set of keys)
        tAxis = set()
        for myKey in myDict:
            tAxis.update(set(myDict[myKey]["dataPayloads"].keys()))
        
        # and convert to a list to make it iterable
        tAxis = list(sorted(tAxis))
        print(f"----> tAxis length is {len(tAxis)}")

        jsonMetadata = None
        arrayToDump = None
        
        if len(tAxis) > 0:
            intrvl = [0]
            while len(intrvl) > 0:
                if intrvl[-1] >= len(tAxis):
                    break
                # collect the keys that fully match the int
                # assume all keys are in
                keysThatFullyMatchInterval = set(myDict.keys())
                for myKey in myDict:
                    for chnk in intrvl:
                        ts = tAxis[chnk]
                        if ts not in myDict[myKey]["dataPayloads"]:
                            keysThatFullyMatchInterval.discard(myKey) # discard() won't raise error if the key is not there

                if len(keysThatFullyMatchInterval) == 0:
                    intrvl = intrvl[1:] # remove the first element
                    continue
                
                # number of samples in the interval
                samplesInInterval = 0
                theKey = next(iter(keysThatFullyMatchInterval))
                for chnk in intrvl:                    
                    samplesInInterval += len(myDict[theKey]["dataPayloads"][tAxis[chnk]])

                # print(f"Number of samples in the interval: {samplesInInterval}")
                if samplesInInterval > nSamplesToCollect:
                    print(f"Number of samples has reached the required number: {nSamplesToCollect}. Channels to export: {keysThatFullyMatchInterval}")
                    # Form the arrayToDump
                    arrayToDump = np.full((samplesInInterval, len(keysThatFullyMatchInterval)), np.nan, dtype=np.float32)
                    # To what column of the array to write? I can only guarantee the alphabetical order!
                    # Sort the keysThatFullyMatchInterval
                    keysSortedList = sorted(list(keysThatFullyMatchInterval))
                    for inxClmn in range(len(keysSortedList)):
                        theKey = keysSortedList[inxClmn]
                        strt = 0
                        for chnk in intrvl:
                            lngs = len(myDict[theKey]["dataPayloads"][tAxis[chnk]])
                            arrayToDump[strt:strt+lngs, inxClmn] = myDict[theKey]["dataPayloads"][tAxis[chnk]]
                            strt += lngs

                    # Check for nans in the result
                    nan_indices = np.where(np.isnan(arrayToDump[:,0]))
                    if len(nan_indices[0]) != 0:
                        print(f"{len(nan_indices[0])} NaNs in the output array! Indicies: {nan_indices}", file=sys.stderr)

                    # Form the metadata
                    listMetadata = []            
                    for inxClmn in range(len(keysSortedList)):
                        theKey = keysSortedList[inxClmn]
                        # get the metadata string...
                        json_metadata = json.loads(myDict[theKey]["MetadataJsonAsByteObject"])
                        listMetadata.insert(inxClmn, json_metadata)
                    
                    jsonMetadata = json.dumps({
                        "TimeAxis": {
                            "StartSecFromEpoch": tAxis[intrvl[0]][0], 
                            "Nanosec": tAxis[intrvl[0]][1], 
                            "TimeAtCollectionStart": f'{datetime.utcfromtimestamp(tAxis[intrvl[0]][0]) + timedelta(microseconds=tAxis[intrvl[0]][1] / 1000)}',
                            "Fs": timeAxis["Fs"]},                
                        "Channels": listMetadata})
                    
                    # Generate the new topics
                    newTopic = replace_subtopics(next(iter(keysThatFullyMatchInterval)), json_config["Output"]["ModifySubtopics"])
                    newTopicMetadata = newTopic + "/metadata"
                    newTopicData = newTopic + "/data"

                    # Release the memory
                    print(" ====== Cleaning ======= ")
                    for myKey in myDict:
                        for chnk in intrvl:
                            del myDict[myKey]["dataPayloads"][tAxis[chnk]]
                    intrvl = [0]
                else:
                    intrvl.append(intrvl[-1]+1)

        bReadingMyDict = False

        if arrayToDump is not None:
            # Flushing to the destination
            if json_config["Output"]["Destination"] == "file":
                # ------- Dump to file ---------------
                print("Dumping to a file is not supported!", file=sys.stderr)
                sys.exit(1)
            elif json_config["Output"]["Destination"] == "mongodb":
                # ------- Dump to MongoDB ---------------
                print("Saving to MongoDB is not supported!", file=sys.stderr)
                sys.exit(1)
            elif json_config["Output"]["Destination"] == "MQTT":
                # ------- Dump to MQTT ---------------
                # Publish
                print(f"DEBUG: aray to dump dimensions: {arrayToDump.shape[0]} x {arrayToDump.shape[1]}")
                mqttc.publish(newTopicMetadata, jsonMetadata, qos=json_config["MQTT"]["QoS"])
                mqttc.publish(newTopicData, arrayToDump.tobytes(), qos=json_config["MQTT"]["QoS"])
                print(f"Topics {newTopicMetadata} and {newTopic} attempted to publish!")
            else:
                print(f"Error: Unknown destination: {json_config['Output']['Destination']}", file=sys.stderr)
                sys.exit(1)

        arrayToDump = None
        jsonMetadata = None


        time.sleep(3)
        continue

if __name__ == "__main__":
    main()