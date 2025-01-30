"""
This program will
1. read CP-SENS MQTT messages, both data and metadata
2. plot the data
"""
import matplotlib.pyplot as plt
import numpy as np
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import queue
import struct
import time
from datetime import datetime
import argparse
import json
import sys
import os
import hashlib
from enum import Enum
import get_blocks_of_zeros

CONFIG_FILE_DEFAULT = "config.json"
DEFAULT_WAITTIME_BEFORE_DECLARING_DEAD = 3000000 # microsec Note: it should be longer than the time needed to collect required samples!

myDict = {}
bReadingMyDict = False
bWritingMyDict = False
common_Fs = -1

json_config = {}
mqttc = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)

# Events
class DataStreamEvent(Enum):
    AllGood = 0
    NewChannelDiscovered = 1
    ChannelDisappeared = 2
    ChannelMetadataChanged = 3

currentEvent = DataStreamEvent.AllGood    

def on_publish(client, userdata, mid, arg1, arg2):
    print(f"Message {mid} published.")

def on_connect(mqttc, userdata, flags, rc, properties=None):
    global json_config
    print("Connected with response code %s" % rc)
    for topic in json_config["MQTT"]["TopicsToSubscribe"]:
        print(f"Subscribing to the topic {topic}...")
        mqttc.subscribe(topic, qos=json_config["MQTT"]["QoS"])

def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print("mid/response = " + str(msg) + " / " + str(granted_qos))

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

def on_message(client, userdata, msg):
    global json_config
    global myDict, bReadingMyDict, bWritingMyDict
    global common_Fs
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
            Fs = json_metadata["Analysis chain"][-1]["Sampling"] # take the sampling freq. from the last element of the analysis chain!
            # Only for metadata ver.>=2
            try:
                secAtAcqusitionStart = json_metadata["TimeAtAquisitionStart"]["Seconds"]
                nanosec = json_metadata["TimeAtAquisitionStart"]["Nanosec"]
            except Exception as e:
                print("Incompatible version. Use earlier version of cpsns_LifePlot!", file=sys.stderr)
                sys.exit(1)

            # Check the consistency of the Fs
            bIgnoreTopic = False
            if common_Fs == -1:
                common_Fs = Fs # the first wins
            elif common_Fs != Fs:
                print(f"Weird: Fs of topic {topic} is {Fs} Sa/s is different from the common Fs = {common_Fs} Sa/s! The topic is ignored!", file=sys.stderr)
                bIgnoreTopic = True

            if not bIgnoreTopic:
                myDict[myKey] = {
                    "SamplesInPayload": nSamples, 
                    "DataType": cType, 
                    "SampleRate": Fs, 
                    "MetadataHashTag": hash_bytes(msg.payload), 
                    "MetadataJsonAsByteObject": msg.payload,
                    "PayloadQueue": queue.Queue(), 
                    "SecAtAcqusiitionStart": secAtAcqusitionStart, 
                    "Nanosec": nanosec,
                    "SampleWhenEntryCreated": 0,
                    "IsAlive": None,
                    "LastUpdated": None,
                    "Data": None,
                }
                # New channel:
                currentEvent = DataStreamEvent.NewChannelDiscovered
                print(f"   ---> Event: {currentEvent}; key={myKey}")
        else:
            # check if metadata has changed
            if hash_bytes(msg.payload) != myDict[myKey]["MetadataHashTag"]:
                currentEvent = DataStreamEvent.ChannelMetadataChanged
                print(f"   ---> Event: {currentEvent}; key={myKey}")
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
                nSamples = round((payload_len-descriptorLength)/struct.calcsize(cType))
            # Data
            strBinFormat = str(nSamples) + str(cType)  # e.g., '640f' for 640 floats
            # data
            data = np.array(struct.unpack_from(strBinFormat, payload, descriptorLength))
            # time stamp of the payload
            secFromEpoch = struct.unpack_from('Q', payload, 4)[0]
            nanosec = struct.unpack_from('Q', payload, 4+8)[0]
            # nSamples
            nSamplesFromDAQStart = 0
            if metadataVer >= 2:
                nSamplesFromDAQStart = struct.unpack_from('Q', payload, 4+8+8)[0]
            else:
                raise Exception("Incompatible version. Use earlier version of cpsns_Aligner!")
            if myDict[myKey]["SampleWhenEntryCreated"] == 0:
                # First time
                myDict[myKey]["SampleWhenEntryCreated"] = nSamplesFromDAQStart

            myDict[myKey]["PayloadQueue"].put({"SampleFromDAQStart": nSamplesFromDAQStart, "PayloadData": data})
            myDict[myKey]["LastUpdated"] = datetime.now()
            myDict[myKey]["IsAlive"] = True
        else:
            print("Waiting for the metadata...")
    
    bWritingMyDict = False


def main():
    global json_config
    global myDict, bReadingMyDict, bWritingMyDict
    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="This program will align the data collected from MQTT messages. The parameters to be in the JSON configuration file.")
    parser.add_argument('--config', type=str, help='Specify the JSON configuration file. Defaults to ' + CONFIG_FILE_DEFAULT, default=CONFIG_FILE_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    # Name of the configuration file
    strConfigFile = args.config

    # Read the configuration file
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

    nSamplesToCollect = json_config["Output"]["SamplesToCollect"]

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

    # common time axis
    inxAxisCommon = None

    while True:
        time.sleep(0.5)

        bNeedToReset = False
        while bWritingMyDict:
            # make the thread sleep
            # print("Waiting for bWritingMyDict")
            time.sleep(0.01)
        bReadingMyDict = True  

        if myDict is None or not myDict:
            bReadingMyDict = False  
            continue
        
        # Define the global time axis
        if inxAxisCommon is None:
            # Find the smallest one
            sample_when_entry_created = np.iinfo(np.uint64).max
            secFromEpoch = None
            nanosec = None
            Fs = None
            for key, val in myDict.items():
                if val['SampleWhenEntryCreated'] < sample_when_entry_created:
                    Fs = val["SampleRate"]
                    sample_when_entry_created = val['SampleWhenEntryCreated']
                    secFromEpoch = val["SecAtAcqusiitionStart"]
                    nanosec = val["Nanosec"]

            inxAxisCommon = {
                "Fs": Fs, 
                "SecAtAcqusiitionStart": secFromEpoch,
                "Nanosec": nanosec,
                "AxisStartsAtSample": sample_when_entry_created, 
                "AxisLength": int(round(1.1 * nSamplesToCollect))
                }
            #print(f'AxisLength = {inxAxisCommon["AxisLength"]}')

        for key, val in myDict.items():
            if val["PayloadQueue"].empty():
                continue

            if val["Data"] is None:
                # allocate
                val["Data"] = np.full(inxAxisCommon["AxisLength"], np.nan, dtype=np.float32)

            while not val["PayloadQueue"].empty():
                # place the data where it is suposed to be
                payload = val["PayloadQueue"].get()
                inx = payload["SampleFromDAQStart"]
                inxToPlaceData = inx - inxAxisCommon["AxisStartsAtSample"]
                if inxToPlaceData<0:
                    ## ignore this payload...
                    print(f"{datetime.now()}: key={key}. Negative index: {inxToPlaceData}. The payload is ignored! . Resetting...", file=sys.stderr)  
                    #continue
                    # need to reset!
                    bNeedToReset = True
                    break

                data = payload["PayloadData"]
                if inxToPlaceData+len(data) < inxAxisCommon["AxisLength"]:
                    # okay
                    val["Data"][inxToPlaceData:inxToPlaceData+len(data)] = data
                else:
                    # need to "scroll"... by 
                    scrl = 1 + inxToPlaceData+len(data) - inxAxisCommon["AxisLength"]
                    print(f"key={key}. Scrolling by {scrl} elements")  
                    # scroll the time axis
                    inxAxisCommon["AxisStartsAtSample"] += scrl
                    if scrl < inxAxisCommon["AxisLength"]:
                        # scroll all other dataset
                        for kk in myDict:
                            if myDict[kk]["Data"] is None:
                                continue
                            myDict[kk]["Data"] = np.roll(myDict[kk]["Data"], -scrl)
                            if kk == key:
                                myDict[kk]["Data"][inxToPlaceData-scrl:inxToPlaceData+len(data)-scrl] = data
                            else:
                                myDict[kk]["Data"][-scrl:] = np.full(scrl, np.nan, dtype=np.float32)
                    else:
                        # need to reset!
                        print(f"{datetime.now()}: Scroll index {scrl} >= axis length {inxAxisCommon['AxisLength']}. Resetting...", file=sys.stderr)
                        bNeedToReset = True
                        break
            
            # Is the channel alive?
            time_interval_ms = (datetime.now()-val["LastUpdated"]).microseconds
            if time_interval_ms > DEFAULT_WAITTIME_BEFORE_DECLARING_DEAD:
                val["IsAlive"] = False
                print(f"{datetime.now()}: Channel {key} is declared dead. Resetting...", file=sys.stderr)
                bNeedToReset = True
        
        if not bNeedToReset:
            # Now all the topic entries are updated and aligned
            channel_cnt_arr = np.full(inxAxisCommon["AxisLength"],0)
            for key, val in myDict.items():
                if val["Data"] is not None:
                    channel_cnt_arr += np.where(np.isnan(val["Data"]), 0, 1)

            # Find the longest consequitive block where all channels are not nans
            channel_cnt_arr -= len(myDict)
            #print(f"{len(myDict)}. New arr: {channel_cnt_arr.shape} {channel_cnt_arr}")
            not_nan_blocks = get_blocks_of_zeros.find_zero_blocks(channel_cnt_arr)
            if not_nan_blocks:
                #print(f"Not nan blocks: {not_nan_blocks}")
                # find the longest block
                longest_block = max(not_nan_blocks, key=lambda x: x[1])
                if longest_block[1] >= nSamplesToCollect:
                    # prepare to export/publish
                    # 0. UTC time of the first sample in the collection
                    total_seconds = \
                        inxAxisCommon["SecAtAcqusiitionStart"] + \
                        inxAxisCommon["Nanosec"] / 1e9 + \
                        inxAxisCommon["AxisStartsAtSample"]/inxAxisCommon["Fs"]
                    utcAtFirstSample = datetime.utcfromtimestamp(total_seconds)

                    # 1. Form the arrayToDump
                    arrayToDump = np.full((nSamplesToCollect, len(myDict)), np.nan, dtype=np.float32)
                    # 2. To what column of the array to write? I can only guarantee the alphabetical order!
                    # Sort the keysThatFullyMatchInterval
                    keysSortedList = sorted(list(myDict.keys()))
                    for inx_column, key in enumerate(keysSortedList):
                        arrayToDump[:, inx_column] = myDict[key]["Data"][longest_block[0]:longest_block[0]+nSamplesToCollect]
                    
                    # Check for nans in the result
                    nan_indices = np.where(np.isnan(arrayToDump[:,0]))
                    if len(nan_indices[0]) != 0:
                        print(f"{len(nan_indices[0])} NaNs in the output array! Indicies: {nan_indices}", file=sys.stderr)
                    else:
                        print(f"\n{datetime.now()}: Array to dump is ready, no nans. Shape: {arrayToDump.shape}\n")

                    # scroll
                    scrl = longest_block[0]+nSamplesToCollect
                    #print(f"Scrolling everything by {scrl} elements")  
                    # scroll the time axis
                    inxAxisCommon["AxisStartsAtSample"] += scrl
                    # scroll all dataset
                    for kk in myDict:
                        if myDict[kk]["Data"] is None:
                            continue
                        myDict[kk]["Data"] = np.roll(myDict[kk]["Data"], -scrl)
                        myDict[kk]["Data"][-scrl:] = np.full(scrl, np.nan, dtype=np.float32)

                    # 3. Form the metadata
                    # Generate the new topics
                    newTopic = replace_subtopics(next(iter(keysSortedList)), json_config["Output"]["ModifySubtopics"])
                    newTopicMetadata = newTopic + "/metadata"
                    newTopicData = newTopic + "/data"

                    listMetadata = []            
                    for inx_column, key in enumerate(keysSortedList):
                        # get the metadata string...
                        json_metadata = json.loads(myDict[key]["MetadataJsonAsByteObject"])
                        listMetadata.insert(inx_column, json_metadata)
                    
                    jsonMetadata = json.dumps({
                        "DataChunk": {
                            "UTCAtFirstSample": f"{utcAtFirstSample}", 
                            "Fs": inxAxisCommon["Fs"]
                            },                
                        "Channels": listMetadata},indent=4)

                    # PUBLISH!
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
                        mqttc.publish(newTopicMetadata, jsonMetadata, qos=json_config["MQTT"]["QoS"])
                        mqttc.publish(newTopicData, arrayToDump.tobytes(), qos=json_config["MQTT"]["QoS"])
                        print(f"Topics {newTopicMetadata} and {newTopicData} attempted to publish!")
                    else:
                        print(f"Error: Unknown destination: {json_config['Output']['Destination']}", file=sys.stderr)
                        sys.exit(1)


        if bNeedToReset:
            print("Long break in the data detected. Resetting the plot", file=sys.stderr)
            bNeedToReset = False

            # reset the dictionary
            myDict = {}
            inxAxisCommon = None
            common_Fs = -1

        bReadingMyDict = False

if __name__ == "__main__":
    main()
