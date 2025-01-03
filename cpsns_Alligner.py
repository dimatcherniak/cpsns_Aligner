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
import numpy as np
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import struct
import time
import argparse
import os
import sys
import json

CONFIG_FILE_DEFAULT = "config.json"

myDict = {}
bReadingMyDict = False
bWritingMyDict = False

json_config = {}

mqttc = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)


def on_connect(mqttc, userdata, flags, rc, properties=None):
    global json_config
    print("Connected with response code %s" % rc)
    for topic in json_config["MQTT"]["TopicsToSubscribe"]:
        print(f"Subscribing to the topic {topic}...")
        mqttc.subscribe(topic, qos=json_config["MQTT"]["QoS"])


def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print(f"Subscribed to {msg}")


def on_message(client, userdata, msg):
    global myDict, bReadingMyDict, bWritingMyDict
    topic = msg.topic
    substrings = topic.split('/')
    bIsMetadata = True
    if substrings[-1] == "data":
        bIsMetadata = False
    elif substrings[-1] == "metadata":
        bIsMetadata = True
    else:
        raise Exception("Unknown topic: " + substrings[-1])

    # Create a tuple made of the topic string without the last element (data/metadata)
    myKey = tuple(substrings[:-1])

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
            secAtAcqusitionStart = 0
            nsecAtAcqusitionStart = 0

            # Find the column where to write the data
            try:
                colInx = json_config["MQTT"]["TopicsToSubscribe"].index(topic)
            except ValueError:
                colInx = -1
                print(f"Weird: topic {topic} is not in the list of the TopicsToSubscribe in the config file! Ignored!", file=sys.stderr)
            
            if colInx != -1:
                #                0         1      2   3       4     5     6     7                     8                      9
                myDict[myKey] = [nSamples, cType, Fs, colInx, None, None, None, secAtAcqusitionStart, nsecAtAcqusitionStart, msg.payload]
    else:
        if myKey in myDict:
            # Parse the payload
            payload = msg.payload
            descriptorLength, metadataVer = struct.unpack_from('HH', payload)
            # how many samples and what's its type, float or double?
            cType = myDict[myKey][1]
            nSamples = myDict[myKey][0]
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

            if myDict[myKey][7] == 0:
                # initialize the beginning of the time axis
                myDict[myKey][7] = secFromEpoch
                myDict[myKey][8] = nanosec
                """ Will be using a global 2D array
                # allocate the array 
                if cType=='d':
                    myDict[myKey][5] = np.full(nSamples, np.nan, dtype=np.float64)
                else:
                    myDict[myKey][5] = np.full(nSamples, np.nan, dtype=np.float32)
                """

            # Compute the index where to copy the data
            print(f"{nSamples} samples at {secFromEpoch}:{nanosec} s.")

            # Compute the interval between the current timestamp and the timestamp at the start
            delta_sec = secFromEpoch - myDict[myKey][7]
            delta_nsec = nanosec - myDict[myKey][8]
            if delta_nsec < 0:
                delta_nsec += 1000000000
                delta_sec -= 1
            # convert to microsec
            delta_mjus = delta_sec * 1000000.0 + delta_nsec / 1000.0
            rowInx = round(delta_mjus * (myDict[myKey][2] / 1000000.0))
            print(f"delta = {delta_mjus} mjus. inx = {rowInx}")
            
        else:
            print("Waiting for the metadata...")
    
    bWritingMyDict = False


def main():
    global myDict, bReadingMyDict, bWritingMyDict
    global json_config
    global readBuffer, timeAxis

    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="""This program 
1. reads the JSON config file
a. MQTT:
- M: list of length M of topics to subscribe
- QoS
- ClientID
b. Time axis
- N: How many samples to collect
2. Subscribes to SEVERAL MQTT topics listed in the config file or (not recommended, not supported initially) to one topic using a wildcard 
3. Collects the payloads to form a binary data chunk N x M making sure the data are alligned (share the same time axis)
4. When ready, publishes the JSON string explaining the data
""")
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
    readBuffer = np.full((nSamplesToCollect, nChannelsToObserve), np.nan, dtype=np.float32)
    
    # Set username and password
    if json_config["MQTT"]["userId"] != "":
        mqttc.username_pw_set(json_config["MQTT"]["userId"], json_config["MQTT"]["password"])

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_subscribe = on_subscribe
    mqttc.connect(json_config["MQTT"]["host"], json_config["MQTT"]["port"], 60)

    mqttc.loop_start()

    while True:

        time.sleep(1)
        continue

        while bWritingMyDict:
            # make the thread sleep
            # print("Waiting for bWritingMyDict")
            time.sleep(0.01)
        bReadingMyDict = True    

        for key, val in myDict.items():
            if not val[5].empty():
                data = val[5].get()
                nPackSize = val[0]
                if val[6] is None:
                    Fs = val[2]
                    timeAxis = np.linspace(0, 3 * nPackSize / Fs, 3 * nPackSize)
                    line, = ax.plot(timeAxis, np.zeros(3 * nPackSize), label=str(key))
                    plt.legend(loc="upper left")
                    val[6] = line

                line = val[6]
                curData = line.get_ydata()
                curData = np.roll(curData, -nPackSize)
                curData[-nPackSize:] = data
                line.set_ydata(curData)
                #bNeedToRedraw = True # Dima 16-Dec

        if bNeedToRedraw:
            fig.canvas.draw()
            fig.canvas.flush_events()

        bReadingMyDict = False
        time.sleep(0.1) # Dima 16-Dec


if __name__ == "__main__":
    main()
