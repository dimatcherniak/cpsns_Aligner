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
import argparse
import json

HOST_DEFAULT = "dtl-server-2.st.lab.au.dk"
PORT_DEFAULT = 8090
USERNAME_DEFAULT = "hbk1"
PASSWORD_DEFAULT = "hbk1shffd"
MQTT_TOPIC_DEFAULT = "cpsens/+/+/1/acc/raw/+"  # "cpsens/+/+/1/+/+/data"

myDict = {}
bReadingMyDict = False
bWritingMyDict = False
strMQTTTopic = MQTT_TOPIC_DEFAULT


mqttc = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)


def on_connect(mqttc, userdata, flags, rc, properties=None):
    print("connected with response code %s" % rc)
    mqttc.subscribe(strMQTTTopic)


def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print("mid/response = " + str(msg) + " / " + str(granted_qos))


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
            Fs = json_metadata["Analysis chain"][0]["Sampling"]
            # Need to know: physical quantity, units
            strPhysQuantity = json_metadata["Analysis chain"][-1]["Output"]
            strUnits = json_metadata["Data"]["Unit"]
            secAtAcqusitionStart = 0
            usTimeStamp = 0
            #                0         1      2   3                4         5              6     7                     8
            myDict[myKey] = [nSamples, cType, Fs, strPhysQuantity, strUnits, queue.Queue(), None, secAtAcqusitionStart, usTimeStamp]
    else:
        if myKey in myDict:
            nSamples = myDict[myKey][0]
            cType = myDict[myKey][1]
            # Parse the payload
            payload = msg.payload
            descriptorLength, metadataVer = struct.unpack_from('HH', payload)
            strBinFormat = str(nSamples) + str(cType)  # e.g., '640f' for 640 floats
            # data
            data = np.array(struct.unpack_from(strBinFormat, payload, descriptorLength))
            # time stamp
            secFromEpoch = struct.unpack_from('Q', payload, 4)[0]
            nanosec = struct.unpack_from('Q', payload, 12)[0]

            if myDict[myKey][7] == 0:
                myDict[myKey][7] = secFromEpoch

            # Adjust to be inside the DOUBLE resolution
            secFromAcquisitionStart = secFromEpoch - myDict[myKey][7]
            # Time stamp is the time in microsec (us) from the data acqusition start
            usTimeStamp = secFromAcquisitionStart * 1e6 + nanosec * 1e-3  # e(-9+6)
            # Dima 18-Dec
            print(f"Current time: {usTimeStamp/1000000} s")
            # end Dima 18-Dec
            myDict[myKey][8] = usTimeStamp
            # Update the dictionary
            myDict[myKey][5].put(data)
        else:
            print("Waiting for the metadata...")
    
    bWritingMyDict = False


def main():
    global strMQTTTopic
    global myDict, bReadingMyDict, bWritingMyDict
    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="This Python script reads the time data from MQTT and outputs it on a life graph.")
    parser.add_argument('--host', type=str, help='Specify the host to connect to. Defaults to ' + HOST_DEFAULT, default=HOST_DEFAULT)
    parser.add_argument('--port', type=int, help='Connect to the port specified. Defaults to ' + str(PORT_DEFAULT), default=PORT_DEFAULT)
    parser.add_argument('--username', type=str, help='Provide a username to be used for authenticating with the broker. See also the --pw argument. Defaults to ' + USERNAME_DEFAULT, default=USERNAME_DEFAULT)
    parser.add_argument('--pw', type=str, help='Provide a password to be used for authenticating with the broker. See also the --username option. Defaults to ' + PASSWORD_DEFAULT, default=PASSWORD_DEFAULT)
    parser.add_argument('--topic', type=str, help='The topic parameter. Defaults to ' + MQTT_TOPIC_DEFAULT, default=MQTT_TOPIC_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    strMQTTTopic = args.topic

    # Set username and password
    mqttc.username_pw_set(args.username, args.pw)

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_subscribe = on_subscribe
    mqttc.connect(args.host, args.port, 60)

    mqttc.loop_start()

    # initiate the plt
    plt.ion()
    fig, ax = plt.subplots()
    # New: grid lines
    ax.grid(True)
    # Labels
    ax.set_xlabel('Time, s')

    last_redraw_time = time.time()
    while True:
        # Dima 18-Dec
        current_time = time.time()
        if current_time-last_redraw_time >= 0.1:
            bNeedToRedraw = True
            last_redraw_time=current_time
        else:
            bNeedToRedraw = False

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

    """
    # initiate the plt
    plt.ion()
    fig, ax = plt.subplots()
    timeAxis = np.linspace(0, 3 * nPackSize / Fs, 3 * nPackSize)
    line1, = ax.plot(timeAxis, np.zeros(3 * nPackSize))
    line2, = ax.plot(timeAxis, np.zeros(3 * nPackSize))
    # New: grid lines
    ax.grid(True)
    # Labels
    ax.set_xlabel('Time, s')
    ax.set_ylabel('Acceleration, m/s^2')
    # Limits
    plt.ylim(-2, 2)

    while True:
        bNeedToRedraw = False
        if not data_queue_ch1.empty():
            data = data_queue_ch1.get()
            # line1.set_xdata(np.linspace(timeStamp_ch1,timeStamp_ch1 + 1e6*nPackSize/Fs ,nPackSize)) # timeStamp_ch1 in us
            # current data
            curData = line1.get_ydata()
            curData = np.roll(curData, -nPackSize)
            curData[-nPackSize:] = data
            line1.set_ydata(curData)
            bNeedToRedraw = True
        if not data_queue_ch2.empty():
            data = data_queue_ch2.get()
            # line2.set_xdata(np.linspace(timeStamp_ch2,timeStamp_ch2 + 1e6*nPackSize/Fs ,nPackSize))
            curData = line2.get_ydata()
            curData = np.roll(curData, -nPackSize)
            curData[-nPackSize:]=data
            line2.set_ydata(curData)
            bNeedToRedraw = True
        if bNeedToRedraw:
            fig.canvas.draw()
            fig.canvas.flush_events()

        # Sleep for a short time to reduce CPU usage
        time.sleep(0.1)
        pass
    """

if __name__ == "__main__":
    main()
