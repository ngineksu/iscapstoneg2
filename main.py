import datetime
from google.cloud import pubsub_v1
import socket
import struct
import time
import random
import threading
from tkinter import *
 
# Function to pull the current time from NTP server
def pullTime(host="pool.ntp.org"):
    port = 123
    buf = 1024
    address = (host, port)
    msg = '\x1b' + 47 * '\0'
    TIME1970 = 2208988800  # 1970-01-01 00:00:00
 
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client.sendto(msg.encode('utf-8'), address)
    msg, address = client.recvfrom(buf)
    t = struct.unpack("!12I", msg)[10]
    t -= TIME1970
    return time.ctime(t).replace("  ", " ")
 
# Function to send messages to the topic
def sendMessages():
    project_id = "acquired-talent-433100-q6"
    topic_id = "capstone"
 
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
 
    ourNumber = 2
 
    for n in range(1, ourNumber):
        data_str = "Message from publisher"
        data = data_str.encode("utf-8")
        attributes = {
            "FirstName": str(random.choice(first_name)),
            "LastName": str(random.choice(last_name)),
            "ItemId": str(random.randint(111111, 999999)),
            "Quantity": str(random.randint(1, 100)),
            "TransactionDateTime": pullTime(),
            "TransactionNumber": str(random.randint(11111111, 99999999))
        }
 
        future = publisher.publish(topic_path, data, **attributes)
        future.result()
 
        # Format the attributes for display
        attr_output = "\n".join(f"{key}: {value}" for key, value in attributes.items())
        text_widget.insert(END, f"Published message with attributes:\n{attr_output}\n\n")
        text_widget.see(END)  # Scroll to the end
 
# Continuous pull messages in a separate thread
def start_subscriber():
    project_id = "acquired-talent-433100-q6"
    subscription_id = "is_capstone_pull"
 
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
 
    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        attributes = message.attributes
        output = (
            f"Received message: ID={message.message_id} Data={message.data.decode('utf-8')}\n"
            f"Attributes:\n" + "\n".join(f"{key}: {value}" for key, value in attributes.items()) + "\n"
            f"[{datetime.datetime.now()}] Processing: {message.message_id}\n"
        )
        text_widget.insert(END, output)
        text_widget.see(END)  # Scroll to the end
 
        time.sleep(3)  # Simulate processing time
        text_widget.insert(END, f"[{datetime.datetime.now()}] Processed: {message.message_id}\n\n")
        text_widget.see(END)  # Scroll to the end
        message.ack()
 
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    text_widget.insert(END, f"Pulling messages from {subscription_path}...\n")
    text_widget.see(END)  # Scroll to the end
 
    with subscriber:
        try:
            streaming_pull_future.result()
        except Exception as e:
            text_widget.insert(END, f"Error: {e}\n")
            text_widget.see(END)  # Scroll to the end
 
# UI setup
master = Tk()
master.geometry("400x400")
 
# Text widget to display messages
text_widget = Text(master, wrap=WORD, height=15)
text_widget.pack(pady=10)
 
# Function to open a new window
def openNewWindow():
    newWindow = Toplevel(master)
    newWindow.title("New Window")
    newWindow.geometry("200x200")
    Label(newWindow, text="Welcome to our project!").pack()
 
label = Label(master, text="Welcome to our project!")
label.pack(pady=10)
 
# Buttons to interact with functions
send_button = Button(master, text="Send message", command=sendMessages)
send_button.pack(pady=5)
 
open_button = Button(master, text="Open New Window", command=openNewWindow)
open_button.pack(pady=5)
 
#random name generation
first_name = [ 'James', 'Michael','Robert','John','David','William','Richard','Joseph','Thomas','Christopher','Charles','Daniel','Matthew','Anthony','Mark','Donald','Steven','Andrew','Paul','Joshua','Kenneth','Kevin','Brian','Timothy','Ronald','George','Jason','Edward','Jeffrey','Ryan','Jacob','Nicholas','Gary','Eric','Jonathan','Stephen','Larry','Justin','Scott','Brandon','Benjamin','Samuel','Gregory','Alexander','Patrick','Frank','Raymond','Jack','Dennis','Jerry','Tyler','Aaron','Jose','Adam','Nathan','Henry','Zachary','Douglas','Peter','Kyle','Noah','Ethan','Jeremy','Christian','Walter','Keith','Austin','Roger','Terry','Sean','Gerald','Carl','Dylan','Harold','Jordan','Jesse','Bryan','Lawrence','Arthur','Gabriel','Bruce','Logan','Billy','Joe','Alan','Juan','Elijah','Willie','Albert','Wayne','Randy','Mason','Vincent','Liam','Roy','Bobby','Caleb','Bradley','Russell','Lucas', ]
last_name = [ 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzales', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell']
 
# Start the subscriber in a separate thread
threading.Thread(target=start_subscriber, daemon=True).start()