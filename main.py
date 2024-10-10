#imports google cloud stuff
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

#imports window/ui stuff
from tkinter import *
from tkinter.ttk import *
#imports time stuff
from socket import AF_INET, SOCK_DGRAM
import sys
import socket
import struct, time
#import randomizer function
import random

#this function pulls the current time from NTP server

def pullTime(host = "pool.ntp.org"):
        port = 123
        buf = 1024
        address = (host,port)
        msg = '\x1b' + 47 * '\0'
 
        # reference time (in seconds since 1900-01-01 00:00:00)
        TIME1970 = 2208988800 # 1970-01-01 00:00:00
 
        # connect to server
        client = socket.socket( AF_INET, SOCK_DGRAM)
        client.sendto(msg.encode('utf-8'), address)
        msg, address = client.recvfrom( buf )
 
        t = struct.unpack( "!12I", msg )[10]
        t -= TIME1970
        return time.ctime(t).replace("  "," ")

# this function sends messages to the topic

def sendMessages():
    # TODO(developer)
    project_id = "acquired-talent-433100-q6" 
    topic_id = "capstone"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    ourNumber = 2

    for n in range(1, ourNumber):
        data_str = f"Message from publisher"
        # Data must be a bytestring
        data = data_str.encode("utf-8")
        # Add two attributes, origin and username, to the message
        future = publisher.publish(
            topic_path, data, FirstName="python-sample", ItemId="gcp", Quantity="test", TransactionDateTime=pullTime(), TransactionNumber="random number"
        )
        print(future.result())

    print(f"Published message with custom attributes to {topic_path}.")

def pullMessages():

    # TODO(developer)
    project_id = "acquired-talent-433100-q6"
    subscription_id = "is_capstone_pull"
    # Number of seconds the subscriber should listen for messages
    timeout = 5.0

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message.data!r}.")
        if message.attributes:
            print("Attributes:")
            for key in message.attributes:
                value = message.attributes.get(key)
                print(f"{key}: {value}")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

# below is UI design


# creates a Tk() object
master = Tk()
 
# sets the geometry of main 
# root window
master.geometry("200x200")
 
 
# function to open a new window 
# on a button click
def openNewWindow():
     
    # Toplevel object which will 
    # be treated as a new window
    newWindow = Toplevel(master)
 
    # sets the title of the
    # Toplevel widget
    newWindow.title("New Window")
 
    # sets the geometry of toplevel
    newWindow.geometry("200x200")

     # A Label widget to show in toplevel
    Label(newWindow, 
          text ="Welcome to our project!").pack()
    
 
 
label = Label(master, 
              text ="Welcome to our project!")
 
label.pack(pady = 10)
 
# a button widget which will send messages on click

btn = Button(master, 
             text ="Send message", 
             command = sendMessages)
btn.pack(pady = 10)
 # a button widget which will open a new window on button click
btn = Button(master, 
             text ="Open New Window", 
             command = openNewWindow)
btn.pack(pady = 10) 
btn = Button(master, 
             text ="Pull messages", 
             command = pullMessages)
btn.pack(pady = 10)

#random name generation
first_name = { "James", "Michael","Robert","John","David","William","Richard","Joseph","Thomas","Christopher","Charles","Daniel","Matthew","Anthony","Mark","Donald","Steven","Andrew","Paul","Joshua","Kenneth","Kevin","Brian","Timothy","Ronald","George","Jason","Edward","Jeffrey","Ryan","Jacob","Nicholas","Gary","Eric","Jonathan","Stephen","Larry","Justin","Scott","Brandon","Benjamin","Samuel","Gregory","Alexander","Patrick","Frank","Raymond","Jack","Dennis","Jerry","Tyler","Aaron","Jose","Adam","Nathan","Henry","Zachary","Douglas","Peter","Kyle","Noah","Ethan","Jeremy","Christian","Walter","Keith","Austin","Roger","Terry","Sean","Gerald","Carl","Dylan","Harold","Jordan","Jesse","Bryan","Lawrence","Arthur","Gabriel","Bruce","Logan","Billy","Joe","Alan","Juan","Elijah","Willie","Albert",
"Wayne","Randy","Mason","Vincent","Liam","Roy","Bobby","Caleb","Bradley","Russell","Lucas",}
last_name = {
    "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzales", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Gomez"
}


# mainloop, runs infinitely
mainloop()