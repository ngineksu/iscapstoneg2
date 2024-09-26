#imports google cloud stuff
from google.cloud import pubsub_v1
#imports window stuff
from tkinter import *
from tkinter.ttk import *
#imports time stuff
from socket import AF_INET, SOCK_DGRAM
import sys
import socket
import struct, time

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
        data_str = f"Message number {n}"
        # Data must be a bytestring
        data = data_str.encode("utf-8")
        # Add two attributes, origin and username, to the message
        future = publisher.publish(
            topic_path, data, Name="python-sample", ItemId="gcp", Quantity="test", TransactionDateTime=pullTime(), TransactionNumber="random number"
        )
        print(future.result())

    print(f"Published messages with custom attributes to {topic_path}.")

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
             text ="Send messages", 
             command = sendMessages)
btn.pack(pady = 10)
 # a button widget which will open a new window on button click
btn = Button(master, 
             text ="Open new Window", 
             command = openNewWindow)
btn.pack(pady = 10) 

# mainloop, runs infinitely
mainloop()