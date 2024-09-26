from google.cloud import pubsub_v1
from tkinter import *
from tkinter.ttk import *


def sendMessages():
    # TODO(developer)
    project_id = "acquired-talent-433100-q6" 
    topic_id = "capstone"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    for n in range(1, 10):
        data_str = f"Message number {n}"
        # Data must be a bytestring
        data = data_str.encode("utf-8")
        # Add two attributes, origin and username, to the message
        future = publisher.publish(
            topic_path, data, origin="python-sample", username="gcp"
        )
        print(future.result())

    print(f"Published messages with custom attributes to {topic_path}.")

# below this line is copypasted from tutorial


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
 
# a button widget which will open a 
# new window on button click
btn = Button(master, 
             text ="Send messages", 
             command = sendMessages)
btn.pack(pady = 10)
 
# mainloop, runs infinitely
mainloop()