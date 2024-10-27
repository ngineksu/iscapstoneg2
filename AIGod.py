import datetime
import sqlite3
from google.cloud import pubsub_v1
import socket
import struct
import time
import random
import threading
from tkinter import *
from tkinter import ttk

#makes the database if it doesn't already exist
def setup_database():
    conn = sqlite3.connect('messages.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            message_id TEXT,
            data TEXT,
            first_name TEXT,
            last_name TEXT,
            item_id TEXT,
            quantity TEXT,
            transaction_date_time TEXT,
            transaction_number TEXT,
            is_duplicate INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    return conn

#function to pull the current time from NTP server
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

#names for the first/last name attributes
first_name = ['James', 'Michael', 'Robert', 'John', 'David', 'William', 'Richard', 'Joseph', 'Thomas', 'Christopher', 'Charles', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Andrew', 'Paul', 'Joshua', 'Kenneth', 'Kevin', 'Brian', 'Timothy', 'Ronald', 'George', 'Jason', 'Edward', 'Jeffrey', 'Ryan', 'Jacob', 'Nicholas', 'Gary', 'Eric', 'Jonathan', 'Stephen', 'Larry', 'Justin', 'Scott', 'Brandon', 'Benjamin', 'Samuel', 'Gregory', 'Alexander', 'Patrick', 'Frank', 'Raymond', 'Jack', 'Dennis', 'Jerry', 'Tyler', 'Aaron', 'Jose', 'Adam', 'Nathan', 'Henry', 'Zachary', 'Douglas', 'Peter', 'Kyle', 'Noah', 'Ethan', 'Jeremy', 'Christian', 'Walter', 'Keith', 'Austin', 'Roger', 'Terry', 'Sean', 'Gerald', 'Carl', 'Dylan', 'Harold', 'Jordan', 'Jesse', 'Bryan', 'Lawrence', 'Arthur', 'Gabriel', 'Bruce', 'Logan', 'Billy', 'Joe', 'Alan', 'Juan', 'Elijah', 'Willie', 'Albert', 'Wayne', 'Randy', 'Mason', 'Vincent', 'Liam', 'Roy', 'Bobby', 'Caleb', 'Bradley', 'Russell', 'Lucas']
last_name = ['Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzales', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell']

#function to send a message to the topic
def sendMessages():
    project_id = "acquired-talent-433100-q6"
    topic_id = "capstone"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    ourNumber = 2

    for n in range(1, ourNumber):
        data_str = input_message
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

        #makes attributes appear in initial window
        attr_output = "\n".join(f"{key}: {value}" for key, value in attributes.items())
        text_widget.insert(END, f"Published message with attributes:\n{attr_output}\n\n")
        text_widget.see(END)

#continuously pulls messages from the subscriber
def start_subscriber(conn):
    project_id = "acquired-talent-433100-q6"
    subscription_id = "is_capstone_pull"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        attributes = message.attributes
        data = message.data.decode('utf-8')

        # Insert into database
        cursor = conn.cursor()

        # Check for duplicates
        cursor.execute('SELECT COUNT(*) FROM messages WHERE message_id = ?', (message.message_id,))
        count = cursor.fetchone()[0]
        is_duplicate = 1 if count > 0 else 0

        cursor.execute('''
            INSERT INTO messages (message_id, data, first_name, last_name, item_id, quantity, transaction_date_time, transaction_number, is_duplicate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            message.message_id,
            data,
            attributes.get('FirstName', ''),
            attributes.get('LastName', ''),
            attributes.get('ItemId', ''),
            attributes.get('Quantity', ''),
            attributes.get('TransactionDateTime', ''),
            attributes.get('TransactionNumber', ''),
            is_duplicate
        ))
        conn.commit()

        #make each message appear with attributes on their own lines in the view window
        output = (
            f"Received message: ID={message.message_id} Data={data}\n"
            f"Attributes:\n" + "\n".join(f"{key}: {value}" for key, value in attributes.items()) + "\n"
            f"[{datetime.datetime.now()}] Processing: {message.message_id}\n"
        )
        text_widget.insert(END, output)
        text_widget.see(END)
        #processing time, otherwise messages would overlap each other when button is spammed
        time.sleep(3) 
        text_widget.insert(END, f"[{datetime.datetime.now()}] Processed: {message.message_id}\n\n")
        text_widget.see(END) 
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    text_widget.insert(END, f"Pulling messages from {subscription_path}...\n")
    text_widget.see(END) 

    with subscriber:
        try:
            streaming_pull_future.result()
        except Exception as e:
            text_widget.insert(END, f"Error: {e}\n")
            text_widget.see(END)

#function to create the database window
def open_database_window(conn):
    db_window = Toplevel(master)
    db_window.title("Database Records")
    db_window.geometry("1920x1080")

    #creating the table with a column for each attribute
    tree = ttk.Treeview(db_window, columns=("ID", "Message ID", "Data", "First Name", "Last Name", "Item ID", "Quantity", "Transaction Date Time", "Transaction Number", "Duplicate"), show='headings')
    for col in tree["columns"]:
        tree.heading(col, text=col)
    tree.pack(expand=True, fill='both')

    #variables to keep track of what the user is searching/sorting
    current_sort_column = "id"
    search_term = ""
    sort_order = True

    #function to refresh the table and check for duplicates each refresh
    def refresh_table():
        for row in tree.get_children():
            tree.delete(row)

        cursor = conn.cursor()
        if search_term:
            cursor.execute("SELECT * FROM messages WHERE last_name LIKE ?", ('%' + search_term + '%',))
        else:
            if current_sort_column == "quantity":
                order = "ASC" if sort_order else "DESC"
                cursor.execute(f"SELECT * FROM messages ORDER BY CAST(quantity AS INTEGER) {order}")
            else:
                cursor.execute(f"SELECT * FROM messages ORDER BY {current_sort_column}")

        for row in cursor.fetchall():
            is_duplicate_str = "Yes" if row[-1] == 1 else "No"
            tree.insert("", "end", values=row[:-1] + (is_duplicate_str,))

        #keeps refreshing the table every 2 seconds
        db_window.after(2000, refresh_table)

    #function that sorts records by ID
    def sort_by_id():
        nonlocal current_sort_column, search_term
        current_sort_column = "id"
        search_term = "" 
        refresh_table()

    #function that sorts records by Quantity
    def sort_by_quantity():
        nonlocal current_sort_column, search_term, sort_order
        current_sort_column = "quantity"
        search_term = ""
        sort_order = not sort_order 
        refresh_table()

    #function to filter records by last name
    def search_by_last_name():
        nonlocal search_term
        search_term = search_var.get().strip().lower()
        refresh_table() 

    #buttons to sort by id, quantity, and the search box and button for last name
    sort_id_button = Button(db_window, text="Sort by ID", command=sort_by_id)
    sort_id_button.pack(side=LEFT, padx=5, pady=5)

    sort_quantity_button = Button(db_window, text="Sort by Quantity", command=sort_by_quantity)
    sort_quantity_button.pack(side=LEFT, padx=5, pady=5)

    search_var = StringVar()
    search_entry = Entry(db_window, textvariable=search_var, width=20)
    search_entry.pack(side=LEFT, padx=5, pady=5)
    search_entry.insert(0, "Search by Last Name")

    search_button = Button(db_window, text="Search", command=search_by_last_name)
    search_button.pack(side=LEFT, padx=5, pady=5)

    refresh_table()

#function to adjust the input message for the send function
def update_input_message():
    global input_message
    input_message = input_entry.get()
    text_widget.insert(END, f"Input message updated to: {input_message}\n")
    text_widget.see(END)
    input_entry.delete(0, END)

#opening the send and view window
master = Tk()
master.title("Send and View Messages")
master.geometry("400x400")

input_message = "User has not input a message to send."

text_widget = Text(master, wrap=WORD, height=15)
text_widget.pack(pady=10)

#open the database upon startup
open_database_window(setup_database())

label = Label(master, text="Welcome to our project!")
label.pack(pady=10)

#entry widget for user input
input_entry = Entry(master, width=40)
input_entry.pack(pady=5)

#button to change input message
update_button = Button(master, text="Update Message", command=update_input_message)
update_button.pack(pady=5)

#send message
send_button = Button(master, text="Send message", command=sendMessages)
send_button.pack(pady=5)

#start the continuous pull
db_connection = setup_database()
threading.Thread(target=start_subscriber, args=(db_connection,), daemon=True).start()


mainloop()