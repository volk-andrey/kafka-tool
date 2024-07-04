import PySimpleGUI as sg
from kafka.admin import KafkaAdminClient, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType
from kafka.errors import KafkaError
from kafka import KafkaProducer, KafkaConsumer

bootstrap_servers = ""
connected_to_kafka = False
producer = None
consumer = None
topic_name = ""


def add_acl_rule(window, values):
    bootstrap_servers = values["bootstrap_servers"]
    sasl_mechanism = values["sasl_mechanism"]
    security_protocol = values["security_protocol"]
    sasl_plain_username = values["username"]
    sasl_plain_password = values["password"]
    if not connected_to_kafka:
        return

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers,
                                    security_protocol=security_protocol,
                                    sasl_mechanism=sasl_mechanism,
                                    sasl_plain_username=sasl_plain_username,
                                    sasl_plain_password=sasl_plain_password)

    resource_pattern = ResourcePattern(ResourceType.TOPIC, values["topic_name"], AclPermissionType.ALLOW)
    acl = NewAcl(principal=values["username"], host="*", operation=AclOperation.READ, permission_type=AclPermissionType.ALLOW, resource_pattern=resource_pattern)

    try:
        admin_client.create_acl(acl)
        window["acl_status"].update("ACL Rule added successfully", text_color='green')
    except KafkaError:
        window["acl_status"].update("Failed to add ACL Rule", text_color='red')

def connect_to_kafka(window, values):
    global bootstrap_servers, connected_to_kafka, producer, consumer
    bootstrap_servers = values["bootstrap_servers"]
    sasl_mechanism = values["sasl_mechanism"]
    security_protocol = values["security_protocol"]
    sasl_plain_username = values["username"]
    sasl_plain_password = values["password"]
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                 security_protocol=security_protocol,
                                 sasl_mechanism=sasl_mechanism,
                                 sasl_plain_username=sasl_plain_username,
                                 sasl_plain_password=sasl_plain_password)
        consumer = KafkaConsumer(values["topic_name"], bootstrap_servers=bootstrap_servers, group_id='test_group',
                                 security_protocol=security_protocol,
                                 sasl_mechanism=sasl_mechanism,
                                 sasl_plain_username=sasl_plain_username,
                                 sasl_plain_password=sasl_plain_password)
        connected_to_kafka = True
        window["connection_status"].update("Connected to Kafka", text_color='green')
    except KafkaError:
        window["connection_status"].update("Connection to Kafka failed", text_color='red')

def send_message(window, values):
    global producer, topic_name
    if not connected_to_kafka:
        return
    message = values["message"]
    topic_name = values["topic_name"]
    producer.send(topic_name, message.encode('utf-8'))

def read_messages(window):
    global consumer
    if not connected_to_kafka:
        return
    for message in consumer:
        window["text"].print(message.value.decode('utf-8') + "\n")

layout = [
    [sg.Text("Enter bootstrap servers:"), sg.InputText(key="bootstrap_servers")],
    [sg.Text("Select security protocol:"), sg.Combo(['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'], key='security_protocol')],
    [sg.Text("Select sasl_mechanism:"), sg.Combo(['PLAIN', 'GSSAPI', 'OAUTHBEARER', 'SCRAM-SHA-256', 'SCRAM-SHA-512'], key='sasl_mechanism')],
    [sg.Text("Enter username:"), sg.InputText(key="username")],
    [sg.Text("Enter password:"), sg.InputText(key="password", password_char='*')],
    [sg.Button("Connect to Kafka")],
    [sg.Text("Enter message:"), sg.InputText(key="message")],
    [sg.Text("Enter topic name:"), sg.InputText(key="topic_name")],
    [sg.Button("Send Message")],
    [sg.Multiline(size=(50,10), key="text")],
    [sg.Button("Read Messages")],
    [sg.Text("", key="connection_status")],
    [sg.Button("Add ACL Rule")],
    [sg.Text("", key="acl_status")]

]

window = sg.Window("Kafka Tool", layout)

while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED:
        break
    if event == "Connect to Kafka":
        connect_to_kafka(window, values)
    if event == "Send Message":
        send_message(window, values)
    if event == "Read Messages":
        read_messages(window)
    if event == "Add ACL Rule":
        add_acl_rule(window, values)

window.close()