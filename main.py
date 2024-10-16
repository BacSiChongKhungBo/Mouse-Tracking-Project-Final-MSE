from website import create_app
from flask_socketio import SocketIO
from kafka import KafkaProducer
from pynput.mouse import Listener
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import from_json,current_timestamp
import json
import os
import threading
import logging
app = create_app()
socketIO = SocketIO(app)

#log = logging.getLogger('werkzeug')
#log.setLevel(logging.FATAL)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Update with your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@socketIO.on('mouse_event')
def handle_mouse_event(data):
    structured_data = {
        'x': data.get('x'),
        'y': data.get('y'),
        'button': data.get('button')
    }
    print(f'{data}')
    producer.send('mouse_events_topic', value=structured_data)
    producer.flush() 

if __name__ == '__main__':
    socketIO.run(app, debug=True)

