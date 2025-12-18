from flask import Flask
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import json
import threading

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

KAFKA_BOOTSTRAP_SERVERS = "192.168.80.57:9093"
KAFKA_INPUT_TOPIC = "input_data"
KAFKA_OUTPUT_TOPIC = "predictions"

# Class names for fraud detection
CLASS_NAMES = {
    0: "Normal",
    1: "Fraud"
}

def consume_input_data():
    """Consume from input_data topic and emit to frontend"""
    consumer = KafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"✓ Started consuming from {KAFKA_INPUT_TOPIC}")
    
    for message in consumer:
        data = message.value

        # Add class name for display
        if 'Class' in data:
            data['class_name'] = CLASS_NAMES.get(data['Class'], 'unknown')

        # Print summary of transaction
        print(f"→ Input: Time={data.get('Time', 'N/A'):.0f}, Amount={data.get('Amount', 0):.2f}, Class={data.get('Class', 'N/A')} ({data.get('class_name', 'N/A')})")
        socketio.emit('input_data', data)

def consume_predictions():
    """Consume from predictions topic and emit to frontend"""
    consumer = KafkaConsumer(
        KAFKA_OUTPUT_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"✓ Started consuming from {KAFKA_OUTPUT_TOPIC}")
    
    for message in consumer:
        data = message.value

        # Add class names for display
        if 'actual_label' in data:
            data['actual_label_name'] = CLASS_NAMES.get(int(data['actual_label']), 'unknown')

        if 'predicted_label' in data:
            data['predicted_label_name'] = CLASS_NAMES.get(int(data['predicted_label']), 'unknown')

        # Calculate if prediction is correct
        if 'actual_label' in data and 'predicted_label' in data:
            data['is_correct'] = int(data['actual_label']) == int(data['predicted_label'])
            status = "✓" if data['is_correct'] else "✗"
            print(f"{status} Prediction: actual={int(data['actual_label'])} ({data['actual_label_name']}), predicted={int(data['predicted_label'])} ({data['predicted_label_name']})")
        elif 'Class' in data and 'prediction' in data:
            # Alternative format
            data['actual_label'] = data['Class']
            data['predicted_label'] = data['prediction']
            data['actual_label_name'] = CLASS_NAMES.get(int(data['Class']), 'unknown')
            data['predicted_label_name'] = CLASS_NAMES.get(int(data['prediction']), 'unknown')
            data['is_correct'] = int(data['Class']) == int(data['prediction'])
            status = "✓" if data['is_correct'] else "✗"
            print(f"{status} Prediction: actual={int(data['Class'])} ({data['actual_label_name']}), predicted={int(data['prediction'])} ({data['predicted_label_name']})")
        else:
            print(f"← Prediction: {data}")

        socketio.emit('prediction_data', data)

@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Kafka Monitor Backend</title>
        <style>
            body { 
                font-family: Arial; 
                padding: 20px; 
                background: #1a1a1a; 
                color: white; 
            }
            code { 
                background: #333; 
                padding: 2px 6px; 
                border-radius: 3px; 
            }
            .status { 
                color: #4CAF50; 
                font-weight: bold; 
            }
        </style>
    </head>
    <body>
        <h1>✓ Kafka Stream Monitor Backend</h1>
        <p class="status">WebSocket server is running successfully!</p>
        <p>Open <code>monitor.html</code> in your browser to view the dashboard.</p>
        <hr>
        <h3>Configuration:</h3>
        <ul>
            <li>Kafka Server: <code>192.168.80.57:9093</code></li>
            <li>Input Topic: <code>input_data</code></li>
            <li>Output Topic: <code>predictions</code></li>
            <li>WebSocket: <code>ws://0.0.0.0:8000</code></li>
        </ul>
        <h3>Fraud Detection Classes:</h3>
        <ul>
            <li>0 = Normal Transaction</li>
            <li>1 = Fraudulent Transaction</li>
        </ul>
    </body>
    </html>
    '''

@socketio.on('connect')
def handle_connect():
    print('✓ Client connected')
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    print('✗ Client disconnected')

if __name__ == '__main__':
    # Start Kafka consumers in background threads
    input_thread = threading.Thread(target=consume_input_data, daemon=True)
    prediction_thread = threading.Thread(target=consume_predictions, daemon=True)
    
    input_thread.start()
    prediction_thread.start()
    
    print("\n" + "="*60)
    print("🚀 Kafka Monitor Backend Server - Fraud Detection")
    print("="*60)
    print(f"→ Server: http://0.0.0.0:8000")
    print(f"→ Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"→ Input Topic: {KAFKA_INPUT_TOPIC}")
    print(f"→ Output Topic: {KAFKA_OUTPUT_TOPIC}")
    print(f"→ Classes: 0=Normal, 1=Fraud")
    print("="*60 + "\n")
    
    socketio.run(app, host='0.0.0.0', port=8000, debug=False)