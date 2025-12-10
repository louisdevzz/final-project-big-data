from kafka import KafkaConsumer
import json
from datetime import datetime
import sys
import time

KAFKA_BOOTSTRAP_SERVERS = "192.168.80.57:9093"

# Check if topics exist and have data
def check_topic_status(topic_name):
    """Check if topic exists and has messages"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            consumer_timeout_ms=5000
        )
        
        topics = consumer.topics()
        if topic_name not in topics:
            print(f"❌ ERROR: Topic '{topic_name}' does not exist!")
            print(f"Available topics: {topics}")
            consumer.close()
            return False
            
        # Check partitions
        partitions = consumer.partitions_for_topic(topic_name)
        print(f"✓ Topic '{topic_name}' exists with {len(partitions)} partition(s)")
        
        consumer.close()
        return True
    except Exception as e:
        print(f"❌ ERROR checking topic: {e}")
        return False

def view_input_data():
    """View input_data topic with features and actual labels"""
    print("="*80)
    print("📥 VIEWING INPUT DATA TOPIC")
    print("="*80)
    print(f"Connected to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: input_data")
    
    if not check_topic_status('input_data'):
        return
    
    print("-"*80)
    print(f"{'Time':<20} {'Feature1':<10} {'Feature2':<10} {'Feature3':<10} {'Feature4':<10} {'Actual':<8}")
    print("-"*80)
    
    consumer = KafkaConsumer(
        'input_data',
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='input-viewer-group'
    )
    
    try:
        for message in consumer:
            data = message.value
            timestamp = datetime.fromtimestamp(message.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"{timestamp:<20} "
                  f"{data['feature1']:<10.2f} "
                  f"{data['feature2']:<10.2f} "
                  f"{data['feature3']:<10.2f} "
                  f"{data['feature4']:<10.2f} "
                  f"{data['target']:<8}")
            
    except KeyboardInterrupt:
        print("\n\n✓ Stopped viewing input_data")
    finally:
        consumer.close()


def view_predictions():
    """View predictions topic with actual vs predicted labels"""
    print("="*100)
    print("🔮 VIEWING PREDICTIONS TOPIC")
    print("="*100)
    print(f"Connected to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: predictions")
    
    if not check_topic_status('predictions'):
        return
    
    print("-"*100)
    print(f"{'Time':<20} {'Feature1':<10} {'Feature2':<10} {'Feature3':<10} {'Feature4':<10} {'Actual':<8} {'Predicted':<10} {'Match':<6}")
    print("-"*100)
    
    consumer = KafkaConsumer(
        'predictions',
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='predictions-viewer-group'
    )
    
    correct = 0
    total = 0
    
    try:
        for message in consumer:
            data = message.value
            timestamp = datetime.fromtimestamp(message.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            actual = int(data['actual_label'])
            predicted = int(data['predicted_label'])
            match = "✓" if actual == predicted else "✗"
            
            total += 1
            if actual == predicted:
                correct += 1
            
            accuracy = (correct / total) * 100
            
            print(f"{timestamp:<20} "
                  f"{data['feature1']:<10.2f} "
                  f"{data['feature2']:<10.2f} "
                  f"{data['feature3']:<10.2f} "
                  f"{data['feature4']:<10.2f} "
                  f"{actual:<8} "
                  f"{predicted:<10.0f} "
                  f"{match:<6}  [Accuracy: {accuracy:.1f}% ({correct}/{total})]")
            
    except KeyboardInterrupt:
        print("\n")
        print("="*100)
        print(f"📊 FINAL STATISTICS")
        print(f"Total predictions: {total}")
        print(f"Correct: {correct}")
        print(f"Incorrect: {total - correct}")
        print(f"Accuracy: {accuracy:.2f}%")
        print("="*100)
        print("\n✓ Stopped viewing predictions")
    finally:
        consumer.close()


def view_both_topics():
    """View both topics side by side (requires threading)"""
    import threading
    
    def input_thread():
        view_input_data()
    
    def prediction_thread():
        view_predictions()
    
    print("Starting viewers for both topics...")
    print("Press Ctrl+C to stop\n")
    
    t1 = threading.Thread(target=input_thread)
    t2 = threading.Thread(target=prediction_thread)
    
    t1.start()
    t2.start()
    
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\n✓ Stopped all viewers")


if __name__ == "__main__":
    print("\n🔍 Kafka Topic Viewer")
    print("=" * 50)
    print("Select option:")
    print("1. View input_data topic")
    print("2. View predictions topic")
    print("3. View both topics (split view)")
    print("=" * 50)
    
    choice = input("Enter choice (1/2/3): ").strip()
    
    if choice == "1":
        view_input_data()
    elif choice == "2":
        view_predictions()
    elif choice == "3":
        view_both_topics()
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)