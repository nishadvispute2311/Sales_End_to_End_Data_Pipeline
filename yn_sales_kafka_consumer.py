from kafka import KafkaConsumer
import json
import os

# List of topics to consume
topics = ['yn-customers-0812', 'yn-orders-0812', 'yn-products-0812', 'yn-order_items-0812']

# Output folder for CSVs
output_dir = 'csv_consumed_data_kafka'
os.makedirs(output_dir, exist_ok=True)  # Ensure folder exists

# Create Kafka Consumer
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='yn_sales_multi_topic_consumer_group_v1',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Track which files have had headers written
written_headers = {}

for message in consumer:
    topic = message.topic
    data = message.value
    print(f"[{topic}] Consumed: {data}")

    # Set CSV filename per topic
    # csv_file = f"{topic}_data.csv"
    csv_file = os.path.join(output_dir, f"{topic}_data.csv")

    # Check if file already had a header written
    write_header = not os.path.exists(csv_file) or topic not in written_headers

    # Write to CSV
    with open(csv_file, 'a', newline='') as f:
        if write_header:
            header_line = ",".join(data.keys())
            f.write(header_line + "\n")
            written_headers[topic] = True

        data_line = ",".join(str(data.get(key, "")) for key in data.keys())
        f.write(data_line + "\n")
