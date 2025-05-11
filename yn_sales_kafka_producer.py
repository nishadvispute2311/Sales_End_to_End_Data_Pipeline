import uuid
import json
import random
import time
from faker import Faker
from datetime import datetime, timedelta
from confluent_kafka import Producer

fake = Faker()

# Kafka configuration
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Pre-generated static datasets
NUM_CUSTOMERS = 97
NUM_PRODUCTS = 80


def customers_generate():
    customer = []
    for i in range(NUM_CUSTOMERS):
        gender = random.choice(['male', 'female'])
        if gender == "male":
            name = fake.name_male()
        else:
            name = fake.name_female()
        c_data = {
            "customer_id": f"C_{random.randint(100000,99999999)}",
            "name": name,
            "gender": gender,
            "email": f"{name.split()[0].lower()}{name.split()[1].lower()}@email.com",
            "phone": fake.phone_number(),
            "signup_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "location": fake.city(),
            "membership_type": random.choice(['Gold','Silver','Bronze'])
        }
        customer.append(c_data)
    return customer



product_names_by_category = {
    "Home": [
        "Ceramic Vase", "Wall Clock", "Cushion Set", "LED Lamp", "Curtains",
        "Table Runner", "Floor Mat", "Storage Basket", "Scented Candles", "Wall Art"],
    "Electronics": [
        "Bluetooth Speaker", "Wireless Mouse", "Laptop Stand", "Power Bank", "Smart Watch",
        "USB Hub", "Noise Cancelling Headphones", "Webcam", "Portable SSD", "LED Monitor"],
    "Clothing": [
        "Denim Jacket", "Graphic T-Shirt", "Cotton Hoodie", "Slim Fit Jeans", "Sports Shorts",
        "Leather Belt", "Sneakers", "Formal Shirt", "Chinos", "Raincoat"],
    "Books": [
        "Python Programming", "Data Science Handbook", "Machine Learning Guide", "Fictional Novel",
        "Biography of Innovator", "Fantasy Epic", "Productivity Hacks", "AI Revolution", 
        "History of Time", "Startup Playbook"]
}

def products_generate():
    products = []
    for i in range(NUM_PRODUCTS):
        category = random.choice(list(product_names_by_category.keys()))
        name = random.choice(product_names_by_category[category])
        p_data = {
            "product_id": f"P_{random.randint(100000,99999999)}",
            "category": category,
            "name": name,
            "price": round(random.uniform(10, 1000), 2)
        }
        products.append(p_data)
    return products

customers_data = customers_generate()
print(customers_data)
products_data = products_generate()


def generate_order():
    customer = random.choice(customers_data)
    random_hours = random.randint(2, 4)
    order_timestamp = datetime.now() + timedelta(hours=random_hours)
    return {
        "order_id": f"O_{random.randint(100000,99999999)}",
        "customer_id": customer["customer_id"],
        "order_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        "total_amount": 0.0,  # Will update after items are generated
        "sales_channel": random.choice(['Online','In-store']),
        "payment_method": random.choice(['Credit Card','UPI','Netbanking','Cash'])
    }

def generate_order_items(order_id, num_items=1):
    items = []
    total = 0.0
    for _ in range(num_items):
        product = random.choice(products_data)
        quantity = random.randint(1, 5)
        item = {
            "item_id": f"OI_{random.randint(100000,99999999)}",
            "order_id": order_id,
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": product["price"]
        }
        total += quantity * product["price"]
        items.append(item)
    return items, total

def produce(topic, key, value):
    producer.produce(topic, key=key, value=json.dumps(value))
    producer.poll(0)

def flush_all():
    producer.flush()

def stream_static_data():
    for c in customers_data:
        produce("yn-customers-0812", c["customer_id"], c)
    for p in products_data:
        produce("yn-products-0812", p["product_id"], p)
    flush_all()
    print("✅ Static data streamed to 'customers' and 'products' topics.")


def stream_orders(interval_seconds=2):
    print(f"📡 Streaming orders and order_items every {interval_seconds} seconds. Press Ctrl+C to stop.")
    while True:
        try:
            order = generate_order()
            items, total = generate_order_items(order["order_id"], random.randint(1, 5))
            order["total_amount"] = round(total, 2)

            produce("yn-orders-0812", order["order_id"], order)
            for item in items:
                produce("yn-order_items-0812", item["item_id"], item)

            flush_all()
            print(f"🟢 Streamed order: {order['order_id']} with {len(items)} items.")
            time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\n🛑 Streaming stopped by user.")
            break

# Main runner
if __name__ == "__main__":
    stream_static_data()       # Send once
    stream_orders(interval_seconds=2)  # Stream every 2 sec


# docker exec -it c25955b318e3 kafka-console-consumer --bootstrap-server localhost:9092 --topic yn-customers-0812 --from-beginning
# docker exec -it c25955b318e3 bash -c "/usr/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic customers-0812"