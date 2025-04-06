from pymongo import MongoClient
from datetime import datetime
import logging

logging.basicConfig(filename='shop_demo.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def log_and_print(message):
    print(message)
    logging.info(message)

# Підключення
client = MongoClient("mongodb://localhost:27017")
db = client.shop

# 1. Створення товарів
log_and_print("1. Створення товарів")
items = [
    {"category": "Phone", "model": "iPhone 13", "producer": "Apple", "price": 999},
    {"category": "TV", "model": "Samsung QLED", "producer": "Samsung", "price": 1500, "size": 55},
    {"category": "Smart Watch", "model": "Galaxy Watch 5", "producer": "Samsung", "price": 300, "battery_life": "24h"},
    {"category": "Phone", "model": "Pixel 6", "producer": "Google", "price": 699}
]
db.items.drop()
db.items.insert_many(items)

# 2. Запити

def run_queries():
    log_and_print("2.1. Всі товари:")
    for item in db.items.find():
        log_and_print(str(item))

    log_and_print("2.2. Кількість товарів категорії 'Phone':")
    log_and_print(str(db.items.count_documents({"category": "Phone"})))

    log_and_print("2.3. Унікальні категорії:")
    log_and_print(str(db.items.distinct("category")))

    log_and_print("2.4. Унікальні виробники:")
    log_and_print(str(db.items.distinct("producer")))

    log_and_print("2.5. Категорія Phone та ціна 600-1000:")
    for i in db.items.find({"$and": [{"category": "Phone"}, {"price": {"$gte": 600, "$lte": 1000}}]}):
        log_and_print(str(i))

    log_and_print("2.6. Модель iPhone 13 або Pixel 6:")
    for i in db.items.find({"$or": [{"model": "Pixel 6"}, {"model": "iPhone 13"}]}):
        log_and_print(str(i))

    log_and_print("2.7. Виробники зі списку [Google, Samsung]:")
    for i in db.items.find({"producer": {"$in": ["Google", "Samsung"]}}):
        log_and_print(str(i))

    log_and_print("2.8. Товари з полем battery_life:")
    for item in db.items.find({"battery_life": {"$exists": True}}):
        log_and_print(str(item))

# 3. Оновлення товарів
log_and_print("3. Оновлення товарів: додаємо 'discount' і 'available'")
db.items.update_many({"category": "Phone"}, {"$set": {"discount": 10, "available": True}})
log_and_print("Оновлення цін для товарів з battery_life")
db.items.update_many({"battery_life": {"$exists": True}}, {"$inc": {"price": 50}})

# 4. Створення замовлень
log_and_print("4. Створення замовлень")
db.orders.drop()
item_ids = list(db.items.find())
order1 = {
    "order_number": 1001,
    "date": datetime.utcnow(),
    "total_sum": 2299,
    "customer": {
        "name": "Andrii",
        "surname": "Rodionov",
        "phones": [9876543, 1234567],
        "address": "PTI, Peremohy 37, Kyiv, UA"
    },
    "payment": {
        "card_owner": "Andrii Rodionov",
        "cardId": 12345678
    },
    "items_id": [item_ids[0]['_id'], item_ids[1]['_id']]
}
order2 = {
    "order_number": 1002,
    "date": datetime.utcnow(),
    "total_sum": 999,
    "customer": {
        "name": "Oksana",
        "surname": "Ivanova",
        "phones": [9870000],
        "address": "Khreshchatyk 1, Kyiv, UA"
    },
    "payment": {
        "card_owner": "Oksana Ivanova",
        "cardId": 87654321
    },
    "items_id": [item_ids[1]['_id'], item_ids[2]['_id']]
}
db.orders.insert_many([order1, order2])

# 5. Запити до замовлень
def order_queries():
    log_and_print("5.1. Всі замовлення:")
    for o in db.orders.find():
        log_and_print(str(o))

    log_and_print("5.2. Замовлення з сумою > 1000:")
    for o in db.orders.find({"total_sum": {"$gt": 1000}}):
        log_and_print(str(o))

    log_and_print("5.3. Замовлення клієнта Andrii:")
    for o in db.orders.find({"customer.name": "Andrii"}):
        log_and_print(str(o))

    log_and_print("5.4. Замовлення з товаром Samsung QLED:")
    samsung_tv_id = item_ids[1]['_id']
    for o in db.orders.find({"items_id": samsung_tv_id}):
        log_and_print(str(o))

    log_and_print("5.5. Додаємо товар Pixel 6 до всіх замовлень з Samsung QLED та збільшуємо вартість на 100")
    pixel_id = item_ids[3]['_id']
    db.orders.update_many({"items_id": samsung_tv_id}, {"$push": {"items_id": pixel_id}, "$inc": {"total_sum": 100}})

    log_and_print("5.6. Кількість товарів у кожному замовленні:")
    for o in db.orders.find():
        log_and_print(f"Order {o['order_number']} has {len(o['items_id'])} items")

    log_and_print("5.7. Інфо про кастомера + номер картки (total > 1000):")
    for o in db.orders.find({"total_sum": {"$gt": 1000}}, {"customer": 1, "payment.cardId": 1, "_id": 0}):
        log_and_print(str(o))

    log_and_print("5.8. Видалення Samsung QLED з замовлень за 2024 рік")
    db.orders.update_many(
        {"date": {"$gte": datetime(2024, 1, 1), "$lte": datetime(2024, 12, 31)}},
        {"$pull": {"items_id": samsung_tv_id}}
    )

    log_and_print("5.9. Перейменування Rodionov на Haiduchyk")
    db.orders.update_many({"customer.surname": "Rodionov"}, {"$set": {"customer.surname": "Haiduchyk"}})

    log_and_print("5.10. Деталі товарів у замовленнях клієнта Andrii (join):")
    for o in db.orders.find({"customer.name": "Andrii"}):
        log_and_print(f"Customer: {o['customer']}")
        for i_id in o['items_id']:
            item = db.items.find_one({"_id": i_id})
            if item:
                log_and_print(f" - {item['model']} (${item['price']})")

# 6. Відгуки
def create_reviews():
    log_and_print("6. Створення capped collection для відгуків")
    db.drop_collection("reviews")
    db.create_collection("reviews", capped=True, size=4096, max=5)
    for i in range(7):
        db.reviews.insert_one({
            "review": f"Review {i}",
            "rating": 5 - (i % 3),
            "user": f"user{i}",
            "created_at": datetime.utcnow()
        })
    log_and_print("Актуальні 5 відгуків:")
    for r in db.reviews.find():
        log_and_print(str(r))


if __name__ == "__main__":
    run_queries()
    order_queries()
    create_reviews()
