from confluent_kafka import Consumer, KafkaError
import json

# Конфигурация Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Замените на адрес вашего Kafka сервера
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest',
}

# Создание Consumer
consumer = Consumer(conf)

# Подписка на тему
topic = 'product_purchases'
consumer.subscribe([topic])

# Словарь для хранения данных о покупках
purchase_data = {}

try:
    while True:
        # Получение сообщения
        msg = consumer.poll(1.0)  # Ожидание сообщения в течение 1 секунды

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Конец потока
                continue
            else:
                print(f"Ошибка: {msg.error()}")
                break

        # Декодирование сообщения
        message = json.loads(msg.value().decode('utf-8'))

        # Получение product_id из сообщения
        product_id = str(message['product_id'])

        # Добавление сообщения в purchase_data
        if product_id not in purchase_data:
            purchase_data[product_id] = []  # Создаем новый список, если ключ отсутствует

        purchase_data[product_id].append(message)

        # Вывод текущих данных о покупках
        print("Текущие данные о покупках:")
        for product_id, purchases in purchase_data.items():
            total_quantity = sum(item['quantity'] for item in purchases)
            total_price = sum(item['quantity'] * item['price'] for item in purchases)
            print(f"Product ID: {product_id}, Total Quantity: {total_quantity}, Total Price: {total_price:.2f}")

            # Проверка на превышение общей стоимости
            if total_price > 3000:
                print(f"ALERT: Общая стоимость для Product ID {product_id} превышает 3000! (Total Price: {total_price:.2f})")

except KeyboardInterrupt:
    # Обработка прерывания (например, Ctrl+C)
    print("Остановка Consumer...")

finally:
    # Закрытие Consumer
    consumer.close()