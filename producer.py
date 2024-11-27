from confluent_kafka import Producer, KafkaError
import json
import time  # Импортируем модуль time

# Конфигурация Kafka Producer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Замените на адрес вашего Kafka сервера
}

# Создаем Producer
producer = Producer(conf)

# Функция для обработки отчета о доставке
def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")

try:
    while True:
        # Вводим данные о продукте от пользователя
        product_id = input("Введите ID продукта (или введите 'exit' для выхода): ")
        if product_id.lower() == 'exit':
            break

        quantity = input("Введите количество: ")
        price = input("Введите цену: ")

        # Создаем сообщение
        message = {
            'product_id': product_id,
            'quantity': int(quantity),
            'price': float(price),
            'timestamp': int(time.time())  # Используем time.time() для получения текущего временного штампа
        }

        # Отправляем сообщение в тему 'product_purchases'
        producer.produce('product_purchases', json.dumps(message).encode('utf-8'), callback=delivery_report)

        # Ожидаем доставки всех ожидающих сообщений
        producer.poll(0)

except KeyboardInterrupt:
    print("Остановка Producer...")

finally:
    # Ожидаем до 1 секунды для событий. Обработчики будут вызваны во время
    # вызова flush(), если есть какие-либо ожидающие сообщения.
    producer.flush()