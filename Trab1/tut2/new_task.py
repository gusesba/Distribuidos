#!/usr/bin/env python
import pika
import sys

# Conecta com o broker no endereço localhost
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declara a fila 'task_queue'
channel.queue_declare(queue='task_queue', durable=True)

message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(exchange='',
                      routing_key="task_queue",
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = pika.DeliveryMode.Persistent
                      ))
print(f" [x] Sent {message}")

connection.close()