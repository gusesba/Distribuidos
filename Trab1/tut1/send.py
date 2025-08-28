#!/usr/bin/env python
import pika

# Conecta com o broker no endereço localhost
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declara a fila 'hello'
channel.queue_declare(queue='hello')

# Envia a mensagem "Hello World!" para a fila hello
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')

print(" [x] Sent 'Hello World!'")

connection.close()