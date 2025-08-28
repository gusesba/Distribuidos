import sys, os, pika, threading

def main():
    t1 = threading.Thread(target=escuta_lances)
    t1.start()

    t2 = threading.Thread(target=finaliza_leilao)
    t2.start()

    t1.join()

def escuta_lances():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='lance_validado', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='lance_validado', queue=queue_name, routing_key='publicar')

    def callback(ch, method, properties, body):
        lance = eval(body.decode())
        ch.exchange_declare(exchange='leilao', exchange_type='direct')
        ch.basic_publish(
            exchange='leilao',
            routing_key='leilao_{}'.format(lance['id']),
            body=str(lance).encode()
        )

        print('leilao_{}'.format(lance['id']))

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

def finaliza_leilao():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='leilao', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='leilao', queue=queue_name, routing_key='leilao_vencedor')

    def callback(ch, method, properties, body):
        vencedor_info = eval(body.decode())
        ch.basic_publish(
            exchange='leilao',
            routing_key='leilao_{}'.format(vencedor_info['id']),
            body=str({"status": "finalizado"}).encode()
        )

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)