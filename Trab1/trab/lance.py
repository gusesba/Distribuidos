import sys,os,pika,threading,datetime


leiloes = []


def main():
    t1 = threading.Thread(target=adicionar_leiloes)
    t1.start()

    t2 = threading.Thread(target=escutar_lances)
    t2.start()

    t3 = threading.Thread(target=remover_leiloes)
    t3.start()

    t1.join()

def adicionar_leiloes():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='leilao', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='leilao', queue=queue_name, routing_key='iniciar')
    
    def callback(ch, method, properties, body): 
        global leiloes 
        leilao = body.decode() 
        print("Leilão recebido:", leilao)
        obj = eval(leilao, {}) 
        obj['valor'] = 0
        leiloes.append(obj)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

def escutar_lances():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='lance_validado', exchange_type='direct')
    channel.exchange_declare(exchange='lance', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='lance', queue=queue_name, routing_key='publicar')

    def callback(ch, method, properties, body):
        lance = eval(body.decode())
        leilao_id = lance['id']

        leilao_idx = next((idx for idx, l in enumerate(leiloes) if l['id'] == leilao_id), None)
        
        if leilao_idx is None:
            return
        
        leilao = leiloes[leilao_idx]

        status = leilao.get('status', 'ativo')
        cliente = leilao.get('cliente', '-')
        valor = leilao.get('valor', '-')

        if(lance['valor'] > leilao['valor'] and status != 'finalizado'):
            leilao['valor'] = lance['valor']
            leilao['cliente'] = lance['cliente']

        if(status == 'finalizado'):
            leilao['valor'] = valor
            leilao['cliente'] = cliente
            leilao['status'] = 'finalizado'

        channel.basic_publish(
            exchange='lance_validado',
            routing_key='publicar',
            body=str({"id": leilao_id, "valor": leilao['valor'], "cliente": leilao['cliente'], "status": status})
        )

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

def remover_leiloes():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='leilao', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='leilao', queue=queue_name, routing_key='encerrar')

    def callback(ch, method, properties, body):
        global leiloes
        leilao_final = eval(body.decode(), {"datetime": datetime})  # recebe o leilão finalizado
        leilao_id = leilao_final['id']

        leilao = next((l for l in leiloes if l['id'] == leilao_id), None)
        if not leilao:
            return

        vencedor = leilao.get('cliente', None)
        valor_final = leilao.get('valor', 0)

        mensagem = {
            "id": leilao_id,
            "vencedor": vencedor,
            "valor": valor_final
        }

        ch.basic_publish(
            exchange="leilao",
            routing_key="leilao_vencedor",
            body=str(mensagem).encode()
        )

        leilao['status'] = 'finalizado'

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