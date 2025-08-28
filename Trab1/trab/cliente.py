import sys,os,pika,threading

leiloes = []
ativos = []
cliente = ""  # Nome do cliente, pode ser modificado conforme necessário

def main():
    if len(sys.argv) < 2:
        print("Uso: python lance.py <cliente>")
        sys.exit(1)

    global cliente
    cliente = sys.argv[1]
    t1 = threading.Thread(target=adicionar_leiloes)
    t1.start()
    
    t2 = threading.Thread(target=aguarda_user)
    t2.start()

    t2.join()

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
        obj = eval(leilao, {}) 
        leiloes.append(obj)
        mostrar_leiloes()
        print("> ")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

def mostrar_leiloes():
    print("\n=== LEILÕES EM EXECUÇÃO ===")

    if not leiloes:
        print("Nenhum leilão ativo no momento.")
    else:
        # Cabeçalho da tabela
        print(f"{'Nº':<5}{'Descrição':<30}{'Valor':<10}{'Último Lance':<20}{'Status':<10}")
        print("-" * 80)

        for i, leilao in enumerate(leiloes):
            valor = f"R${leilao['valor']:.2f}" if 'valor' in leilao and leilao['valor'] is not None else "-"
            # cliente é string, não precisa de :.2f
            ultimo_lance = leilao['cliente'] if 'cliente' in leilao and leilao['cliente'] is not None else "-"
            status = leilao['status'] if 'status' in leilao else "ativo"
            print(f"{i:<5}{leilao['descricao']:<30}{valor:<10}{ultimo_lance:<20}{status:<10}")


    print("=" * 80)
    print("Digite: <numero_do_leilao> <valor_do_lance>")
    print("Ou digite 'sair' para encerrar\n")

def aguarda_user():
    while True:
        mostrar_leiloes()
        comando = input("> ").strip()

        if comando.lower() == "sair":
            print("Encerrando trabalhador...")
            break

        partes = comando.split()
        if len(partes) != 2:
            print("Formato inválido! Use: <numero> <valor>")
            continue

        try:
            idx = int(partes[0])
            valor = float(partes[1])
        except ValueError:
            print("Número do leilão deve ser inteiro e valor deve ser numérico.")
            continue

        if idx < 0 or idx >= len(leiloes):
            print("Leilão inválido.")
            continue

        publicar_lance(leiloes[idx], valor)

def publicar_lance(leilao, valor):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='lance', exchange_type='direct')
    channel.basic_publish(exchange='lance', routing_key='publicar', body=str({"id": leilao['id'], "valor": valor, "cliente": cliente}))
    if(leilao['id'] in ativos):
        return
    threading.Thread(target=acompanhar_leilao, args=(leilao['id'],)).start()
    ativos.append(leilao['id'])

def acompanhar_leilao(id):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='leilao', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='leilao', queue=queue_name, routing_key=f'leilao_{id}')

    def callback(ch, method, properties, body):
        lance = eval(body.decode())
        valor = lance.get('valor')
        if(valor is not None):
            for leilao in leiloes:
                if leilao['id'] == id:
                    leilao['valor'] = lance['valor']
                    leilao['cliente'] = lance['cliente']
                    leilao['status'] = lance['status']
                    break
        
        status = lance.get('status')
        if(status is not None):
            for leilao in leiloes:
                if leilao['id'] == id:
                    leilao['status'] = lance['status']
                    break
        
        mostrar_leiloes()
        print("> ")

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