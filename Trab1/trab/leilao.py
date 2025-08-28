from datetime import datetime, timedelta
import time
import pika
import os
import sys

leiloes = [
    {
        "id": 0,
        "descricao": "Playstation 5",
        "inicio": datetime.now(),
        "fim": datetime.now() + timedelta(minutes=1),
        "status": "aguardando inicio"
    },
    {
        "id": 1,
        "descricao": "iPhone 15 Pro Max",
        "inicio": datetime.now() + timedelta(minutes=1),
        "fim": datetime.now() + timedelta(minutes=1, seconds=30),
        "status": "aguardando inicio"
    },
    {
        "id": 2,
        "descricao": "Notebook Dell XPS 13",
        "inicio": datetime.now() + timedelta(seconds=40),
        "fim": datetime.now() + timedelta(minutes=1),
        "status": "aguardando inicio"
    },
    {
        "id": 3,
        "descricao": "Smart TV Samsung 65\" 4K",
        "inicio": datetime.now(),
        "fim": datetime.now() + timedelta(minutes=1, seconds=20),
        "status": "aguardando inicio"
    },
    {
        "id": 4,
        "descricao": "Fone de Ouvido Sony WH-1000XM5",
        "inicio": datetime.now() + timedelta(minutes=2, seconds=30),
        "fim": datetime.now() + timedelta(minutes=3, seconds=30),
        "status": "aguardando inicio"
    }
]

def main():
    channel = iniciarConexao()

    while True:
        agora = datetime.now()
        for leilao in leiloes:
            if leilao["status"] == "aguardando inicio" and agora >= leilao["inicio"]:
                iniciarLeilao(channel, leilao)

            if leilao["status"] == "ativo" and agora >= leilao["fim"]:
                finalizarLeilao(channel, leilao)

        if all(l["status"] == "encerrado" for l in leiloes):
            break

        time.sleep(1)
    
    channel.close()

def iniciarConexao():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='leilao', exchange_type='direct')
    return channel

def iniciarLeilao(channel, leilao):
    print("Iniciando leilão:", leilao["descricao"])
    leilao["status"] = "ativo"
    channel.basic_publish(
        exchange='leilao',
        routing_key='iniciar',
        body=str({"id": leilao["id"], "descricao": leilao["descricao"]})
    )

def finalizarLeilao(channel, leilao):
    print("Leilão finalizado:", leilao["descricao"])
    leilao["status"] = "encerrado"
    channel.basic_publish(
        exchange='leilao',
        routing_key='encerrar',
        body=str(leilao)
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)