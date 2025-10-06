import Pyro5.api
import threading
import sys
import time

HEARTBEAT_TIMEOUT = 15
HEARTBEAT_INTERVAL = 3

class RicartAgrawala:
    def __init__(self, pid):
        self.pid = pid
        self.state = "RELEASED"
        self.peers = []        # lista de URIs conhecidos
        self.peersNames = []   # nomes simbólicos dos peers
        self.peersHeartbeat = {}  # dicionário {uri: timestamp do último heartbeat}

    @Pyro5.api.expose
    def receberHeartBeat(self, uri, name):
        agora = time.time()
        self.peersHeartbeat[uri] = agora

        if uri not in self.peers:
            self.peers.append(uri)
            self.peersNames.append(name)
            print(f"[{self.pid}] Novo peer adicionado: {name}")

        print(f"[{self.pid}] Heartbeat recebido de {name} em {time.ctime(agora)}")

    def update_peers(self):
        """Busca todos os peers registrados no NameServer que comecem com 'ricart.'"""
        try:
            with Pyro5.api.locate_ns() as ns:
                entries = ns.list(prefix="ricart.")
                for name, uri in entries.items():
                    if uri not in self.peers and name != self.pid:
                        self.peers.append(uri)
                        self.peersNames.append(name)
                        self.peersHeartbeat[uri] = time.time()
                        print(f"[{self.pid}] Peer encontrado no NS: {name}")
        except Exception as e:
            print(f"[{self.pid}] Erro ao buscar peers no NameServer: {e}")


def main():
    if len(sys.argv) < 2:
        print("Uso: python ricart.py <NOME_DO_PROCESSO>")
        sys.exit(1)

    pid = "ricart." + sys.argv[1]
    process = RicartAgrawala(pid)

    daemon = Pyro5.api.Daemon()
    uri = daemon.register(process)
    process.uri = uri

    with Pyro5.api.locate_ns() as ns:
        ns.register(pid, uri)
    print(f"[{pid}] registrado no nameserver como {uri}.")

    # Atualiza a lista de peers antes de entrar no loop
    process.update_peers()

    daemon.requestLoop()


if __name__ == "__main__":
    main()
