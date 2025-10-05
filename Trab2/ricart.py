import Pyro5.api
import threading
import time
import sys

@Pyro5.api.expose
class RicartAgrawalaProcess:
    def __init__(self, pid):
        self.pid = pid
        self.state = "RELEASED"
        self.timestamp = 0
        self.replies = 0
        self.peers = []   # guarda URIs dos peers
        self.peersNames = []
        self.deferred = []
        self.lock = threading.Lock()

    def update_peers(self):
        """Atualiza a lista de peers a partir do nameserver, ignorando o nameserver"""
        with Pyro5.api.locate_ns() as ns:
            self.peers = []
            for name, uri in ns.list().items():
                if name.startswith("ricart.") and name != self.pid:  # só processos válidos
                    self.peers.append(uri)
                    self.peersNames.append(name)
        print(f"[{self.pid}] Peers atualizados: {self.peersNames}")

    def request_resource(self):
        """Solicita o recurso a todos os peers"""
        self.state = "WANTED"
        self.timestamp = time.time()
        self.replies = 0
        print(f"[{self.pid}] Pedindo recurso...")

        for uri in self.peers:
            try:
                with Pyro5.api.Proxy(uri) as peer:
                    peer.request(self.timestamp, self.pid)
            except Exception as e:
                print(f"[{self.pid}] Peer {uri} não disponível, ignorando ({e})")

        # espera todos os replies válidos
        while self.replies < len(self.peers):
            time.sleep(0.1)

        self.state = "HELD"
        print(f"[{self.pid}] Recurso adquirido.")

    def release_resource(self):
        """Libera o recurso e responde pedidos adiados"""
        self.state = "RELEASED"
        print(f"[{self.pid}] Liberando recurso.")

        for (ts, pid) in self.deferred:
            try:
                with Pyro5.api.Proxy(f"PYRONAME:{pid}") as peer:
                    peer.reply(self.pid)
            except Exception as e:
                print(f"[{self.pid}] Falha ao responder {pid}, ignorando ({e})")

        self.deferred = []

    # --------------------- Métodos remotos -----------------------

    def request(self, ts, pid):
        """Recebe pedido de outro processo"""
        with self.lock:
            if (self.state == "HELD" or
               (self.state == "WANTED" and (ts, pid) > (self.timestamp, self.pid))):
                self.deferred.append((ts, pid))
                print(f"[{self.pid}] Pedido de {pid} adiado.")
            else:
                try:
                    with Pyro5.api.Proxy(f"PYRONAME:{pid}") as peer:
                        peer.reply(self.pid)
                except Exception as e:
                    print(f"[{self.pid}] Falha ao responder {pid} imediatamente ({e})")

    def reply(self, pid):
        """Recebe resposta de outro processo"""
        with self.lock:
            self.replies += 1
            print(f"[{self.pid}] Resposta recebida de {pid} ({self.replies}/{len(self.peers)}).")

# --------------------- Interface do terminal ---------------------

def interface(process):
    while True:
        print(f"\nEstado atual: {process.state}")
        if process.state == "RELEASED":
            cmd = input("Digite [p] para pedir o recurso ou [q] para sair: ").strip()
            if cmd == "p":
                process.update_peers()
                process.request_resource()
            elif cmd == "q":
                break
        elif process.state == "HELD":
            cmd = input("Digite [l] para liberar o recurso: ").strip()
            if cmd == "l":
                process.release_resource()
        else:
            time.sleep(0.5)

# --------------------- Inicialização -----------------------------

def main():
    if len(sys.argv) < 2:
        print("Uso: python ricart.py <NOME_DO_PROCESSO>")
        sys.exit(1)

    pid = "ricart." + sys.argv[1]
    process = RicartAgrawalaProcess(pid)

    # inicia o daemon Pyro
    daemon = Pyro5.api.Daemon()
    uri = daemon.register(process)

    # registra no nameserver
    with Pyro5.api.locate_ns() as ns:
        ns.register(pid, uri)
    print(f"[{pid}] registrado no nameserver.")

    # thread para atender requisições remotas
    t = threading.Thread(target=daemon.requestLoop, daemon=True)
    t.start()

    # interface de usuário
    interface(process)

if __name__ == "__main__":
    main()
