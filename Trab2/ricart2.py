import Pyro5.api
import threading
import sys
import time
import queue

HEARTBEAT_TIMEOUT = 15
HEARTBEAT_INTERVAL = 3
REQUEST_TIMEOUT = 10
RECURSO_TIMEOUT = 20  # tempo máximo para segurar o recurso

class RicartAgrawala:
    def __init__(self, pid):
        self.pid = pid
        self.state = "RELEASED"
        self.peers = []
        self.peersNames = []
        self.peersHeartbeat = {}
        self.deferred = []
        self.lock = threading.Lock()

    @Pyro5.api.expose
    def receberHeartBeat(self, uri, name):
        agora = time.time()
        self.peersHeartbeat[uri] = agora

        # Adiciona o peer à lista se ainda não estiver nela
        if uri != str(self.uri) and uri not in self.peers:
            self.peers.append(uri)
            self.peersNames.append(name)
            print(f"[{self.pid}] Novo peer adicionado: {name}")

        #print(f"[{self.pid}] Heartbeat recebido de {name} em {agora}")

    def enviar_heartbeat(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            for idx, uri in enumerate(self.peers):
                try:
                    peer_name = self.peersNames[idx]
                    with Pyro5.api.Proxy(uri) as proxy:
                        proxy.receberHeartBeat(self.uri, self.pid) 
                    #print(f"[{self.pid}] Heartbeat enviado para {peer_name}")
                except Exception as e:
                    #print(f"[{self.pid}] Erro ao enviar heartbeat para {peer_name}: {e}") 
                    continue

    def update_peers(self):
        self.peers = []
        self.peersNames = []
        self.peersHeartbeat = {}  
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
            #print(f"[{self.pid}] Erro ao buscar peers no NameServer: {e}")
            pass

    def request_resource(self):
        self.state = "WANTED"
        self.timestamp = time.time()
        self.replies = 0
        print(f"[{self.pid}] Pedindo recurso...")

        def request_to_peer(uri):
            agora = time.time()
            # valida heartbeat
            ultimo_heartbeat = self.peersHeartbeat.get(uri, 0)
            if agora - ultimo_heartbeat > HEARTBEAT_TIMEOUT:
                print(f"[{self.pid}] Peer {uri} inativo, ignorando.")
                self.replies += 1
                return
            try:
                with Pyro5.api.Proxy(uri) as peer:
                    peer._pyroTimeout = REQUEST_TIMEOUT  # timeout de 10 segundos
                    peer.request(self.timestamp, self.pid)
            except Exception as e:
                #print(f"[{self.pid}] Peer {uri} não disponível ou timeout ({e})")
                self.replies += 1

        threads = []
        for uri in self.peers:
            t = threading.Thread(target=request_to_peer, args=(uri,))
            t.start()
            threads.append(t)

        while self.replies < len(self.peers):
            time.sleep(0.1)

        self.state = "HELD"
        print(f"[{self.pid}] Recurso adquirido.")

    @Pyro5.api.expose
    def request(self, ts, pid):
        """Recebe pedido de outro processo"""
        with self.lock:
            if (self.state == "HELD" or
               (self.state == "WANTED" and (ts, pid) > (self.timestamp, self.pid))):
                try:
                    with Pyro5.api.Proxy(f"PYRONAME:{pid}") as peer:
                        peer.reply(self.pid, False)
                        self.deferred.append((ts, pid))
                        print(f"[{self.pid}] Pedido de {pid} adiado.")
                except Exception as e:
                    #print(f"[{self.pid}] Falha ao responder {pid} imediatamente ({e})")
                    pass
                
            else:
                try:
                    with Pyro5.api.Proxy(f"PYRONAME:{pid}") as peer:
                        peer.reply(self.pid, True)
                except Exception as e:
                    #print(f"[{self.pid}] Falha ao responder {pid} imediatamente ({e})")
                    pass

    @Pyro5.api.expose
    def reply(self, pid, confirmation):
        if confirmation:
            print(f"[{self.pid}] Recebi confirmação de {pid}")
            self.replies += 1
        else:
            print(f"[{self.pid}] Recebi negação de {pid}.")

    def release_resource(self):
        self.state = "RELEASED"
        print(f"[{self.pid}] Liberando recurso.")

        for (ts, pid) in self.deferred:
            try:
                with Pyro5.api.Proxy(f"PYRONAME:{pid}") as peer:
                    peer.reply(self.pid,True)
            except Exception as e:
                #print(f"[{self.pid}] Falha ao responder {pid}, ignorando ({e})")
                pass

        self.deferred = []

    def mostrar_status(self):
        agora = time.time()
        ativos = []
        for idx, uri in enumerate(self.peers):
            ultimo_hb = self.peersHeartbeat.get(uri, 0)
            if agora - ultimo_hb <= HEARTBEAT_TIMEOUT:
                ativos.append(self.peersNames[idx])

        print("\n=== STATUS DO PROCESSO ===")
        print(f"Processo: {self.pid}")
        print(f"Estado: {self.state}")
        print(f"Peers ativos ({len(ativos)}): {', '.join(ativos) if ativos else 'Nenhum'}")
        print("===========================\n")


def input_timeout(prompt, timeout):
    q = queue.Queue()

    def ask():
        q.put(input(prompt))

    t = threading.Thread(target=ask, daemon=True)
    t.start()

    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return None  # timeout ocorreu

def interface(process):
    while True:
        process.mostrar_status()
        if process.state == "RELEASED":
            cmd = input("Digite [p] para pedir o recurso, [a] para atualizar ou [q] para sair: ").strip()
            if cmd == "p":
                process.request_resource()
            elif cmd == "q":
                break
        elif process.state == "HELD":
            cmd = input_timeout("Digite [l] para liberar o recurso: ", timeout=RECURSO_TIMEOUT)
            if cmd is None:
                print("Tempo esgotado, liberando recurso automaticamente...")
                process.release_resource()
            elif cmd.strip().lower() == "l":
                process.release_resource()
        else:
            time.sleep(0.5)



def main():
    if len(sys.argv) < 2:
        print("Uso: python ricart.py <NOME_DO_PROCESSO>")
        sys.exit(1)

    pid = "ricart." + sys.argv[1]
    process = RicartAgrawala(pid)

    process.update_peers()

    daemon = Pyro5.api.Daemon(port=0)
    uri = daemon.register(process)
    process.uri = str(uri)

    with Pyro5.api.locate_ns() as ns:
        ns.register(pid, uri)
    print(f"[{pid}] registrado no nameserver como {uri}.")


    t = threading.Thread(target=process.enviar_heartbeat, daemon=True)
    t.start()

     # thread para atender requisições remotas
    t = threading.Thread(target=daemon.requestLoop, daemon=True)
    t.start()

    # interface de usuário
    interface(process)



if __name__ == "__main__":
    main()
