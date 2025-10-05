import Pyro5.api
import threading
import time
import sys
import random
import collections

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
        self.resource_access_time = 5  # Tempo máximo de acesso ao recurso em segundos
        self.heartbeat_interval = 2    # Intervalo para envio de heartbeat em segundos
        self.heartbeat_timeout = 5     # Tempo limite para receber heartbeat em segundos
        self.request_timeout = 10      # Tempo limite para receber respostas de requisição em segundos
        self.active_peers = {}
        self.last_heartbeat = {}
        self.request_sent_time = {}
        self.coordinator_pid = None # Para o algoritmo de eleição de coordenador
        self.is_coordinator = False
        self.election_in_progress = False
        self.election_timer = None
        self.bully_election_timeout = 5 # Timeout para eleição Bully
        self.bully_higher_pids = [] # PIDs maiores para eleição Bully
        self.bully_replies_received = 0
        self.bully_coordinator_announced = False
        self.election_messages_received = collections.deque()
        self.election_message_lock = threading.Lock()
        self.lock = threading.Lock()

    def update_peers(self):
        """Atualiza a lista de peers a partir do nameserver, ignorando o nameserver"""
        with Pyro5.api.locate_ns() as ns:
            current_peers = []
            current_peers_names = []
            for name, uri in ns.list().items():
                if name.startswith("ricart.") and name != self.pid:  # só processos válidos
                    current_peers.append(uri)
                    current_peers_names.append(name)

            # Adiciona novos peers e remove os que não estão mais no nameserver
            for name, uri in zip(current_peers_names, current_peers):
                if name not in self.active_peers:
                    self.active_peers[name] = Pyro5.api.Proxy(uri)
                    self.last_heartbeat[name] = time.time()
            
            # Remove peers que não estão mais ativos
            peers_to_remove = [p for p in self.active_peers if p not in current_peers_names]
            for p in peers_to_remove:
                del self.active_peers[p]
                if p in self.last_heartbeat: del self.last_heartbeat[p]

            self.peers = list(self.active_peers.values())
            self.peersNames = list(self.active_peers.keys())
        print(f"[{self.pid}] Peers atualizados: {self.peersNames}")

    def send_heartbeat(self):
        """Envia heartbeat para todos os peers ativos."""
        for peer_name, peer_proxy in self.active_peers.items():
            try:
                peer_proxy.receive_heartbeat(self.pid)
            except Exception as e:
                print(f"[{self.pid}] Falha ao enviar heartbeat para {peer_name}: {e}")
                self.remove_peer(peer_name)

    def receive_heartbeat(self, sender_pid):
        """Recebe heartbeat de outro processo."""
        with self.lock:
            if sender_pid in self.last_heartbeat:
                self.last_heartbeat[sender_pid] = time.time()

    def check_heartbeats(self):
        """Verifica heartbeats e remove peers inativos."""
        peers_to_check = list(self.last_heartbeat.keys())
        for peer_name in peers_to_check:
            if time.time() - self.last_heartbeat[peer_name] > self.heartbeat_timeout:
                print(f"[{self.pid}] Peer {peer_name} inativo (timeout de heartbeat). Removendo.")
                self.remove_peer(peer_name)

    def remove_peer(self, peer_name):
        """Remove um peer da lista de peers ativos."""
        with self.lock:
            if peer_name in self.active_peers:
                del self.active_peers[peer_name]
            if peer_name in self.last_heartbeat:
                del self.last_heartbeat[peer_name]
            self.peers = list(self.active_peers.values())
            self.peersNames = list(self.active_peers.keys())
            print(f"[{self.pid}] Peer {peer_name} removido. Peers restantes: {self.peersNames}")

    def request_resource(self):
        """Solicita o recurso a todos os peers"""
        self.state = "WANTED"
        self.timestamp = time.time()
        self.replies = 0
        print(f"[{self.pid}] Pedindo recurso...")

        self.request_sent_time = {name: time.time() for name in self.peersNames}
        expected_replies = len(self.peersNames)

        for peer_name, peer_proxy in self.active_peers.items():
            try:
                peer_proxy.request(self.timestamp, self.pid)
            except Exception as e:
                print(f"[{self.pid}] Peer {peer_name} não disponível ao enviar requisição ({e}). Removendo.")
                self.remove_peer(peer_name)
                expected_replies -= 1

        # Espera todos os replies válidos ou timeout
        start_wait_time = time.time()
        while self.replies < expected_replies and (time.time() - start_wait_time < self.request_timeout):
            # Verifica se algum peer que enviamos a requisição falhou durante a espera
            peers_to_check_for_timeout = list(self.request_sent_time.keys())
            for p_name in peers_to_check_for_timeout:
                if p_name not in self.active_peers: # Peer já foi removido por heartbeat
                    if p_name in self.request_sent_time: del self.request_sent_time[p_name]
                    expected_replies -= 1
                    continue
                if time.time() - self.request_sent_time[p_name] > self.request_timeout:
                    print(f"[{self.pid}] Timeout ao esperar resposta de {p_name}. Removendo.")
                    self.remove_peer(p_name)
                    if p_name in self.request_sent_time: del self.request_sent_time[p_name]
                    expected_replies -= 1

            if self.replies < expected_replies:
                time.sleep(0.05)

        if self.replies < expected_replies:
            print(f"[{self.pid}] Não recebeu todas as respostas dentro do tempo limite. Prosseguindo com {self.replies}/{expected_replies} respostas.")

        if self.replies < len(self.active_peers): # Se não recebeu de todos os ativos, pode ser um problema
            print(f"[{self.pid}] Aviso: Não recebeu respostas de todos os peers ativos. Pode haver falhas não detectadas ou atrasos.")

        self.state = "HELD"
        print(f"[{self.pid}] Recurso adquirido. Acessando por {self.resource_access_time} segundos...")
        time.sleep(self.resource_access_time) # Simula o acesso ao recurso
        print(f"[{self.pid}] Tempo de acesso ao recurso expirado.")
        self.release_resource()

    def release_resource(self):
        """Libera o recurso e responde pedidos adiados"""
        self.state = "RELEASED"
        print(f"[{self.pid}] Liberando recurso.")

        for (ts, pid) in self.deferred:
            if pid in self.active_peers:
                try:
                    self.active_peers[pid].reply(self.pid)
                except Exception as e:
                    print(f"[{self.pid}] Falha ao responder {pid} (peer inativo ou erro): {e}")
                    self.remove_peer(pid)
            else:
                print(f"[{self.pid}] Peer {pid} não está mais ativo, não enviando resposta adiada.")

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
                    if pid in self.active_peers:
                        self.active_peers[pid].reply(self.pid)
                    else:
                        print(f"[{self.pid}] Peer {pid} inativo, não respondendo à requisição.")
                except Exception as e:
                    print(f"[{self.pid}] Falha ao responder {pid} imediatamente ({e})")

    def reply(self, pid):
        """Recebe resposta de outro processo"""
        with self.lock:
            self.replies += 1
            print(f"[{self.pid}] Resposta recebida de {pid} ({self.replies}/{len(self.peers)}).")

    @Pyro5.api.expose
    def start_election(self, sender_pid):
        """Inicia uma eleição ou responde a uma eleição."""
        with self.lock:
            print(f"[{self.pid}] Recebeu mensagem de eleição de {sender_pid}.")
            # Se o PID do remetente for menor que o meu, respondo com 'OK' e inicio minha própria eleição
            if self.pid > sender_pid:
                try:
                    Pyro5.api.Proxy(f"PYRONAME:{sender_pid}").election_ok(self.pid)
                except Exception as e:
                    print(f"[{self.pid}] Falha ao enviar OK para {sender_pid}: {e}")
                    self.remove_peer(sender_pid)
                if not self.election_in_progress:
                    self.initiate_election()
            else:
                # Se o PID do remetente for maior, apenas registro que recebi a mensagem
                self.election_messages_received.append(sender_pid)

    @Pyro5.api.expose
    def election_ok(self, sender_pid):
        """Recebe um 'OK' em resposta a uma mensagem de eleição."""
        with self.lock:
            print(f"[{self.pid}] Recebeu OK de {sender_pid} na eleição.")
            self.bully_replies_received += 1

    @Pyro5.api.expose
    def coordinator_elected(self, coordinator_pid):
        """Recebe a mensagem de que um coordenador foi eleito."""
        with self.lock:
            print(f"[{self.pid}] Coordenador {coordinator_pid} eleito.")
            self.coordinator_pid = coordinator_pid
            self.election_in_progress = False
            self.is_coordinator = (self.pid == coordinator_pid)
            self.bully_coordinator_announced = True
            if self.election_timer:
                self.election_timer.cancel()
                self.election_timer = None

    def initiate_election(self):
        """Inicia o processo de eleição Bully."""
        with self.lock:
            if self.election_in_progress: # Evita múltiplas eleições simultâneas
                return

            print(f"[{self.pid}] Iniciando eleição Bully...")
            self.election_in_progress = True
            self.bully_replies_received = 0
            self.bully_coordinator_announced = False
            self.bully_higher_pids = [p for p in self.peersNames if p > self.pid]

            if not self.bully_higher_pids: # Eu sou o maior PID ativo
                self.coordinator_pid = self.pid
                self.is_coordinator = True
                self.election_in_progress = False
                print(f"[{self.pid}] Eu sou o novo coordenador!")
                self.announce_coordinator(self.pid)
            else:
                # Envia mensagem de eleição para todos os peers com PID maior
                for peer_name in self.bully_higher_pids:
                    if peer_name in self.active_peers:
                        try:
                            self.active_peers[peer_name].start_election(self.pid)
                        except Exception as e:
                            print(f"[{self.pid}] Falha ao enviar eleição para {peer_name}: {e}")
                            self.remove_peer(peer_name)

                # Inicia temporizador para esperar respostas
                self.election_timer = threading.Timer(self.bully_election_timeout, self.check_election_results)
                self.election_timer.start()

    def check_election_results(self):
        """Verifica os resultados da eleição após o timeout."""
        with self.lock:
            if self.bully_coordinator_announced: # Já recebeu anúncio de coordenador
                return

            if self.bully_replies_received == len(self.bully_higher_pids): # Recebeu OK de todos os maiores
                # Alguém maior que eu deve ter se tornado coordenador e anunciado
                # Se não recebi o anúncio, ele falhou. Inicio nova eleição.
                print(f"[{self.pid}] Recebeu OK de todos os peers maiores, mas nenhum anúncio de coordenador. Iniciando nova eleição.")
                self.election_in_progress = False # Reset para iniciar nova eleição
                self.initiate_election()
            else:
                # Não recebeu OK de todos os maiores, ou não recebeu nenhum OK
                # Isso significa que os maiores falharam ou não responderam.
                # Eu sou o maior entre os ativos, então me torno coordenador.
                print(f"[{self.pid}] Não recebeu OK de todos os peers maiores. Assumindo coordenação.")
                self.coordinator_pid = self.pid
                self.is_coordinator = True
                self.election_in_progress = False
                print(f"[{self.pid}] Eu sou o novo coordenador!")
                self.announce_coordinator(self.pid)

    def announce_coordinator(self, coordinator_pid):
        """Anuncia o coordenador eleito para todos os peers."""
        for peer_name, peer_proxy in self.active_peers.items():
            try:
                peer_proxy.coordinator_elected(coordinator_pid)
            except Exception as e:
                print(f"[{self.pid}] Falha ao anunciar coordenador para {peer_name}: {e}")
                self.remove_peer(peer_name)

    def check_coordinator_status(self):
        """Verifica se o coordenador está ativo. Se não, inicia uma eleição."""
        if self.is_coordinator:
            return

        if self.coordinator_pid is None or self.coordinator_pid not in self.active_peers:
            print(f"[{self.pid}] Coordenador {self.coordinator_pid} não encontrado ou inativo. Iniciando eleição.")
            self.initiate_election()
        else:
            # Tenta pingar o coordenador para verificar se está vivo
            try:
                self.active_peers[self.coordinator_pid].ping()
            except Exception as e:
                print(f"[{self.pid}] Coordenador {self.coordinator_pid} falhou ao pingar: {e}. Iniciando eleição.")
                self.remove_peer(self.coordinator_pid)
                self.initiate_election()

    @Pyro5.api.expose
    def ping(self):
        """Método simples para verificar a atividade do processo."""
        return True

def send_heartbeats_periodically(process):
    while True:
        process.send_heartbeat()
        time.sleep(process.heartbeat_interval)

def check_heartbeats_periodically(process):
    while True:
        process.check_heartbeats()
        time.sleep(process.heartbeat_interval)

def check_coordinator_periodically(process):
    while True:
        process.check_coordinator_status()
        time.sleep(process.heartbeat_interval * 2) # Verifica o coordenador com menos frequência

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
        print("Uso: python agrawala.py <NOME_DO_PROCESSO>")
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

    # Inicia threads para heartbeat e verificação
    heartbeat_thread = threading.Thread(target=send_heartbeats_periodically, args=(process,), daemon=True)
    heartbeat_thread.start()

    check_heartbeat_thread = threading.Thread(target=check_heartbeats_periodically, args=(process,), daemon=True)
    check_heartbeat_thread.start()

    # Inicia thread para verificar o coordenador e iniciar eleição se necessário
    check_coordinator_thread = threading.Thread(target=check_coordinator_periodically, args=(process,), daemon=True)
    check_coordinator_thread.start()

    # interface de usuário
    interface(process)

if __name__ == "__main__":
    main()
