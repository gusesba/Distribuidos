# TRablho 2
#Alunos: Márcia Eliana Ferreira
# Gustavo Esmanhoto Bareta

import Pyro5.api
import threading
import time
import sys
import re

@Pyro5.api.expose
class AgrawalaProcess:
    def __init__(self, pid):
        self.pid = pid
        self.state = "RELEASED"
        self.timestamp = 0
        self.replies = 0
        self.deferred = []
        self.lock = threading.Lock()

        # Configurações
        self.resource_access_time = 5
        self.heartbeat_interval = 2
        self.heartbeat_timeout = 6  # Aumentado para maior tolerância em redes lentas
        self.request_timeout = 10

        # Estado compartilhado: Armazena URIs em vez de proxies para segurança de thread.
        self.active_peers_uris = {}
        self.last_heartbeat = {}

    def get_proxy(self, peer_name):
        """Cria um proxy para um peer de forma segura, usando sua URI."""
        with self.lock:
            uri = self.active_peers_uris.get(peer_name)
        if uri:
            try:
                # Cria um novo proxy. Essencial para segurança entre threads.
                return Pyro5.api.Proxy(uri)
            except Exception as e:
                print(f"[{self.pid}] Erro ao criar proxy para {peer_name}: {e}")
        return None

    def update_peers(self):
        """Atualiza a lista de URIs de peers a partir do nameserver."""
        try:
            with Pyro5.api.locate_ns() as ns:
                # Filtra por nomes no formato 'ricart.PeerX', excluindo a si mesmo.
                peer_uris = {name: uri for name, uri in ns.list().items()
                             if re.match(r'^ricart\.Peer[A-Za-z0-9]+$', name) and name != self.pid}

            with self.lock:
                self.active_peers_uris = peer_uris
                # Remove heartbeats de peers que não estão mais registrados
                stale_heartbeats = [p_name for p_name in self.last_heartbeat if p_name not in self.active_peers_uris]
                for p_name in stale_heartbeats:
                    del self.last_heartbeat[p_name]
                # Adiciona novos peers ao rastreamento de heartbeat
                for p_name in self.active_peers_uris:
                    if p_name not in self.last_heartbeat:
                        self.last_heartbeat[p_name] = time.time()

        except Exception as e:
            # Evita que o programa pare se o nameserver estiver temporariamente indisponível
            print(f"[{self.pid}] Falha ao conectar com o Name Server durante a atualização de peers: {e}")

    def periodic_updater(self):
        """Thread única que executa tarefas de manutenção periodicamente."""
        while True:
            self.update_peers()
            self.check_heartbeats()
            self.send_heartbeat()
            time.sleep(self.heartbeat_interval)

    def send_heartbeat(self):
        """Envia um sinal de 'vida' (heartbeat) para todos os peers ativos."""
        with self.lock:
            peers_to_send = list(self.active_peers_uris.keys())

        for peer_name in peers_to_send:
            proxy = self.get_proxy(peer_name)
            if proxy:
                try:
                    proxy.receive_heartbeat(self.pid)
                except Exception:
                    # A falha será detectada pelo timeout no 'check_heartbeats'
                    pass

    @Pyro5.api.oneway
    def receive_heartbeat(self, sender_pid):
        """Registra a recepção de um heartbeat de outro processo."""
        with self.lock:
            if sender_pid in self.active_peers_uris:
                self.last_heartbeat[sender_pid] = time.time()

    def check_heartbeats(self):
        """Verifica se algum peer ficou inativo (não enviou heartbeat a tempo)."""
        with self.lock:
            peers_to_check = list(self.last_heartbeat.items())
            now = time.time()
            removed_peers = []
            for peer_name, last_seen in peers_to_check:
                if now - last_seen > self.heartbeat_timeout:
                    removed_peers.append(peer_name)
            
            if removed_peers:
                print(f"[{self.pid}] Peers inativos por timeout de heartbeat: {removed_peers}. Removendo.")
                for peer_name in removed_peers:
                    if peer_name in self.active_peers_uris:
                        del self.active_peers_uris[peer_name]
                    if peer_name in self.last_heartbeat:
                        del self.last_heartbeat[peer_name]
                print(f"[{self.pid}] Peers ativos após remoção: {list(self.active_peers_uris.keys())}")

    def request_resource(self):
        """Inicia o processo de solicitação para acessar o recurso crítico."""
        with self.lock:
            self.state = "WANTED"
            self.timestamp = time.time()
            self.replies = 0
            peers_to_request = list(self.active_peers_uris.keys())

        print(f"[{self.pid}] Pedindo recurso para {len(peers_to_request)} peers...")
        if not peers_to_request:
            print(f"[{self.pid}] Nenhum peer ativo. Adquirindo recurso localmente.")
            self.acquire_and_use_resource()
            return

        expected_replies = len(peers_to_request)
        for peer_name in peers_to_request:
            proxy = self.get_proxy(peer_name)
            if proxy:
                try:
                    proxy.request(self.timestamp, self.pid)
                except Exception as e:
                    print(f"[{self.pid}] Falha ao enviar requisição para {peer_name}: {e}")
                    with self.lock:
                        expected_replies -= 1
            else:
                 with self.lock:
                    expected_replies -= 1

        # Aguarda por respostas ou até o timeout
        start_wait_time = time.time()
        while time.time() - start_wait_time < self.request_timeout:
            with self.lock:
                if self.replies >= expected_replies:
                    break
            time.sleep(0.1)

        with self.lock:
            if self.replies < expected_replies:
                print(f"[{self.pid}] Timeout! Recebeu {self.replies}/{expected_replies} respostas. Assumindo posse do recurso.")

        self.acquire_and_use_resource()

    def acquire_and_use_resource(self):
        """Método interno para entrar na seção crítica."""
        with self.lock:
            self.state = "HELD"
        print(f"[{self.pid}] Recurso adquirido! Acessando por {self.resource_access_time} segundos...")
        time.sleep(self.resource_access_time)
        self.release_resource()

    def release_resource(self):
        """Libera o recurso e responde a todos os pedidos que foram adiados."""
        with self.lock:
            self.state = "RELEASED"
            deferred_copy = list(self.deferred)
            self.deferred = []

        print(f"[{self.pid}] Recurso liberado. Respondendo a {len(deferred_copy)} pedidos adiados.")
        for (ts, pid) in deferred_copy:
            proxy = self.get_proxy(pid)
            if proxy:
                try:
                    proxy.reply(self.pid)
                except Exception as e:
                    print(f"[{self.pid}] Falha ao responder a {pid} (adiado): {e}")

    @Pyro5.api.oneway
    def request(self, ts, pid):
        """Recebe um pedido de recurso de outro processo."""
        with self.lock:
            # Lógica do algoritmo de Ricart-Agrawala para decidir se responde ou adia
            should_defer = (self.state == "HELD" or
                           (self.state == "WANTED" and (self.timestamp, self.pid) < (ts, pid)))
            if should_defer:
                self.deferred.append((ts, pid))
                print(f"[{self.pid}] Pedido de {pid} adiado.")
                return

        # Responde imediatamente se não precisar adiar
        try:
            with Pyro5.api.Proxy(f"PYRONAME:{pid}") as peer_proxy:
                peer_proxy.reply(self.pid)
        except Exception as e:
            print(f"[{self.pid}] Falha ao responder a {pid} (imediato): {e}")

    @Pyro5.api.oneway
    def reply(self, pid):
        """Recebe uma resposta (permissão) de outro processo."""
        with self.lock:
            self.replies += 1
            # A contagem de `expected_replies` é local ao método `request_resource`
            print(f"[{self.pid}] Resposta recebida de {pid} ({self.replies} recebidas).")

# --- Ponto de Entrada e Interface de Usuário ---
def main():
    if len(sys.argv) < 2:
        print("Uso: python agrawala_corrigido.py <NomeDoPeer>")
        print("Exemplo: python agrawala_corrigido.py PeerA")
        sys.exit(1)

    # O nome do peer é passado como argumento (ex: PeerA)
    peer_name_arg = sys.argv[1]
    pid = f"ricart.{peer_name_arg}"
    process = AgrawalaProcess(pid)

    # Configuração do servidor Pyro para este processo
    try:
        daemon = Pyro5.api.Daemon()
        uri = daemon.register(process)
        with Pyro5.api.locate_ns() as ns:
            ns.register(pid, uri)
        print(f"[{pid}] registrado no nameserver com sucesso.")
    except Exception as e:
        print(f"[{pid}] Erro na inicialização do Pyro. O Name Server está rodando? Erro: {e}")
        sys.exit(1)

    # Inicia thread para tarefas de fundo (heartbeats, etc.)
    updater_thread = threading.Thread(target=process.periodic_updater, daemon=True)
    updater_thread.start()

    # Inicia thread para o loop de requisições do Pyro
    daemon_thread = threading.Thread(target=daemon.requestLoop, daemon=True)
    daemon_thread.start()

    # Loop de interação com o usuário
    print(f"--- Processo {pid} iniciado. ---")
    while True:
        try:
            with process.lock:
                current_state = process.state
            
            cmd = input(f"\n[{pid} | {current_state}] Digite [p] para pedir recurso, ou [q] para sair: ").strip().lower()
            
            if cmd == 'p':
                with process.lock:
                    state = process.state
                if state == "RELEASED":
                    # A chamada a request_resource é blocante, então a executamos em uma nova thread
                    # para não travar a interface de usuário.
                    threading.Thread(target=process.request_resource).start()
                else:
                    print(f"[{pid}] Ação negada. Estado atual é {state}, não 'RELEASED'.")
            elif cmd == 'q':
                print(f"[{pid}] Encerrando...")
                break
            else:
                print("Comando inválido. Use 'p' ou 'q'.")
        except (KeyboardInterrupt, EOFError):
            print(f"\n[{pid}] Encerrando por interrupção.")
            break

if __name__ == "__main__":
    main()

