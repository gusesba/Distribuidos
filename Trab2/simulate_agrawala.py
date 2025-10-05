import subprocess
import time
import os
import signal

def start_agrawala_process(pid_name):
    print(f"Iniciando processo Agrawala: {pid_name}...")
    # Inicia cada processo Agrawala em um processo separado
    # Usamos um arquivo de log para capturar a saída de cada processo
    log_file = open(f"log_{pid_name}.txt", "w")
    # Removido preexec_fn=os.setsid para compatibilidade com Windows
    process = subprocess.Popen(["python", "agrawala.py", pid_name.split(".")[-1]], stdout=log_file, stderr=log_file)
    print(f"Processo {pid_name} iniciado com PID: {process.pid}")
    return process, log_file

def main():
    processes = []
    log_files = []
    pid_names = ["ricart.PeerA", "ricart.PeerB", "ricart.PeerC", "ricart.PeerD"]

    # O Name Server deve ser iniciado manualmente em um terminal separado antes de executar este script.
    # Exemplo: python -m Pyro5.nameserver

    # 2. Iniciar os processos Agrawala
    for pid_name in pid_names:
        p, lf = start_agrawala_process(pid_name)
        processes.append(p)
        log_files.append(lf)
    
    time.sleep(5) # Dá um tempo para todos os processos se registrarem e atualizarem seus peers

    print("\nSimulação iniciada. Os processos estão rodando em segundo plano.")
    print("Verifique os arquivos log_PeerX.txt para a saída de cada processo.")
    print("Para testar a exclusão mútua, você pode interagir com os processos via terminal.")
    print("Exemplo: Em um novo terminal, execute: python agrawala.py PeerA")
    print("E então digite 'p' para pedir o recurso.")
    print("Você pode tentar matar um processo (ex: PeerC) para testar a eleição de coordenador e detecção de falhas.")
    print("Para parar a simulação, pressione Ctrl+C.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nEncerrando simulação...")
    finally:
        # Encerrar todos os processos
        for p in processes:
            if p.poll() is None: # Se o processo ainda estiver rodando
                p.terminate()
                p.wait()
        for lf in log_files:
            lf.close()
        print("Simulação encerrada. Arquivos de log fechados.")

if __name__ == "__main__":
    main()
