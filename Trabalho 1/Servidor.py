import socket
import threading
import os
import time
from Config import *
from Utils import *

class ServidorUDP:
    def __init__(self):
        # 1. Configurações Iniciais
        self.host = IP_SERVIDOR
        self.port = PORTA_SERVIDOR
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        
        # Controle de estado (futuramente útil para múltiplas conexões)
        self.running = True

    def iniciar(self):
        """Inicia a thread de escuta e o loop principal."""
        self.listener_thread = threading.Thread(target=self._escutar, daemon=True)
        self.listener_thread.start()
        print(f"[*] Servidor iniciado em {self.host}:{self.port}")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[!] Desligando servidor...")
            self.running = False

    def _escutar(self):
        """Método interno que roda na thread de escuta."""
        while self.running:
            try:
                pacote, addr = self.sock.recvfrom(TAMANHO_PAYLOAD + TAMANHO_HEADER)
                self._processar_pacote(pacote, addr)
            except Exception as e:
                if self.running:
                    print(f"[!] Erro no listener: {e}")
                break

    def _processar_pacote(self, pacote, addr):
        tipo, seq, checksum, tamanho, payload = desempacotar_pacote(pacote)

        if tipo == REQ:
            self._processar_requisicao(payload, addr)
        elif tipo == ACK:
            print(f"[ACK] Recebido de {addr} para seq {seq}")
            # Aqui você vai disparar um evento para o Stop-and-Wait futuramente
        elif tipo == ERRO:
            print(f"[ERRO] Recebido de {addr}")
        else:
            print(f"[?] Tipo desconhecido ({tipo}) recebido de {addr}")

    def _processar_requisicao(self, payload, addr):
        nome_arquivo = payload.decode()
        path = os.path.join("Data", nome_arquivo)
        
        print(f"[REQ] Arquivo: {nome_arquivo} pedido por {addr}")
        
        if os.path.exists(path):
            # Handshake de confirmação
            confirmacao = criar_pacote(ACK, 0, "OK".encode())
            self.sock.sendto(confirmacao, addr)
            
            # Dispara o envio em uma thread separada para não travar o servidor
            threading.Thread(target=self._enviar_arquivo, args=(addr, path), daemon=True).start()
        else:
            erro = criar_pacote(ERRO, 0, "Arquivo nao encontrado".encode())
            self.sock.sendto(erro, addr)

    def _enviar_arquivo(self, addr, path):
        print(f"[SEND] Iniciando envio: {path}")
        
        try:
            with open(path, "rb") as f:
                seq = 0
                while True:
                    dados = f.read(TAMANHO_PAYLOAD)
                    if not dados:
                        break
                    
                    pacote = criar_pacote(DADO, seq, dados)
                    self.sock.sendto(pacote, addr)
                    print(f"[SEND] Enviado segmento {seq}")
                    
                    # --- AQUI ENTRARÁ O STOP-AND-WAIT ---
                    # Por enquanto, apenas um pequeno delay
                    time.sleep(0.01)
                    seq += 1

            # Envia FIM
            pacote_fim = criar_pacote(FIM, 0, b"")
            self.sock.sendto(pacote_fim, addr)
            print(f"[DONE] Envio de {path} concluído.")
            
        except Exception as e:
            print(f"[!] Erro ao enviar arquivo: {e}")

# --- Execução ---
if __name__ == "__main__":
    servidor = ServidorUDP()
    servidor.iniciar()