import socket
import threading
import time
import zlib
from Config import *
from Utils import *

class ClienteUDP:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', 0))  # Porta aleatória local
        self.servidor_addr = (IP_SERVIDOR, PORTA_SERVIDOR)
        
        self.arquivo_sendo_recebido = None
        self.running = True

    def iniciar(self):
        """Inicia o listener e prepara o cliente."""
        self.thread_listener = threading.Thread(target=self._escutar, daemon=True)
        self.thread_listener.start()
        print(f"[*] Cliente pronto e vinculado na porta {self.sock.getsockname()[1]}")

    def solicitar_arquivo(self, nome_arquivo):
        """Envia a requisição inicial para o servidor."""
        print(f"[*] Solicitando arquivo: {nome_arquivo}")
        try:
            pacote = criar_pacote(REQ, 0, nome_arquivo.encode())
            self.sock.sendto(pacote, self.servidor_addr)
            
            # Loop para manter o programa principal vivo enquanto recebe
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._finalizar()

    def _escutar(self):
        """Loop da thread que recebe os pacotes do servidor."""
        buffer_size = TAMANHO_PAYLOAD + TAMANHO_HEADER
        while self.running:
            try:
                pacote, addr = self.sock.recvfrom(buffer_size)
                self._processar_pacote(pacote, addr)
            except Exception as e:
                if self.running:
                    print(f"[!] Erro no listener do cliente: {e}")
                break

    def _processar_pacote(self, pacote, addr):
        """Lógica de decisão para cada pacote recebido."""
        tipo, seq, checksum, tamanho, payload = desempacotar_pacote(pacote)

        if tipo == DADO:
            self._tratar_dados(payload, seq, checksum, addr)
        
        elif tipo == ACK:
            # O ACK aqui geralmente é a confirmação do Handshake (Arquivo encontrado)
            print(f"[ACK] Servidor confirmou a requisição: {payload.decode()}")
            
        elif tipo == FIM:
            self._finalizar_arquivo()
            
        elif tipo == ERRO:
            print(f"\n[ERRO SERVIDOR] {payload.decode()}")
            self.running = False

    def _tratar_dados(self, payload, seq, checksum, addr):
        """Valida, grava os dados no disco e responde com ACK."""
        # 1. Validação de Integridade
        if zlib.crc32(payload) != checksum:
            print(f"[!] Checksum inválido no pacote {seq}. Descartando...")
            return

        # 2. Abertura do arquivo (Lazy Loading)
        if self.arquivo_sendo_recebido is None:
            # Nome fixo ou baseado na requisição
            self.arquivo_sendo_recebido = open("resultado_transferencia.png", "wb")
        
        # 3. Gravação
        self.arquivo_sendo_recebido.write(payload)
        print(f"[RECV] Segmento {seq} recebido e gravado.")

        # 4. Envio do ACK
        pacote_ack = criar_pacote(ACK, seq, b"")
        self.sock.sendto(pacote_ack, addr)

    def _finalizar_arquivo(self):
        """Fecha o arquivo e encerra o processo de download."""
        if self.arquivo_sendo_recebido:
            self.arquivo_sendo_recebido.close()
            self.arquivo_sendo_recebido = None
        print("\n[SUCESSO] Arquivo recebido completamente!")
        self.running = False

    def _finalizar(self):
        print("\n[*] Encerrando cliente...")
        self.running = False
        self.sock.close()

# --- Execução ---
if __name__ == "__main__":
    cliente = ClienteUDP()
    cliente.iniciar()
    cliente.solicitar_arquivo("MrPenis.png")