import socket
import threading
import os
import time
import struct
import zlib
import random
from collections import deque

# --- CONFIGURAÇÕES E UTILS INTEGRADOS ---
IP_SERVIDOR = '192.168.1.100' # Alterado para localhost para facilitar testes
PORTA_SERVIDOR = 5000
TAMANHO_PAYLOAD = 1024
TIMEOUT_REDE = 5.0
FORMATO_HEADER = '!BIIH'
TAMANHO_HEADER = struct.calcsize(FORMATO_HEADER)
CHANCE_CORRUPCAO = 0.05  # 5% de chance de erro

REQ, ACK, ERRO, DADO, FIM = 0, 1, 2, 3, 4

def criar_pacote(tipo, seq, dados=b""):
    checksum = zlib.crc32(dados)
    header = struct.pack(FORMATO_HEADER, tipo, seq, checksum, len(dados))
    return header + dados

def desempacotar_pacote(pacote):
    header_bruto = pacote[:TAMANHO_HEADER]
    payload = pacote[TAMANHO_HEADER:]
    tipo, seq, checksum, tamanho = struct.unpack(FORMATO_HEADER, header_bruto)
    return tipo, seq, checksum, tamanho, payload

# --- CLASSE DO SERVIDOR ---
class ServidorUDP:
    def __init__(self):
        self.host = IP_SERVIDOR
        self.port = PORTA_SERVIDOR
        self.base_dir = "Servidor-data"

        self.transferencias_ativas = {} # Dicionário: { (ip, porta): (Event, ultimo_ack) }
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.ack_event = threading.Event()
        self.ultimo_ack_recebido = -1
        self.running = True

        self.tamanho_janela = 5
        self.janela = deque()
        self.proxima_seq_leitura = 0
        self.ack_event = threading.Event()
        self.ultimo_ack_recebido = -1
        
        # Garante que a pasta de dados do servidor existe
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.running = True

    def iniciar(self):
        self.listener_thread = threading.Thread(target=self._escutar, daemon=True)
        self.listener_thread.start()
        print(f"[*] Servidor iniciado em {self.host}:{self.port}")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False

    def _escutar(self):
        while self.running:
            try:
                pacote, addr = self.sock.recvfrom(TAMANHO_PAYLOAD + TAMANHO_HEADER)
                self._processar_pacote(pacote, addr)
            except:
                break

    def _processar_pacote(self, pacote, addr):
        tipo, seq, checksum, tamanho, payload = desempacotar_pacote(pacote)

        if tipo == REQ:
            self._processar_requisicao(payload, addr)
        elif tipo == ACK:
            if addr in self.transferencias_ativas:
                event, ack_ref = self.transferencias_ativas[addr]
                ack_ref[0] = seq # Atualiza o valor do ACK na lista mutável
                event.set() # Acorda a thread específica desse cliente

    def _processar_requisicao(self, payload, addr):
        nome_arquivo = payload.decode()
        path = os.path.join(self.base_dir, nome_arquivo)
        
        if os.path.exists(path):
            # Criamos uma thread e passamos o ADDR. 
            # NÃO usamos variáveis de self. lá dentro para controle de envio.
            threading.Thread(target=self._enviar_arquivo, args=(addr, path), daemon=True).start()
        else:
            erro = criar_pacote(ERRO, 0, "Arquivo nao encontrado".encode())
            self.sock.sendto(erro, addr)

    def _enviar_arquivo(self, addr, path):
        # --- VARIÁVEIS LOCAIS (ISOLADAS POR THREAD) ---
        janela_local = deque()
        proxima_seq_leitura = 0
        ultimo_ack_recebido = [-1] # Usamos uma lista para ser mutável dentro de outra thread
        ack_event_local = threading.Event()

        # Precisamos de uma forma de o listener avisar ESTA thread que o ACK chegou
        # Vamos registrar este cliente em um dicionário global temporário
        self.transferencias_ativas[addr] = (ack_event_local, ultimo_ack_recebido)

        print(f"[GBN] Iniciando envio para {addr} | Arquivo: {os.path.basename(path)}")
        
        try:
            with open(path, "rb") as f:
                # Carga inicial
                while len(janela_local) < self.tamanho_janela:
                    dados = f.read(TAMANHO_PAYLOAD)
                    if not dados: break
                    janela_local.append(criar_pacote(DADO, proxima_seq_leitura, dados))
                    proxima_seq_leitura += 1

                while janela_local:
                    for pacote in janela_local:
                        _, s, _, _, _ = desempacotar_pacote(pacote)
                        envio = pacote
                        if random.random() < CHANCE_CORRUPCAO:
                            print(f"[!] Corrompendo propositalmente seq {s}")
                            envio = self._corromper_pacote(pacote)
                        print(f"[SEND] Segmento {s} enviado")
                        self.sock.sendto(envio, addr)

                    ack_event_local.clear()
                    if ack_event_local.wait(timeout=0.2):
                        while janela_local:
                            _, s_base, _, _, _ = desempacotar_pacote(janela_local[0])
                            if s_base < ultimo_ack_recebido[0]:
                                confirmado = janela_local.popleft()
                                _, s_conf, _, _, _ = desempacotar_pacote(confirmado)
                                print(f"[OK] Segmento {s_conf} confirmado. Deslizando...")

                                novos_dados = f.read(TAMANHO_PAYLOAD)
                                if novos_dados:
                                    janela_local.append(criar_pacote(DADO, proxima_seq_leitura, novos_dados))
                                    proxima_seq_leitura += 1
                            else:
                                print(f"[NACK] O cliente pediu o reenvio do segmento {desempacotar_pacote(janela_local[0])[1]}. Voltando N...")
                                break
                    else:
                        print(f"[TIMEOUT] Estourou na base {desempacotar_pacote(janela_local[0])[1]}. Voltando N...")
                        break
                
                self.sock.sendto(criar_pacote(FIM, 0, b""), addr)
        finally:
            # Limpa o registro ao terminar
            del self.transferencias_ativas[addr]

    def _corromper_pacote(self, pacote):
        """Altera um byte aleatório no payload para simular erro de rede."""
        lista_bytes = list(pacote)
        # Escolhe um índice aleatório dentro do payload (após o header)
        if len(lista_bytes) > TAMANHO_HEADER:
            indice = random.randint(TAMANHO_HEADER, len(lista_bytes) - 1)
            # Inverte o byte ou muda para um valor fixo
            lista_bytes[indice] = (lista_bytes[indice] + 1) % 256
        return bytes(lista_bytes)

if __name__ == "__main__":
    ServidorUDP().iniciar()