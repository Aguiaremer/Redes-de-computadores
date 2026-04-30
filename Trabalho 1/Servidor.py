import socket
import threading
import os
import time
import struct
import zlib
import random
from collections import deque

# --- CONFIGURAÇÕES ---
IP_SERVIDOR = '192.168.1.100'
PORTA_SERVIDOR = 5000
TAMANHO_PAYLOAD = 1024
FORMATO_HEADER = '!BIIH'
TAMANHO_HEADER = struct.calcsize(FORMATO_HEADER)
CHANCE_CORRUPCAO = 0.001

# Tipos de Pacotes
REQ, ACK, ERRO, DADO, FIM, SYN, SYN_ACK = 0, 1, 2, 3, 4, 5, 6

def criar_pacote(tipo, seq, dados=b""):
    checksum = zlib.crc32(dados)
    header = struct.pack(FORMATO_HEADER, tipo, seq, checksum, len(dados))
    return header + dados

def desempacotar_pacote(pacote):
    header_bruto = pacote[:TAMANHO_HEADER]
    payload = pacote[TAMANHO_HEADER:]
    tipo, seq, checksum, tamanho = struct.unpack(FORMATO_HEADER, header_bruto)
    return tipo, seq, checksum, tamanho, payload

class ServidorUDP:
    def __init__(self):
        self.base_dir = "Servidor-data"
        if not os.path.exists(self.base_dir): os.makedirs(self.base_dir)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((IP_SERVIDOR, PORTA_SERVIDOR))
        self.sock.settimeout(1.0) 
        
        self.clientes_autenticados = set()
        self.transferencias_ativas = {} 
        self.running = True

    def iniciar(self):
        print(f"[*] SERVIDOR ONLINE EM {PORTA_SERVIDOR}")
        print(f"[*] AGUARDANDO CONEXOES... (Ctrl+C para encerrar)")
        print("-" * 50)
        
        try:
            while self.running:
                try:
                    pacote, addr = self.sock.recvfrom(TAMANHO_PAYLOAD + TAMANHO_HEADER)
                    self._processar_pacote(pacote, addr)
                except socket.timeout:
                    continue
        except KeyboardInterrupt:
            print("\n\n[!] SINAL DE INTERRUPCAO RECEBIDO.")
        finally:
            self.parar()

    def parar(self):
        self.running = False
        self.sock.close()
        print("[*] SERVIDOR DESLIGADO COM SUCESSO.")

    def _processar_pacote(self, pacote, addr):
        tipo, seq, checksum, tamanho, payload = desempacotar_pacote(pacote)

        if tipo == SYN:
            print(f"[HANDSHAKE] SYN recebido de {addr}. Enviando SYN_ACK...")
            self.clientes_autenticados.add(addr)
            self.sock.sendto(criar_pacote(SYN_ACK, 0, b"READY"), addr)
            return

        if addr not in self.clientes_autenticados:
            print(f"[BLOQUEIO] Tentativa de acesso sem handshake: {addr}")
            self.sock.sendto(criar_pacote(ERRO, 0, b"Faca handshake primeiro"), addr)
            return

        if tipo == REQ:
            nome = payload.decode()
            print(f"\n[REQ] Cliente {addr} solicitou: {nome}")
            threading.Thread(target=self._enviar_arquivo, args=(addr, nome), daemon=True).start()
        
        elif tipo == ACK:
            #print(f"[ACK RECV] Cliente {addr} enviou ACK {seq}")
            if addr in self.transferencias_ativas:
                event, ack_ref = self.transferencias_ativas[addr]
                ack_ref[0] = seq
                event.set()

    def _enviar_arquivo(self, addr, nome_arquivo):
        path = os.path.join(self.base_dir, nome_arquivo)
        if not os.path.exists(path):
            self.sock.sendto(criar_pacote(ERRO, 0, b"Arquivo nao encontrado"), addr)
            return

        janela_local = deque()
        proxima_seq_leitura = 0
        ultimo_ack_recebido = [-1] 
        ack_event_local = threading.Event()
        self.transferencias_ativas[addr] = (ack_event_local, ultimo_ack_recebido)

        print(f"\n[GBN] Iniciando envio para {addr} | Arquivo: {nome_arquivo}")

        try:
            with open(path, "rb") as f:
                while len(janela_local) < 5:
                    dados = f.read(TAMANHO_PAYLOAD)
                    if not dados: break
                    janela_local.append(criar_pacote(DADO, proxima_seq_leitura, dados))
                    proxima_seq_leitura += 1

                ack_event_local.set() 

                while janela_local and self.running:
                    chegou_ack = ack_event_local.wait(timeout=0.3)
                    ack_event_local.clear()

                    deslizou = False
                    if chegou_ack:
                        while janela_local:
                            _, s_base, _, _, _ = desempacotar_pacote(janela_local[0])
                            if s_base < ultimo_ack_recebido[0]:
                                janela_local.popleft()
                                deslizou = True
                                novos_dados = f.read(TAMANHO_PAYLOAD)
                                if novos_dados:
                                    novo_p = criar_pacote(DADO, proxima_seq_leitura, novos_dados)
                                    janela_local.append(novo_p)
                                    # Envia APENAS o novo
                                    if random.random() < CHANCE_CORRUPCAO:
                                        print(f"[!] Corrompendo propositalmente o pacote {s}")
                                        envio = self._corromper(novo_p)
                                    else:
                                        envio = novo_p
                                    self.sock.sendto(envio, addr)
                                    print(f"[SEND] Novo Segmento {proxima_seq_leitura} enviado")
                                    proxima_seq_leitura += 1
                            else:
                                break

                    if not deslizou and janela_local:
                        base_seq = desempacotar_pacote(janela_local[0])[1]
                        print(f"[PERDA] Reenviando janela a partir de {base_seq}...")
                        
                        for p in janela_local:
                            _, s, _, _, _ = desempacotar_pacote(p)
                            if random.random() < CHANCE_CORRUPCAO:
                                print(f"[!] Corrompendo propositalmente o pacote {s}")
                                envio = self._corromper(p)
                            else:
                                envio = p
                            self.sock.sendto(envio, addr)
                            print(f"[RESEND] Segmento {s} enviado")

                if self.running:
                    self.sock.sendto(criar_pacote(FIM, 0, b""), addr)
                    print(f"[FIM] Transferencia concluida para {addr}")

        finally:
            if addr in self.transferencias_ativas:
                del self.transferencias_ativas[addr]

    def _corromper(self, p):
        _, s, _, _, _ = desempacotar_pacote(p)
        lista_bytes = list(p)
        if len(lista_bytes) > TAMANHO_HEADER:
            # Altera o primeiro byte após o header
            lista_bytes[TAMANHO_HEADER] = (lista_bytes[TAMANHO_HEADER] + 1) % 256
        return bytes(lista_bytes)

if __name__ == "__main__":
    ServidorUDP().iniciar()