import socket
import threading
import time
import os
import struct
import zlib
import random

# --- CONFIGURAÇÕES ---
TAMANHO_PAYLOAD = 1024
CHANCE_PERDA = 0.001
FORMATO_HEADER = '!BIIH'
TAMANHO_HEADER = struct.calcsize(FORMATO_HEADER)

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

class ClienteUDP:
    def __init__(self, ip, porta):
        self.addr_servidor = (ip, porta)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2.0)
        self.base_dir = "Cliente-data"
        if not os.path.exists(self.base_dir): os.makedirs(self.base_dir)
        self.autenticado = False
        self.proxima_seq_esperada = 0
        self.transferencia_em_curso = False

    def fazer_handshake(self):
        print(f"[*] Iniciando Handshake com {self.addr_servidor}...")
        for i in range(3):
            try:
                self.sock.sendto(criar_pacote(SYN, 0), self.addr_servidor)
                dados, _ = self.sock.recvfrom(TAMANHO_PAYLOAD + TAMANHO_HEADER)
                tipo, _, _, _, _ = desempacotar_pacote(dados)
                if tipo == SYN_ACK:
                    print("[+] Handshake OK! Conectado ao servidor.")
                    self.autenticado = True
                    return True
            except socket.timeout:
                print(f"[!] Sem resposta do servidor (Tentativa {i+1}/3)")
        return False

    def solicitar_arquivo(self, nome):
        if not self.autenticado:
            if not self.fazer_handshake(): return

        print(f"[>] Pedindo arquivo: {nome}")
        self.proxima_seq_esperada = 0
        self.transferencia_em_curso = True
        self.sock.sendto(criar_pacote(REQ, 0, nome.encode()), self.addr_servidor)
        
        self._receber_dados(nome)

    def _receber_dados(self, nome):
        caminho = os.path.join(self.base_dir, nome)
        tempo_inicio = time.time()
        ultimo_contato = time.time()

        try:
            with open(caminho, "wb") as f:
                while self.transferencia_em_curso:
                    try:
                        pacote, _ = self.sock.recvfrom(TAMANHO_PAYLOAD + TAMANHO_HEADER)
                        tipo, seq, chk, _, payload = desempacotar_pacote(pacote)
                        ultimo_contato = time.time()

                        if tipo == DADO:
                            if random.random() < CHANCE_PERDA:
                                print(f"[PERDA] Simulada no pacote {seq}")
                                continue
                            
                            if zlib.crc32(payload) != chk:
                                print(f"[CORROMPIDO] Checksum inválido no pacote {seq}. Pedindo reenvio.")
                                self.sock.sendto(criar_pacote(ACK, self.proxima_seq_esperada), self.addr_servidor)
                                continue
                            elif seq != self.proxima_seq_esperada:
                                print(f"[FORA DE ORDEM] Recebi {seq}, mas esperava {self.proxima_seq_esperada}. Reenviando ACK do último OK.")
                                # Importante: enviar ACK da proxima_seq_esperada para o servidor saber onde você travou
                                self.sock.sendto(criar_pacote(ACK, self.proxima_seq_esperada), self.addr_servidor)
                                continue
                            else:
                                f.write(payload)
                                self.proxima_seq_esperada += 1
                                print(f"[RECV] Pacote {seq} OK. Enviando ACK {self.proxima_seq_esperada}")
                                self.sock.sendto(criar_pacote(ACK, self.proxima_seq_esperada), self.addr_servidor)
                                                      
                                # Responde ACK do que está esperando agora
                                self.sock.sendto(criar_pacote(ACK, self.proxima_seq_esperada), self.addr_servidor)

                        elif tipo == FIM:
                            print(f"\n[SUCESSO] Download concluido em {time.time()-tempo_inicio:.2f}s")
                            self.transferencia_em_curso = False

                        elif tipo == ERRO:
                            print(f"\n[ERRO SERVIDOR] {payload.decode()}")
                            self.transferencia_em_curso = False

                    except socket.timeout:
                        if time.time() - ultimo_contato > 5.0:
                            print("\n[ERRO FATAL] O servidor parou de responder.")
                            self.transferencia_em_curso = False
            
            if os.path.exists(caminho) and self.proxima_seq_esperada > 0:
                print(f"[*] Abrindo arquivo: {caminho}")
                os.startfile(caminho) # Comando para Windows

        except Exception as e:
            print(f"[ERRO] Falha ao processar arquivo: {e}")

if __name__ == "__main__":
    ip = input("IP do Servidor [192.168.1.100]: ") or "192.168.1.100"
    porta = input("Porta [5000]: ") or "5000"
    
    cliente = ClienteUDP(ip, int(porta))
    
    if cliente.fazer_handshake():
        while True:
            arq = input("\nDigite o nome do arquivo (ou 'sair'): ").strip()
            if arq.lower() == 'sair': break
            if arq: cliente.solicitar_arquivo(arq)