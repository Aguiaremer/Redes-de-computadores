import socket
import threading
import time
import os
import struct
import zlib
import random

# --- CONFIGURAÇÕES E UTILS INTEGRADOS ---
IP_SERVIDOR = '192.168.1.100'
PORTA_SERVIDOR = 5000
TAMANHO_PAYLOAD = 1024
CHANCE_PERDA = 0.01  # 1% de chance de perda de pacote
TIMEOUT_REDE = 5.0
FORMATO_HEADER = '!BIIH'
TAMANHO_HEADER = struct.calcsize(FORMATO_HEADER)

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

# --- CLASSE DO CLIENTE ---
class ClienteUDP:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', 0))
        self.servidor_addr = (IP_SERVIDOR, PORTA_SERVIDOR)
        self.base_dir = "Cliente-data"

        
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

        self.arquivo_sendo_recebido = None
        self.nome_arquivo_atual = ""
        self.running = True
        self.transferencia_em_curso = False
        
        self.ultimo_contato = 0
        self.TIMEOUT_INATIVIDADE = 5.0
        self.ultima_seq = -1
        self.proxima_seq_esperada = 0
        self.tempo_inicio = 0
        self.esperando_ack_inicial = False

    def iniciar(self):
        self.thread_listener = threading.Thread(target=self._escutar, daemon=True)
        self.thread_listener.start()
        print(f"[*] Cliente pronto. Downloads salvos em: {self.base_dir}")

    def solicitar_arquivo(self, nome_arquivo):
        """Prepara o estado e envia a requisição."""
        # --- RESET DE ESTADO PARA NOVO DOWNLOAD ---
        self.nome_arquivo_atual = nome_arquivo
        self.ultima_seq = -1
        self.proxima_seq_esperada = 0
        self.transferencia_em_curso = True # Nova flag para controlar o loop no terminal
        self.esperando_ack_inicial = True
        self.tempo_inicio = time.time()

        if self.arquivo_sendo_recebido:
            self.arquivo_sendo_recebido.close()
            self.arquivo_sendo_recebido = None

        pacote = criar_pacote(REQ, 0, nome_arquivo.encode())
        self.sock.sendto(pacote, self.servidor_addr)
        print(f"[>] Requisição de '{nome_arquivo}' enviada...")

        tentativas = 0
        max_tentativas = 5
        timeout_conexao = 2.0 # Segundos esperando o primeiro sinal de vida
        
        self.ultimo_contato = time.time()

        while self.esperando_ack_inicial:
            time.sleep(0.5) # Checa a cada meio segundo
            tentativas += 1
            
            if tentativas > (timeout_conexao / 0.5):
                print(f"\n[ERRO] Não foi possível contatar o servidor em {self.servidor_addr}")
                print("[!] Verifique se o servidor está rodando ou se o IP/Porta estão corretos.")
                self.transferencia_em_curso = False
                self.esperando_ack_inicial = False
                return # Aborta a solicitação

        # Aguarda a transferência terminar (o listener vai mudar essa flag no FIM)
        while self.transferencia_em_curso:
            time.sleep(0.1)

            # Checa se o servidor "sumiu"
            if time.time() - self.ultimo_contato > self.TIMEOUT_INATIVIDADE:
                print(f"\n[ERRO FATAL] Conexão perdida com o servidor.")
                print(f"[!] O servidor parou de responder há {self.TIMEOUT_INATIVIDADE}s.")
                
                # Limpa o arquivo incompleto para não deixar lixo
                if self.arquivo_sendo_recebido:
                    self.arquivo_sendo_recebido.close()
                    self.arquivo_sendo_recebido = None
                    path_parcial = os.path.join(self.base_dir, self.nome_arquivo_atual)
                    if os.path.exists(path_parcial):
                        os.remove(path_parcial)
                
                self.transferencia_em_curso = False
                self.esperando_ack_inicial = False
                return  

    def _escutar(self):
        while self.running:
            try:
                pacote, addr = self.sock.recvfrom(TAMANHO_PAYLOAD + TAMANHO_HEADER)
                self._processar_pacote(pacote, addr)
            except:
                break

    def _processar_pacote(self, pacote, addr):
        tipo, seq, checksum, tamanho, payload = desempacotar_pacote(pacote)
        
        self.ultimo_contato = time.time()

        if tipo == DADO:
            self._tratar_dados(payload, seq, checksum, addr)
        elif tipo == ACK:
            # Se era o ACK da requisição (o primeiro "OK")
            if self.esperando_ack_inicial:
                print(f"[*] Conexão estabelecida! Servidor pronto para enviar.")
                self.esperando_ack_inicial = False
        elif tipo == FIM:
            print(f"[FIM] Servidor acabou de enviar o arquivo.")
            self.tempo_total = time.time() - self.tempo_inicio
            self._finalizar_arquivo()
        elif tipo == ERRO:
            print(f"\n[ERRO] {payload.decode()}")
            self.running = False

    def _tratar_dados(self, payload, seq, checksum, addr):
        if random.random() < CHANCE_PERDA:
            print(f"[!] PERDA SIMULADA: {seq}")
            return

        # Se vier errado ou fora de ordem, o ACK diz o que eu REALMENTE quero
        if seq != self.proxima_seq_esperada or zlib.crc32(payload) != checksum:
            if seq != self.proxima_seq_esperada:
                print(f"[!] Fora de ordem. Recebi {seq}, mas quero o {self.proxima_seq_esperada}")
            else:
                print(f"[!] Checksum inválido no {seq}. Pedindo o {self.proxima_seq_esperada} novamente.")
            
            # Manda o que ele está esperando no momento
            self.sock.sendto(criar_pacote(ACK, self.proxima_seq_esperada, b""), addr)
            return

        # --- SUCESSO ---
        if self.arquivo_sendo_recebido is None:
            print(f"[RECV] Segmento {seq} OK. Enviando ACK {self.proxima_seq_esperada} (Esperando o próximo)")
            self.arquivo_sendo_recebido = open(os.path.join(self.base_dir, self.nome_arquivo_atual), "wb")
        
        self.arquivo_sendo_recebido.write(payload)
        
        # Incrementa o que eu quero ANTES de mandar o ACK
        self.proxima_seq_esperada += 1
        self.ultima_seq = seq
        
        print(f"[RECV] Segmento {seq} OK. Enviando ACK {self.proxima_seq_esperada} (Quero o próximo)...")
        self.sock.sendto(criar_pacote(ACK, self.proxima_seq_esperada, b""), addr)

    def _finalizar_arquivo(self):
        if self.arquivo_sendo_recebido:
            self.arquivo_sendo_recebido.close()
            self.arquivo_sendo_recebido = None
        print(f"\n[SUCESSO] Arquivo salvo em {self.base_dir}")
        print(f"Tempo Total: {self.tempo_total:.4f} segundos")

        path_completo = os.path.abspath(os.path.join(self.base_dir, self.nome_arquivo_atual))
        print(f"[*] Abrindo arquivo: {path_completo}")
        
        try:
            if os.name == 'nt': # Windows
                os.startfile(path_completo)
        except Exception as e:
            print(f"[!] Não foi possível abrir o arquivo: {e}")

        self.transferencia_em_curso = False 

if __name__ == "__main__":
    cliente = ClienteUDP()
    cliente.iniciar()
    try:
        while True:
            # O programa fica parado aqui esperando você digitar o nome
            arquivo_pedido = input("\nDigite o nome do arquivo (ou 'sair'): ").strip()
           
            if arquivo_pedido.lower() == 'sair':
                print("Encerrando cliente...")
                break

            if arquivo_pedido:
                cliente.solicitar_arquivo(arquivo_pedido)
    except KeyboardInterrupt:
        print("\nSaindo...")