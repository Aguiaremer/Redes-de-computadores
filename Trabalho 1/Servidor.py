import socket
import os
import struct


# 1. Definindo as configurações
IP_SERVIDOR = '192.168.1.100'
PORTA_SERVIDOR = 5000

# 2. Criando o Socket UDP
# AF_INET = Protocolo IP / SOCK_DGRAM = Protocolo UDP
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# 3. Vinculando o socket à porta (o servidor "escuta" aqui)
sock.bind((IP_SERVIDOR, PORTA_SERVIDOR))

print(f"Servidor pronto e ouvindo na porta {PORTA_SERVIDOR}...")

while True:
    # 4. Esperando uma mensagem (recebe até 1024 bytes)
    # data: o conteúdo da mensagem / addr: (IP, Porta) de quem enviou
    pacote, addr = sock.recvfrom(1024)

    header = pacote[:11]
    payload = pacote[11:]

    tipo, seq, checksum, tamanho = struct.unpack('!BIIH', header)
    mensagem = payload.decode()
    print(f"Tipo: {tipo}, Seq: {seq}, Mensagem: {mensagem}")
    
    # 5. Responder algo simples (opcional)
    sock.sendto("Mensagem recebida!".encode(), addr)