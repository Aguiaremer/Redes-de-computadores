import socket
import struct
import zlib


def criar_pacote(tipo, seq, dados):
    checksum = zlib.crc32(dados)
    tamanho = len(dados)
    header = struct.pack('!BIIH', tipo, seq, checksum, tamanho)
    return header + dados

# 1. Configurações do destino (onde o servidor está rodando)
IP_SERVIDOR = '192.168.1.100' 
PORTA_SERVIDOR = 5000
DESTINO = (IP_SERVIDOR, PORTA_SERVIDOR)

# 2. Criando o Socket UDP
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

try:
    # 3. Enviando uma mensagem inicial
    mensagem = criar_pacote(1,1,"Ola, Servidor!".encode())
    print(f"Enviando requisição: {mensagem}")
    sock.sendto(mensagem, DESTINO)

    # 4. Esperando a resposta do servidor
    # O cliente trava aqui até o servidor responder ou o tempo acabar
    data, addr = sock.recvfrom(1024)
    print(f"Resposta do Servidor {addr}: {data.decode()}")

finally:
    # 5. Fechar o socket ao terminar
    print("Fechando conexão.")
    sock.close()