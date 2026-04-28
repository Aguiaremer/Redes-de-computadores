import struct
import zlib


FORMATO_HEADER = '!BIIH'
TAMANHO_HEADER = struct.calcsize(FORMATO_HEADER)

def criar_pacote(tipo, seq, dados=b""):
    checksum = zlib.crc32(dados)
    tamanho = len(dados)
    header = struct.pack(FORMATO_HEADER, tipo, seq, checksum, tamanho)
    return header + dados

def desempacotar_pacote(pacote):
    header_bruto = pacote[:TAMANHO_HEADER]
    payload = pacote[TAMANHO_HEADER:]
    
    tipo, seq, checksum, tamanho = struct.unpack(FORMATO_HEADER, header_bruto)
    return tipo, seq, checksum, tamanho, payload