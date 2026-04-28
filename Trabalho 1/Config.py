# Configurações de Rede
IP_SERVIDOR = '192.168.1.100'  # Ou '127.0.0.1' para testes locais
PORTA_SERVIDOR = 5000

# Parâmetros de Protocolo
TAMANHO_PAYLOAD = 1024  # Tamanho do pedaço do arquivo (Segmentação)
TIMEOUT_REDE = 5.0      # Segundos para esperar por uma resposta

# Tipos de Pacote
REQ = 0
ACK = 1
ERRO = 2
DADO = 3
FIM = 4

