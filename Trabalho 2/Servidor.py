import socket
import threading
import os
import hashlib
from datetime import datetime

PORT = 5000
BUFFER_SIZE = 8192
BASE_DIR = os.path.abspath("./arquivos")

try:
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
except Exception as e:
    print(f"[ERRO CRÍTICO] Falha ao criar diretório: {e}")

clients = []
clients_lock = threading.Lock()

def broadcast(message, sender_socket=None):
    msg_bytes = message.encode('utf-8')
    header = len(msg_bytes).to_bytes(4, byteorder='big')
    packet = header + msg_bytes
    
    with clients_lock:
        for client in clients:
            if client != sender_socket:
                try:
                    client.sendall(packet)
                except:
                    pass

def safe_path(filename):
    target_path = os.path.abspath(os.path.join(BASE_DIR, filename))
    if target_path.startswith(BASE_DIR):
        return target_path
    return None

def handle_client(client_socket, client_address):
    print(f"[+] Conexão aceita: {client_address}")
    with clients_lock:
        clients.append(client_socket)
    
    try:
        while True:
            header = client_socket.recv(4)
            if not header:
                break
            
            payload_len = int.from_bytes(header, byteorder='big')
            
            payload = b""
            while len(payload) < payload_len:
                packet = client_socket.recv(payload_len - len(payload))
                if not packet:
                    break
                payload += packet
                
            payload_str = payload.decode('utf-8')
            
            if payload_str.startswith("CHAT:"):
                msg_content = payload_str[5:]
                timestamp = datetime.now().strftime("%H:%M:%S")
                formatted_msg = f"CHAT:[{timestamp}] [Porta {client_address[1]}]: {msg_content}"
                print(f"Broadcast: {formatted_msg}")
                broadcast(formatted_msg, client_socket)
                
            elif payload_str.startswith("FILE:"):
                filename = payload_str[5:]
                filepath = safe_path(filename)
                
                if filepath is None:
                    error_msg = "ERR:Acesso negado fora do diretorio permitido."
                    error_bytes = error_msg.encode('utf-8')
                    client_socket.sendall(len(error_bytes).to_bytes(4, byteorder='big') + error_bytes)
                    continue
                
                if not os.path.exists(filepath) or os.path.isdir(filepath):
                    error_msg = "ERR:Arquivo nao encontrado."
                    error_bytes = error_msg.encode('utf-8')
                    client_socket.sendall(len(error_bytes).to_bytes(4, byteorder='big') + error_bytes)
                    continue
                
                file_size = os.path.getsize(filepath)
                sha256 = hashlib.sha256()
                
                success_msg = f"OK:{file_size}"
                success_bytes = success_msg.encode('utf-8')
                client_socket.sendall(len(success_bytes).to_bytes(4, byteorder='big') + success_bytes)
                
                with open(filepath, "rb") as f:
                    while True:
                        chunk = f.read(BUFFER_SIZE)
                        if not chunk:
                            break
                        sha256.update(chunk)
                        
                        chunk_header = len(chunk).to_bytes(4, byteorder='big')
                        client_socket.sendall(chunk_header + chunk)
                
                hash_result = sha256.hexdigest()
                hash_bytes = hash_result.encode('utf-8')
                client_socket.sendall(len(hash_bytes).to_bytes(4, byteorder='big') + hash_bytes)
                
    except ConnectionResetError:
        pass
    finally:
        print(f"[-] Conexão encerrada: {client_address}")
        with clients_lock:
            if client_socket in clients:
                clients.remove(client_socket)
        client_socket.close()

def main():
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", PORT))
        server.listen()
        print(f"[*] Servidor rodando na porta {PORT}...")
        
        while True:
            client_socket, client_address = server.accept()
            t = threading.Thread(target=handle_client, args=(client_socket, client_address))
            t.daemon = True
            t.start()
    except Exception as e:
        print(f"[ERRO CRÍTICO NO CORPO DO SERVIDOR]: {e}")
        input("Pressione Enter para sair...")

if __name__ == "__main__":
    main()