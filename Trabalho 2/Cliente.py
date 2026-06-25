import socket
import threading
import sys
import hashlib
import os
import queue
from datetime import datetime

SERVER_IP = "127.0.0.1"
PORT = 5000
BUFFER_SIZE = 8192

chat_history = []
history_lock = threading.Lock()
file_queue = queue.Queue()

in_chat_screen = threading.Event()
in_chat_screen.clear()

def clear_and_show_chat(my_port):
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== CHAT  ===")
    print("Digite sua mensagem e pressione Enter.")
    print("Para voltar ao menu principal, digite: \\sair")
    print("==================================\n")
    
    with history_lock:
        if not chat_history:
            print("...")
        else:
            for msg in chat_history:
                print(msg)
    print("\n----------------------------------")
    print("> ", end="", flush=True)

def receive_messages(client_socket, get_my_port_func):
    try:
        while True:
            header = client_socket.recv(4)
            if not header:
                print("\n[-] Conexao com o servidor perdida.")
                os._exit(0)
            
            payload_len = int.from_bytes(header, byteorder='big')
            
            payload = b""
            while len(payload) < payload_len:
                packet = client_socket.recv(payload_len - len(payload))
                if not packet:
                    break
                payload += packet
            
            try:
                text_payload = payload.decode('utf-8')
                if text_payload.startswith("CHAT:"):
                    with history_lock:
                        chat_history.append(text_payload[5:])
                    if in_chat_screen.is_set():
                        clear_and_show_chat(get_my_port_func())
                    continue
                elif text_payload.startswith("OK:") or text_payload.startswith("ERR:"):
                    file_queue.put(payload)
                    continue
            except UnicodeDecodeError:
                pass
            
            file_queue.put(payload)
    except:
        pass

def send_request(client_socket, protocol_msg):
    msg_bytes = protocol_msg.encode('utf-8')
    header = len(msg_bytes).to_bytes(4, byteorder='big')
    client_socket.sendall(header + msg_bytes)

def download_file_data(file_size):
    sha256 = hashlib.sha256()
    bytes_received = 0
    chunks_to_write = []
    
    while bytes_received < file_size:
        chunk = file_queue.get()
        chunks_to_write.append(chunk)
        sha256.update(chunk)
        bytes_received += len(chunk)
        
    return chunks_to_write, sha256.hexdigest()

def main():
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client.connect((SERVER_IP, PORT))
        except Exception as e:
            print(f"Erro ao conectar no servidor: {e}")
            input("Pressione Enter para sair...")
            return

        my_port = client.getsockname()[1]

        t = threading.Thread(target=receive_messages, args=(client, lambda: my_port), daemon=True)
        t.start()

        while True:
            print("\n--- MENU PRINCIPAL ---")
            print("1. Entrar no Chat")
            print("2. Baixar Arquivo")
            print("3. Sair")
            opcao = input("> ")

            if opcao == "1":
                in_chat_screen.set()
                clear_and_show_chat(my_port)
                
                while True:
                    msg = input()
                    
                    if msg.strip() == "\\sair":
                        break
                        
                    if msg.strip():
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        local_msg = f"[{timestamp}] [Porta {my_port} (Você)]: {msg}"
                        with history_lock:
                            chat_history.append(local_msg)
                        send_request(client, f"CHAT:{msg}")
                        
                    clear_and_show_chat(my_port)
                
                in_chat_screen.clear()
            
            elif opcao == "2":
                filename = input("Nome do arquivo no servidor: ")
                
                while not file_queue.empty():
                    try:
                        file_queue.get_nowait()
                    except queue.Empty:
                        break
                        
                send_request(client, f"FILE:{filename}")
                
                status_packet = file_queue.get()
                response = status_packet.decode('utf-8')
                
                if response.startswith("ERR:"):
                    print(f"[Erro] {response[4:]}")
                    continue
                    
                if response.startswith("OK:"):
                    file_size = int(response[3:])
                    print(f"Baixando arquivo ({file_size} bytes)...")
                    
                    chunks, client_hash = download_file_data(file_size)
                    
                    output_name = "download_" + os.path.basename(filename)
                    with open(output_name, "wb") as f:
                        for chunk in chunks:
                            f.write(chunk)
                    
                    hash_packet = file_queue.get()
                    server_hash = hash_packet.decode('utf-8')
                    
                    print(f"SHA-256 Servidor: {server_hash}")
                    print(f"SHA-256 Cliente:  {client_hash}")
                    
                    if server_hash == client_hash:
                        print("[Sucesso] Integridade verificada com sucesso!")
                    else:
                        print("[Alerta] Falha na integridade!")
                        
            elif opcao == "3":
                break
            else:
                print("Opcao invalida.")

        client.close()
    except Exception as e:
        print(f"[ERRO CRÍTICO NO CLIENTE]: {e}")
        input("Pressione Enter para sair...")

if __name__ == "__main__":
    main()