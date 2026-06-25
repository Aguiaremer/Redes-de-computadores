import socket
import threading
import os

PORT = 8080
BUFFER_SIZE = 8192
BASE_DIR = os.path.abspath("./arquivos")

if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".jpg": "image/jpeg",
}

def get_content_type(filepath):
    _, ext = os.path.splitext(filepath.lower())
    return MIME_TYPES.get(ext, "application/octet-stream")

def safe_path(url_path):
    decoded_path = url_path.split('?')[0]
    if decoded_path == "/":
        decoded_path = "/index.html"
    
    normalized_path = os.path.normpath(decoded_path.lstrip("/"))
    target_path = os.path.abspath(os.path.join(BASE_DIR, normalized_path))
    
    if target_path.startswith(BASE_DIR):
        return target_path
    return None

def build_http_response(status_code, status_text, content_type, body_bytes):
    headers = [
        f"HTTP/1.1 {status_code} {status_text}",
        "Server: MeuServidorSocketHTTP/1.1",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body_bytes)}",
        "Connection: close",
        "",
        ""
    ]
    return "\r\n".join(headers).encode('utf-8') + body_bytes

def handle_client(client_socket, client_address):
    print(f"[+] Conexão aceita: {client_address}")

    try:
        request_data = b""
        while b"\r\n\r\n" not in request_data:
            chunk = client_socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            request_data += chunk
            if len(request_data) > BUFFER_SIZE * 2:
                break
                
        if not request_data:
            return
            
        request_str = request_data.decode('utf-8', errors='ignore')
        lines = request_str.split("\r\n")
        if not lines or len(lines[0].split()) < 2:
            return
            
        method, url_path, _ = lines[0].split()
        
        filepath = safe_path(url_path)
        
        if filepath is None:
            body = "<html><body><h1>acesso negado</h1><p>Acesso negado fora da pasta raiz.</p></body></html>".encode('utf-8')
            response = build_http_response(403, "Forbidden", "text/html", body)
            client_socket.sendall(response)
            return

        if not os.path.exists(filepath) or os.path.isdir(filepath):
            body = "<html><body><h1>Arquivo nao encontrado</h1><p>O arquivo solicitado nao existe neste servidor.</p></body></html>".encode('utf-8')
            response = build_http_response(404, "Not Found", "text/html", body)
            client_socket.sendall(response)
            return

        content_type = get_content_type(filepath)
        file_size = os.path.getsize(filepath)
        
        headers = [
            "HTTP/1.1 200 OK",
            "Server: MeuServidorSocketHTTP/1.1",
            f"Content-Type: {content_type}",
            f"Content-Length: {file_size}",
            "Connection: close",
            "",
            ""
        ]
        header_bytes = "\r\n".join(headers).encode('utf-8')
        client_socket.sendall(header_bytes)
        
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(BUFFER_SIZE)
                if not chunk:
                    break
                client_socket.sendall(chunk)
                
    except Exception as e:
        pass
    finally:
        client_socket.close()

def main():
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", PORT))
        server.listen()
        print(f"[*] Servidor HTTP rodando na porta {PORT}...")
        
        while True:
            client_socket, client_address = server.accept()
            t = threading.Thread(target=handle_client, args=(client_socket, client_address))
            t.daemon = True
            t.start()
    except Exception as e:
        print(f"[ERRO CRÍTICO NO SERVIDOR]: {e}")
        input("Pressione Enter para sair...")

if __name__ == "__main__":
    main()