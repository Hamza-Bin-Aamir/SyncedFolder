import socket
import uuid
import sys
import os
import threading
import json
import hashlib
import time
from pathlib import Path
from zeroconf import Zeroconf, ServiceInfo, ServiceBrowser


SERVICE_TYPE = "_transceiver._tcp.local."


def get_local_ip():
    """Utility to get the local IP address (IPv4) on the network interface."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # connect to an external address; doesn't need to be reachable
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def get_file_hash(filepath):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return None


def get_folder_manifest(folder_path):
    """Get a manifest of all files in the folder with their hashes."""
    manifest = {}
    folder_path = Path(folder_path)
    
    if not folder_path.exists():
        return manifest
    
    for file_path in folder_path.rglob('*'):
        if file_path.is_file():
            relative_path = file_path.relative_to(folder_path)
            file_hash = get_file_hash(file_path)
            if file_hash:
                manifest[str(relative_path)] = {
                    'hash': file_hash,
                    'size': file_path.stat().st_size,
                    'modified': file_path.stat().st_mtime
                }
    return manifest


class TransceiverService:
    def __init__(self, port, sync_folder):
        self.zeroconf = Zeroconf()
        self.port = port
        self.sync_folder = Path(sync_folder).resolve()
        self.service_name = f"Transceiver-{uuid.uuid4()}." + SERVICE_TYPE
        self.ip = get_local_ip()
        self.info = ServiceInfo(
            SERVICE_TYPE,
            self.service_name,
            addresses=[socket.inet_aton(self.ip)],
            port=self.port,
            properties={"desc": "Python Transceiver", "folder": str(self.sync_folder)},
            server=f"{socket.gethostname()}.local.",
        )
        self.peers = {}  # Store discovered peers
        self.server_socket = None
        self.running = True
        
        # Ensure sync folder exists
        self.sync_folder.mkdir(parents=True, exist_ok=True)
        print(f"Syncing folder: {self.sync_folder}")

    def register(self):
        self.zeroconf.register_service(self.info)
        print(f"Service {self.service_name} registered at {self.ip}:{self.port}")

    def unregister(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.zeroconf.unregister_service(self.info)
        self.zeroconf.close()

    def start_server(self):
        """Start TCP server to handle incoming connections."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        
        print(f"TCP server listening on {self.ip}:{self.port}")
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                threading.Thread(target=self.handle_client, args=(client_socket, address), daemon=True).start()
            except OSError:
                break

    def handle_client(self, client_socket, address):
        """Handle incoming client connections."""
        try:
            # Receive the initial message
            data = client_socket.recv(4096).decode('utf-8')
            message = json.loads(data)
            
            if message['type'] == 'manifest_request':
                # Send our folder manifest
                manifest = get_folder_manifest(self.sync_folder)
                response = {
                    'type': 'manifest_response',
                    'manifest': manifest
                }
                response_data = json.dumps(response).encode('utf-8')
                # Send length first, then data
                client_socket.send(len(response_data).to_bytes(4, 'big'))
                client_socket.send(response_data)
                
            elif message['type'] == 'file_request':
                # Send requested file using binary protocol
                file_path = self.sync_folder / message['file_path']
                if file_path.exists() and file_path.is_file():
                    # Send file info as JSON header
                    file_info = {
                        'type': 'file_response',
                        'file_path': message['file_path'],
                        'file_size': file_path.stat().st_size
                    }
                    header_data = json.dumps(file_info).encode('utf-8')
                    
                    # Send header length, then header, then file data
                    client_socket.send(len(header_data).to_bytes(4, 'big'))
                    client_socket.send(header_data)
                    
                    # Send file data in chunks
                    with open(file_path, 'rb') as f:
                        while True:
                            chunk = f.read(8192)
                            if not chunk:
                                break
                            client_socket.send(chunk)
                else:
                    # File not found
                    error_response = {'type': 'file_not_found', 'file_path': message['file_path']}
                    response_data = json.dumps(error_response).encode('utf-8')
                    client_socket.send(len(response_data).to_bytes(4, 'big'))
                    client_socket.send(response_data)
                    
            elif message['type'] == 'push_manifest':
                # Receive a manifest from a peer who is pushing
                peer_manifest = message['manifest']
                local_manifest = get_folder_manifest(self.sync_folder)
                
                # Find files to request from the peer
                files_to_request = []
                for file_path, file_info in peer_manifest.items():
                    local_file_info = local_manifest.get(file_path)
                    
                    # Request if file doesn't exist locally or has different hash
                    if not local_file_info or local_file_info['hash'] != file_info['hash']:
                        files_to_request.append(file_path)
                
                # Send back list of files we want
                response = {
                    'type': 'push_file_list',
                    'files_needed': files_to_request
                }
                response_data = json.dumps(response).encode('utf-8')
                client_socket.send(len(response_data).to_bytes(4, 'big'))
                client_socket.send(response_data)
                
            elif message['type'] == 'push_file':
                # Receive a file being pushed to us using binary protocol
                file_path = message['file_path']
                file_size = message['file_size']
                
                local_file_path = self.sync_folder / file_path
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Receive file data
                with open(local_file_path, 'wb') as f:
                    remaining = file_size
                    while remaining > 0:
                        chunk_size = min(8192, remaining)
                        chunk = client_socket.recv(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        remaining -= len(chunk)
                
                print(f"Received pushed file: {file_path}")
                
                # Send acknowledgment
                ack_response = {'type': 'push_file_ack'}
                response_data = json.dumps(ack_response).encode('utf-8')
                client_socket.send(len(response_data).to_bytes(4, 'big'))
                client_socket.send(response_data)
                    
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()

    def download_file_from_peer(self, peer_ip, peer_port, file_path):
        """Download a specific file from a peer using binary protocol."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_ip, peer_port))
            
            request = {
                'type': 'file_request',
                'file_path': file_path
            }
            sock.send(json.dumps(request).encode('utf-8'))
            
            # Receive header length
            header_length_data = sock.recv(4)
            if len(header_length_data) != 4:
                sock.close()
                return
            header_length = int.from_bytes(header_length_data, 'big')
            
            # Receive header
            header_data = sock.recv(header_length).decode('utf-8')
            header = json.loads(header_data)
            
            if header['type'] == 'file_response':
                file_size = header['file_size']
                local_file_path = self.sync_folder / file_path
                
                # Create directories if needed
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Receive file data in chunks
                with open(local_file_path, 'wb') as f:
                    remaining = file_size
                    while remaining > 0:
                        chunk_size = min(8192, remaining)
                        chunk = sock.recv(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        remaining -= len(chunk)
                
                print(f"Downloaded: {file_path}")
            elif header['type'] == 'file_not_found':
                print(f"File not found on peer: {file_path}")
            
            sock.close()
                
        except Exception as e:
            print(f"Error downloading file {file_path} from {peer_ip}:{peer_port}: {e}")

    def push_to_all_peers(self):
        """Push (send) current folder contents to all discovered peers."""
        if not self.peers:
            print("No peers discovered yet.")
            return
            
        local_manifest = get_folder_manifest(self.sync_folder)
        if not local_manifest:
            print("No files to push.")
            return
            
        print(f"Pushing folder contents to {len(self.peers)} peer(s)...")
        
        for peer_name, (peer_ip, peer_port) in self.peers.items():
            print(f"Pushing to {peer_name} at {peer_ip}:{peer_port}")
            self.push_to_peer(peer_ip, peer_port, local_manifest)

    def push_to_peer(self, peer_ip, peer_port, local_manifest):
        """Push our files to a specific peer."""
        try:
            # First, send our manifest to see what files the peer needs
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_ip, peer_port))
            
            request = {
                'type': 'push_manifest',
                'manifest': local_manifest
            }
            sock.send(json.dumps(request).encode('utf-8'))
            
            # Receive header length
            header_length_data = sock.recv(4)
            if len(header_length_data) != 4:
                sock.close()
                return
            header_length = int.from_bytes(header_length_data, 'big')
            
            # Receive response
            response_data = sock.recv(header_length).decode('utf-8')
            response = json.loads(response_data)
            sock.close()
            
            if response['type'] == 'push_file_list':
                files_needed = response['files_needed']
                
                if not files_needed:
                    print(f"  Peer {peer_ip}:{peer_port} is already up to date")
                    return
                
                print(f"  Sending {len(files_needed)} file(s) to {peer_ip}:{peer_port}")
                
                # Send each requested file
                for file_path in files_needed:
                    self.send_file_to_peer(peer_ip, peer_port, file_path)
                    
        except Exception as e:
            print(f"Error pushing to peer {peer_ip}:{peer_port}: {e}")

    def send_file_to_peer(self, peer_ip, peer_port, file_path):
        """Send a specific file to a peer using binary protocol."""
        try:
            local_file_path = self.sync_folder / file_path
            
            if not local_file_path.exists():
                print(f"  Warning: File {file_path} no longer exists locally")
                return
            
            file_size = local_file_path.stat().st_size
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_ip, peer_port))
            
            # Send file info header
            request = {
                'type': 'push_file',
                'file_path': file_path,
                'file_size': file_size
            }
            sock.send(json.dumps(request).encode('utf-8'))
            
            # Send file data in chunks
            with open(local_file_path, 'rb') as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    sock.send(chunk)
            
            # Wait for acknowledgment
            header_length_data = sock.recv(4)
            if len(header_length_data) == 4:
                header_length = int.from_bytes(header_length_data, 'big')
                response_data = sock.recv(header_length).decode('utf-8')
                response = json.loads(response_data)
                
                if response['type'] == 'push_file_ack':
                    print(f"  Sent: {file_path}")
            
            sock.close()
            
        except Exception as e:
            print(f"  Error sending file {file_path} to {peer_ip}:{peer_port}: {e}")

    def sync_with_peer(self, peer_ip, peer_port):
        """Pull files from a peer (used for auto-sync when peer is discovered)."""
        try:
            # Get peer's manifest
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_ip, peer_port))
            
            request = {'type': 'manifest_request'}
            sock.send(json.dumps(request).encode('utf-8'))
            
            # Receive header length
            header_length_data = sock.recv(4)
            if len(header_length_data) != 4:
                sock.close()
                return
            header_length = int.from_bytes(header_length_data, 'big')
            
            # Receive manifest response
            response_data = sock.recv(header_length).decode('utf-8')
            response = json.loads(response_data)
            sock.close()
            
            if response['type'] == 'manifest_response':
                peer_manifest = response['manifest']
                local_manifest = get_folder_manifest(self.sync_folder)
                
                # Find files to download
                files_to_download = []
                for file_path, file_info in peer_manifest.items():
                    local_file_info = local_manifest.get(file_path)
                    
                    # Download if file doesn't exist locally or has different hash
                    if not local_file_info or local_file_info['hash'] != file_info['hash']:
                        files_to_download.append(file_path)
                
                # Download files
                for file_path in files_to_download:
                    self.download_file_from_peer(peer_ip, peer_port, file_path)
                    
                if files_to_download:
                    print(f"Downloaded {len(files_to_download)} files from {peer_ip}:{peer_port}")
                else:
                    print(f"No new files to download from {peer_ip}:{peer_port}")
                    
        except Exception as e:
            print(f"Error syncing with peer {peer_ip}:{peer_port}: {e}")


class TransceiverListener:
    def __init__(self, own_service_name, transceiver_service):
        self.own_service_name = own_service_name
        self.transceiver_service = transceiver_service

    def add_service(self, zeroconf, type_, name):
        # Ignore own service announcement
        if name == self.own_service_name:
            return
        info = zeroconf.get_service_info(type_, name)
        if info:
            ip = socket.inet_ntoa(info.addresses[0])
            port = info.port
            print(f"Discovered Transceiver: {name} at {ip}:{port}")
            
            # Store peer information
            self.transceiver_service.peers[name] = (ip, port)
            
            # Automatically sync with new peer
            print(f"Auto-syncing with {name}...")
            self.transceiver_service.sync_with_peer(ip, port)

    def remove_service(self, zeroconf, type_, name):
        print(f"Transceiver removed: {name}")
        if name in self.transceiver_service.peers:
            del self.transceiver_service.peers[name]

    def update_service(self, zeroconf, type_, name):
        """Called when service information is updated."""
        # For now, we'll treat updates the same as adding a service
        # In case the IP or port changed
        if name == self.own_service_name:
            return
        info = zeroconf.get_service_info(type_, name)
        if info:
            ip = socket.inet_ntoa(info.addresses[0])
            port = info.port
            print(f"Transceiver updated: {name} at {ip}:{port}")
            
            # Update peer information
            self.transceiver_service.peers[name] = (ip, port)


def handle_user_input(service):
    """Handle user input for push commands."""
    while service.running:
        try:
            user_input = input().strip().lower()
            if user_input == "push":
                service.push_to_all_peers()
            elif user_input in ["exit", "quit"]:
                break
        except (EOFError, KeyboardInterrupt):
            break


def main():
    # Get sync folder from command line argument or use current directory
    if len(sys.argv) > 1:
        sync_folder = sys.argv[1]
    else:
        sync_folder = os.getcwd()
    
    # Get port from second command line argument or use default
    if len(sys.argv) > 2:
        try:
            transceiver_port = int(sys.argv[2])
            if transceiver_port < 1024 or transceiver_port > 65535:
                print("Error: Port must be between 1024 and 65535.")
                sys.exit(1)
        except ValueError:
            print("Error: Port must be a valid integer.")
            sys.exit(1)
    else:
        transceiver_port = 12345  # Default port
    
    sync_folder = os.path.abspath(sync_folder)
    
    if not os.path.exists(sync_folder):
        print(f"Error: Folder '{sync_folder}' does not exist.")
        sys.exit(1)
    
    if not os.path.isdir(sync_folder):
        print(f"Error: '{sync_folder}' is not a directory.")
        sys.exit(1)

    service = TransceiverService(port=transceiver_port, sync_folder=sync_folder)
    service.register()

    listener = TransceiverListener(service.service_name, service)
    browser = ServiceBrowser(service.zeroconf, SERVICE_TYPE, listener)

    # Start TCP server in a separate thread
    server_thread = threading.Thread(target=service.start_server, daemon=True)
    server_thread.start()

    # Start user input handler in a separate thread
    input_thread = threading.Thread(target=handle_user_input, args=(service,), daemon=True)
    input_thread.start()

    try:
        print("Folder synchronization active!")
        print(f"Syncing folder: {sync_folder}")
        print(f"Listening on port: {transceiver_port}")
        print("Commands:")
        print("  - Type 'push' to sync your folder to all peers")
        print("  - Press Ctrl+C to exit")
        print()
        
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        print("\nShutting down...")
        service.unregister()


if __name__ == "__main__":
    main()
