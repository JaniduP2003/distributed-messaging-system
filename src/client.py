import socket
import threading
import time
import json
import logging
import sys
import signal
import argparse
import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("MessageClient")

class MessageClient:
    """
    Client for the distributed messaging system
    Connects to the cluster and handles message sending/receiving
    """
    def __init__(self, cluster_servers=None):
        """
        Initialize the messaging client
        
        Args:
            cluster_servers (list): List of server addresses in 'host:port' format
        """
        if cluster_servers is None:
            self.cluster_servers = ['127.0.0.1:5000', '127.0.0.1:5001', '127.0.0.1:5002']
        else:
            self.cluster_servers = cluster_servers
        
        self.client_socket = None
        self.connected = False
        self.running = True
        self.username = None
        self.current_server = None
        self.message_queue = []
        self.message_lock = threading.Lock()
        
        # Handle termination signals
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {sig}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def connect(self):
        """
        Connect to any available server in the cluster
        
        Returns:
            bool: True if connected successfully, False otherwise
        """
        while self.running and not self.connected:
            for server in self.cluster_servers:
                try:
                    host, port = server.split(':')
                    
                    # Create new socket for connection attempt
                    self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.client_socket.settimeout(5)  # Set timeout for connection attempts
                    self.client_socket.connect((host, int(port)))
                    
                    self.connected = True
                    self.current_server = server
                    logger.info(f"Connected to server {server}")
                    return True
                except socket.timeout:
                    logger.warning(f"Connection to {server} timed out")
                except Exception as e:
                    logger.warning(f"Unable to connect to server {server}: {e}")
                    
                    # Close socket if connection failed
                    if self.client_socket:
                        try:
                            self.client_socket.close()
                        except:
                            pass
                        self.client_socket = None
            
            # If still not connected, wait before retrying
            if not self.connected:
                logger.info("Trying again in 3 seconds...")
                time.sleep(3)
        
        return False
    
    def send_message(self, message_type, content):
        """
        Send a formatted message to the server
        
        Args:
            message_type (str): Type of message (e.g., 'chat', 'login', 'logout')
            content (str): Message content
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        if not self.connected:
            logger.warning("Not connected to any server")
            with self.message_lock:
                self.message_queue.append((message_type, content))
            return False
        
        try:
            message = {
                "type": message_type,
                "sender": self.username,
                "content": content,
                "timestamp": time.time()
            }
            
            data = json.dumps(message).encode('utf-8')
            self.client_socket.send(data)
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.connected = False
            
            # Queue the message for resending after reconnection
            with self.message_lock:
                self.message_queue.append((message_type, content))
            
            return False
    
    def receive_messages(self):
        """Handle receiving messages from the server"""
        buffer = bytearray()
        
        while self.running:
            if not self.connected:
                time.sleep(1)
                continue
            
            try:
                # Set socket to non-blocking read with timeout
                self.client_socket.settimeout(0.5)
                
                # Receive data
                chunk = self.client_socket.recv(4096)
                if not chunk:
                    logger.info("Lost connection to server")
                    self.connected = False
                    self.reconnect()
                    continue
                
                # Add to buffer
                buffer.extend(chunk)
                
                # Process complete messages
                try:
                    # Try to parse JSON message
                    message_data = buffer.decode('utf-8')
                    message = json.loads(message_data)
                    
                    # Successfully parsed a message
                    buffer.clear()
                    
                    # Process message based on type
                    if message.get("type") == "chat":
                        sender = message.get('sender', 'Unknown')
                        content = message.get('content', '')
                        timestamp = message.get('timestamp', time.time())
                        
                        # Format timestamp
                        time_str = datetime.datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
                        
                        print(f"\n[{time_str}] {sender}: {content}")
                    
                    elif message.get("type") == "system":
                        content = message.get('content', '')
                        timestamp = message.get('timestamp', time.time())
                        
                        # Format timestamp
                        time_str = datetime.datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
                        
                        print(f"\n[{time_str}] SYSTEM: {content}")
                except json.JSONDecodeError:
                    # Incomplete message, keep in buffer
                    pass
                except UnicodeDecodeError:
                    # Invalid UTF-8 data, clear buffer
                    logger.warning("Received invalid UTF-8 data")
                    buffer.clear()
            
            except socket.timeout:
                # This is expected - just continue the loop
                pass
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                self.connected = False
                self.reconnect()
    
    def reconnect(self):
        """
        Attempt to reconnect to any available server
        
        Returns:
            bool: True if reconnected successfully, False otherwise
        """
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass
            self.client_socket = None
        
        logger.info("Attempting to reconnect...")
        if self.connect():
            # Reconnected successfully, resend login
            if self.username:
                self.send_message("login", f"{self.username} has joined the chat")
                
                # Resend any queued messages
                with self.message_lock:
                    for msg_type, content in self.message_queue:
                        self.send_message(msg_type, content)
                    self.message_queue.clear()
                
                return True
        
        return False
    
    def start(self):
        """
        Start the client
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        # Connect to a server
        if not self.connect():
            logger.error("Failed to connect to any server")
            return False
        
        # Get username
        try:
            self.username = input("Enter your username: ")
            while not self.username.strip():
                print("Username cannot be empty")
                self.username = input("Enter your username: ")
        except KeyboardInterrupt:
            logger.info("Client startup cancelled")
            return False
        
        # Send login message
        if not self.send_message("login", f"{self.username} has joined the chat"):
            logger.error("Failed to send login message")
            return False
        
        # Start receiving messages in a separate thread
        receive_thread = threading.Thread(target=self.receive_messages, daemon=True)
        receive_thread.start()
        
        # Display welcome message
        print(f"\nConnected to messaging server at {self.current_server}")
        print("Type your messages and press Enter to send")
        print("Type '/quit' to exit the chat")
        
        # Handle user input
        try:
            while self.running:
                message = input("")
                if message.lower() == "/quit":
                    self.running = False
                    break
                elif message.strip():
                    if not self.send_message("chat", message):
                        logger.warning("Message failed to send. Attempting to reconnect...")
                        self.reconnect()
        except KeyboardInterrupt:
            logger.info("\nDisconnecting...")
        finally:
            self.stop()
        
        return True
    
    def stop(self):
        """Stop the client and clean up resources"""
        self.running = False
        
        if self.connected and self.username:
            try:
                logger.info("Sending logout message...")
                self.send_message("logout", f"{self.username} has left the chat")
            except:
                pass
        
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass
            self.client_socket = None
        
        logger.info("Client stopped")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Distributed Messaging System Client")
    parser.add_argument("--servers", type=str, default="127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002",
                        help="Comma-separated list of server addresses (host:port)")
    args = parser.parse_args()
    
    # Split server addresses
    server_list = args.servers.split(',')
    
    # Create and start client
    client = MessageClient(server_list)
    client.start()
