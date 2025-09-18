import socket
import threading
import json
import time
import asyncio
import logging
import os
import sys
import signal
import argparse

import raftos

from message_storage.storage import MessageStorage
from time_sync.timesync import TimeSync

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("MessageServer")

class MessageServer:
    """
    Message server for handling client connections and message distribution
    Implements the messaging protocol and interfaces with distributed storage
    """
    def __init__(self, host, port):
        """
        Initialize the message server
        
        Args:
            host (str): Host address to bind to
            port (int): Port to listen on
        """
        self.host = host
        self.port = port
        
        # Initialize socket server
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Client handling
        self.clients = {}  # {client_socket: username}
        self.clients_lock = threading.Lock()
        
        # Create asyncio event loop for async operations
        self.loop = asyncio.new_event_loop()
        
        # Initialize components
        self.time_sync = TimeSync()
        self.message_storage = MessageStorage()
        
        # Running state
        self.running = False
        
        # Signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {sig}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """
        Start the message server
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            # Initialize time synchronization
            if not self.time_sync.sync_time():
                logger.warning("Failed to synchronize time, using local time")
            
            # Bind socket
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            
            logger.info(f"Server started on {self.host}:{self.port}")
            
            # Accept client connections in a separate thread
            threading.Thread(target=self.accept_connections, daemon=True).start()
            
            # Start the event loop for async operations
            threading.Thread(target=self._run_asyncio_loop, daemon=True).start()
            
            # Store initial welcome message
            self.loop.call_soon_threadsafe(lambda: asyncio.run_coroutine_threadsafe(
                self._store_welcome_message(), self.loop))
            
            return True
        except Exception as e:
            logger.error(f"Error starting server: {e}")
            return False
    
    async def _store_welcome_message(self):
        """Store a welcome message in distributed storage"""
        try:
            timestamp = self.time_sync.get_synced_time()
            
            message_data = {
                "type": "system",
                "sender": "server",
                "content": f"Server started at {self.host}:{self.port}",
                "timestamp": timestamp
            }
            
            message_id = f"{int(timestamp)}_server_start"
            await self.message_storage.store_message(message_id, json.dumps(message_data))
            logger.info("Welcome message stored in distributed storage")
        except Exception as e:
            logger.error(f"Error storing welcome message: {e}")
    
    def _run_asyncio_loop(self):
        """Run the asyncio event loop in a separate thread"""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
    
    def accept_connections(self):
        """Accept incoming client connections"""
        logger.info("Accepting client connections")
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                logger.info(f"New connection from {address}")
                
                # Start a thread to handle this client
                threading.Thread(
                    target=self.handle_client, 
                    args=(client_socket, address), 
                    daemon=True
                ).start()
            except Exception as e:
                if self.running:
                    logger.error(f"Error accepting connection: {e}")
                    time.sleep(1)  # Avoid tight loop if there's an error
    
    def handle_client(self, client_socket, address):
        """
        Handle communication with a connected client
        
        Args:
            client_socket (socket): Client socket object
            address (tuple): Client address (host, port)
        """
        username = None
        
        try:
            while self.running:
                # Receive data from client
                data = client_socket.recv(4096)
                if not data:
                    # Client disconnected
                    break
                
                # Process the message
                try:
                    message = json.loads(data.decode('utf-8'))
                    message_type = message.get("type")
                    
                    if message_type == "login":
                        username = message.get("sender")
                        # Register client
                        with self.clients_lock:
                            self.clients[client_socket] = username
                        
                        # Use NTP synchronized time for accurate timestamps
                        timestamp = self.time_sync.get_synced_time()
                        
                        # Send welcome message
                        self.send_system_message(client_socket, f"Welcome, {username}!")
                        
                        # Broadcast join message
                        self.broadcast_message({
                            "type": "system",
                            "content": f"{username} has joined the chat",
                            "timestamp": timestamp
                        }, exclude=client_socket)
                        
                        # Store in persistent storage
                        self._store_message_async("join", username, f"{username} joined", timestamp)
                    
                    elif message_type == "chat":
                        if username:  # Only process chat messages if logged in
                            content = message.get("content", "")
                            
                            # Use NTP synchronized time for accurate timestamps
                            timestamp = self.time_sync.get_synced_time()
                            
                            # Broadcast the message to all clients
                            self.broadcast_message({
                                "type": "chat",
                                "sender": username,
                                "content": content,
                                "timestamp": timestamp
                            })
                            
                            # Store in persistent storage
                            self._store_message_async("chat", username, content, timestamp)
                    
                    elif message_type == "logout":
                        # Handle client logout
                        break
                
                except json.JSONDecodeError:
                    logger.warning(f"Received invalid JSON from {address}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        finally:
            # Clean up when client disconnects
            if username:
                with self.clients_lock:
                    if client_socket in self.clients:
                        del self.clients[client_socket]
                
                # Use NTP synchronized time for accurate timestamps
                timestamp = self.time_sync.get_synced_time()
                
                # Broadcast leave message
                self.broadcast_message({
                    "type": "system",
                    "content": f"{username} has left the chat",
                    "timestamp": timestamp
                })
                
                # Store in persistent storage
                self._store_message_async("leave", username, f"{username} left", timestamp)
            
            try:
                client_socket.close()
            except:
                pass
            
            logger.info(f"Connection closed for {address}")
    
    def _store_message_async(self, msg_type, username, content, timestamp):
        """
        Store a message in the distributed storage asynchronously
        
        Args:
            msg_type (str): Message type (chat, join, leave)
            username (str): Sender username
            content (str): Message content
            timestamp (float): Message timestamp
        """
        async def store():
            try:
                message_id = f"{int(timestamp)}_{username}_{msg_type}"
                message_data = {
                    "type": msg_type,
                    "sender": username,
                    "content": content,
                    "timestamp": timestamp
                }
                
                await self.message_storage.store_message(message_id, json.dumps(message_data))
                logger.debug(f"Message stored: {message_id}")
            except Exception as e:
                logger.error(f"Error storing message: {e}")
        
        asyncio.run_coroutine_threadsafe(store(), self.loop)
    
    def send_system_message(self, client_socket, message):
        """
        Send a system message to a specific client
        
        Args:
            client_socket (socket): Client socket object
            message (str): Message content
        """
        try:
            timestamp = self.time_sync.get_synced_time()
            data = json.dumps({
                "type": "system",
                "content": message,
                "timestamp": timestamp
            }).encode('utf-8')
            client_socket.send(data)
        except Exception as e:
            logger.error(f"Error sending system message: {e}")
    
    def broadcast_message(self, message, exclude=None):
        """
        Broadcast a message to all connected clients
        
        Args:
            message (dict): Message to broadcast
            exclude (socket, optional): Client socket to exclude from broadcast
        """
        with self.clients_lock:
            clients = list(self.clients.keys())
        
        data = json.dumps(message).encode('utf-8')
        
        for client_socket in clients:
            if client_socket != exclude:
                try:
                    client_socket.send(data)
                except Exception as e:
                    logger.error(f"Error broadcasting to client: {e}")
                    # Client will be removed on next message receive attempt
    
    async def get_message_history(self, limit=50):
        """
        Get recent message history
        
        Args:
            limit (int): Maximum number of messages to retrieve
            
        Returns:
            list: List of messages
        """
        try:
            messages = await self.message_storage.get_all_messages()
            
            # Sort by timestamp
            messages.sort(key=lambda x: json.loads(x[1]).get('timestamp', 0))
            
            # Take the most recent messages
            recent_messages = messages[-limit:] if len(messages) > limit else messages
            
            return [json.loads(msg[1]) for msg in recent_messages]
        except Exception as e:
            logger.error(f"Error retrieving message history: {e}")
            return []
    
    def stop(self):
        """Stop the message server"""
        logger.info("Stopping message server")
        self.running = False
        
        # Close all client connections
        with self.clients_lock:
            for client_socket in list(self.clients.keys()):
                try:
                    client_socket.close()
                except:
                    pass
            self.clients.clear()
        
        # Close server socket
        try:
            self.server_socket.close()
        except:
            pass
        
        # Stop asyncio loop
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            
            # Wait for loop to stop
            time.sleep(1)
        
        logger.info("Server stopped")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Distributed Messaging Server")
    parser.add_argument("--host", type=str, default="127.0.0.1",
                      help="Host address to bind to")
    parser.add_argument("--port", type=int, default=5000,
                      help="Port to listen on")
    args = parser.parse_args()
    
    # Setup directories
    os.makedirs("./logs", exist_ok=True)
    
    # Create and start server
    server = MessageServer(args.host, args.port)
    
    if server.start():
        try:
            # Keep the main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down server...")
        finally:
            server.stop()

