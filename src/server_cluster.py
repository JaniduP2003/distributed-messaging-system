import socket
import threading
import time
import json
import asyncio
import logging
import os
import sys
import signal

import raftos

from message_storage.storage import MessageStorage
from failover.failover import Failover
from consensus.consensus import Consensus
from time_sync.timesync import TimeSync

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("ClusterServer")

class ClusterServer:
    def __init__(self, cluster_nodes, node_id):
        """
        Initialize a cluster server node
        
        Args:
            cluster_nodes (list): List of node addresses in format 'host:port'
            node_id (int): Index of this node in the cluster_nodes list
        """
        self.cluster_nodes = cluster_nodes
        self.node_id = node_id
        self.node_addr = cluster_nodes[node_id]
        self.host, self.port = self.node_addr.split(':')
        self.port = int(self.port)
        
        # Create log directory if it doesn't exist
        log_dir = f'./logs/{self.node_id}'
        os.makedirs(log_dir, exist_ok=True)
        
        # Setup event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Configure raftos
        raftos.configure({
            'log_path': log_dir,
            'serializer': raftos.serializers.JSONSerializer
        })
        
        # Register this node with raftos
        raftos.register(self.host, self.port)
        
        # Register other nodes
        for i, addr in enumerate(cluster_nodes):
            if i != node_id:  # Skip self
                host, port = addr.split(':')
                raftos.register(host, int(port))
        
        # Initialize components
        self.time_sync = TimeSync()
        self.message_storage = MessageStorage()
        self.failover = Failover(cluster_nodes)
        self.consensus = Consensus(cluster_nodes)
        
        # Server socket for handling client connections
        self.server_socket = None
        self.running = False
        self.clients = {}  # {client_socket: username}
        self.clients_lock = threading.Lock()
        
        # Signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        logger.info(f"Initialized server node {node_id} at {self.node_addr}")
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {sig}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def setup_server_socket(self):
        """Initialize and bind the server socket"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            logger.info(f"Server socket bound to {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to bind server socket: {e}")
            return False
    
    async def store_welcome_message(self):
        """Store a welcome message in the distributed storage"""
        try:
            start_time = self.time_sync.get_synced_time()      ##sysnc time call
            
            message_data = {
                "type": "system",
                "sender": "system",
                "content": "Welcome to the distributed messaging system",
                "timestamp": start_time
            }
            
            message_id = f"{int(start_time)}_system_welcome"
            await self.message_storage.store_message(message_id, json.dumps(message_data))
            logger.info("Welcome message stored in distributed storage")
            
            # Fetch and verify the message was stored
            stored_message = await self.message_storage.get_message(message_id)
            if stored_message:
                logger.info("Successfully verified message storage")
            else:
                logger.warning("Could not verify message storage")
                
            return True
        except Exception as e:
            logger.error(f"Error storing welcome message: {e}")
            return False
    
    def accept_connections(self):
        """Accept incoming client connections"""
        logger.info("Starting to accept client connections")
        
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
        """Handle communication with a connected client"""
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
                        
                        # Add accurate timestamp
                        timestamp = self.time_sync.get_synced_time()
                        
                        # Send welcome message to client
                        self.send_system_message(client_socket, f"Welcome, {username}!")
                        
                        # Broadcast join message to other clients
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
        """Store a message in the distributed storage asynchronously"""
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
        """Send a system message to a specific client"""
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
        """Broadcast a message to all connected clients"""
        with self.clients_lock:
            clients = list(self.clients.keys())
        
        data = json.dumps(message).encode('utf-8')
        
        for client_socket in clients:
            if client_socket != exclude:
                try:
                    client_socket.send(data)
                except Exception as e:
                    logger.error(f"Error broadcasting to client: {e}")
                    # Client will be removed on next message receive
    
    def start(self):
        """Start the server"""
        # Setup server socket
        if not self.setup_server_socket():
            logger.error("Failed to setup server socket")
            return False
        
        # Start time synchronization
        if not self.time_sync.sync_time():
            logger.warning("Failed to synchronize time, will use local time instead")
        
        # Start the server
        self.running = True
        
        # Accept client connections in a separate thread
        threading.Thread(target=self.accept_connections, daemon=True).start()
        
        # Start components
        self.failover.start()
        self.consensus.start_consensus()
        
        logger.info(f"Server started successfully on {self.host}:{self.port}")
        return True
    
    def run_server(self):
        """Run the server, including all components"""
        try:
            # Start the server
            self.start()
            
            # Run initialization tasks
            self.loop.run_until_complete(self.store_welcome_message())
            
            # Run the event loop
            logger.info("Server running and ready to accept connections")
            self.loop.run_forever()
        except KeyboardInterrupt:
            logger.info("Server shutdown requested")
        except Exception as e:
            logger.error(f"Error running server: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the server and all components"""
        logger.info("Stopping server")
        
        # Stop accepting new connections
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
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        # Stop components
        self.failover.stop()
        self.consensus.stop()
        
        # Stop event loop
        try:
            self.loop.stop()
            
            # Run any pending tasks
            pending = asyncio.all_tasks(self.loop)
            if pending:
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            
            self.loop.close()
        except Exception as e:
            logger.error(f"Error stopping event loop: {e}")
        
        logger.info("Server stopped")


if __name__ == "__main__":
    # Define cluster nodes
    cluster_nodes = ['127.0.0.1:5000', '127.0.0.1:5001', '127.0.0.1:5002']
    
    # Get node ID from command line or default to 0
    node_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    
    # Create and run server
    server = ClusterServer(cluster_nodes, node_id)
    server.run_server()