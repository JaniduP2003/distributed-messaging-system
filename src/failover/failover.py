import time
import socket
import threading
import logging
import raftos

logger = logging.getLogger("Failover")

class Failover:
    """
    Handles automatic failover between cluster nodes
    Monitors leader status and enables recovery from node failures
    """
    def __init__(self, cluster_nodes):
        """
        Initialize the failover handler
        
        Args:
            cluster_nodes (list): List of node addresses in format 'host:port'
        """
        self.cluster_nodes = cluster_nodes
        self.current_leader = None
        self.healthy_nodes = set(cluster_nodes)
        self.stop_event = threading.Event()
        self.monitor_thread = None
        self.health_check_interval = 3  # seconds
        
        logger.info(f"Failover handler initialized with {len(cluster_nodes)} nodes")
    
    def start(self):
        """Start the failover monitoring"""
        self.monitor_thread = threading.Thread(target=self._monitor_cluster, daemon=True)
        self.monitor_thread.start()
        logger.info("Failover monitoring started")
    
    def _monitor_cluster(self):
        """Monitor cluster health and handle failovers"""
        logger.info("Starting cluster monitoring")
        
        while not self.stop_event.is_set():
            try:
                # Update node health status
                self._check_node_health()
                
                # Check current leader
                try:
                    leader = raftos.get_leader()
                    
                    if leader != self.current_leader:
                        if self.current_leader is not None:
                            logger.info(f"Leader changed from {self.current_leader} to {leader}")
                        else:
                            logger.info(f"Initial leader is {leader}")
                        
                        self.current_leader = leader
                    
                    # Check if leader is responsive
                    if leader and leader not in self.healthy_nodes:
                        logger.warning(f"Leader {leader} appears to be down, waiting for new election")
                except Exception as e:
                    logger.error(f"Error checking leader status: {e}")
                
            except Exception as e:
                logger.error(f"Error in failover monitoring: {e}")
            
            # Sleep before next check
            self.stop_event.wait(self.health_check_interval)
    
    def _check_node_health(self):
        """Check the health of all cluster nodes"""
        for node_addr in self.cluster_nodes:
            healthy = self._is_node_alive(node_addr)
            
            if healthy and node_addr not in self.healthy_nodes:
                # Node has recovered
                self.healthy_nodes.add(node_addr)
                logger.info(f"Node {node_addr} is now healthy")
            elif not healthy and node_addr in self.healthy_nodes:
                # Node has failed
                self.healthy_nodes.remove(node_addr)
                logger.warning(f"Node {node_addr} appears to be down")
    
    def _is_node_alive(self, node_addr):
        """
        Check if a node is responsive
        
        Args:
            node_addr (str): Node address in 'host:port' format
            
        Returns:
            bool: True if node is responsive, False otherwise
        """
        try:
            host, port = node_addr.split(':')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((host, int(port)))
                return True
        except Exception:
            return False
    
    def get_healthy_nodes(self):
        """
        Get a list of currently healthy nodes
        
        Returns:
            list: List of healthy node addresses
        """
        return list(self.healthy_nodes)
    
    def get_leader(self):
        """
        Get the current leader node
        
        Returns:
            str or None: Current leader address or None if no leader
        """
        return self.current_leader
    
    def stop(self):
        """Stop the failover monitoring"""
        logger.info("Stopping failover monitoring")
        self.stop_event.set()
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
