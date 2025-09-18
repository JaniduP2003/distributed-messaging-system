import raftos
import threading
import time
import logging
import socket
import asyncio

logger = logging.getLogger("Consensus")

class Consensus:
    """
    Manages Raft consensus within the cluster
    Handles leader election and state replication
    """
    def __init__(self, cluster_nodes):
        """
        Initialize consensus manager
        
        Args:
            cluster_nodes (list): List of node addresses in 'host:port' format
        """
        self.cluster_nodes = cluster_nodes
        self.leader = None
        self.is_leader = False
        self.stop_event = threading.Event()
        self.monitor_thread = None
        self.monitor_interval = 1  # seconds
        
        logger.info(f"Consensus manager initialized with {len(cluster_nodes)} nodes")
    
    def start_consensus(self):
        """Start consensus monitoring"""
        logger.info("Starting Raft consensus monitoring")
        
        # Start leader monitoring in a background thread
        self.monitor_thread = threading.Thread(target=self._monitor_leadership, daemon=True)
        self.monitor_thread.start()
        
        return True
    
    def _monitor_leadership(self):
        """Monitor leadership status within the cluster"""
        while not self.stop_event.is_set():
            try:
                # Check if we are the leader
                leader = raftos.get_leader()
                
                if leader != self.leader:
                    self.leader = leader
                    logger.info(f"Leader changed to: {self.leader}")
                    
                    # Update our leader status
                    self._update_leader_status()
            
            except Exception as e:
                logger.error(f"Error monitoring leadership: {e}")
            
            # Sleep before next check
            self.stop_event.wait(self.monitor_interval)
    
    def _update_leader_status(self):
        """Update whether this node is the leader"""
        if not self.leader:
            self.is_leader = False
            return
            
        try:
            # Parse the leader address
            leader_parts = self.leader.split(':')
            if len(leader_parts) >= 2:
                leader_host = leader_parts[0]
                leader_port = int(leader_parts[1])
                
                # Check if our node is the leader
                is_leader_now = False
                for node in raftos.server.servers.values():
                    if node.host == leader_host and node.port == leader_port:
                        is_leader_now = True
                        break
                
                # Log leadership change
                if is_leader_now != self.is_leader:
                    if is_leader_now:
                        logger.info("This node is now the leader")
                    else:
                        logger.info("This node is no longer the leader")
                    
                self.is_leader = is_leader_now
        except Exception as e:
            logger.error(f"Error updating leader status: {e}")
    
    def get_leader(self):
        """
        Get the current leader node
        
        Returns:
            str or None: Current leader address or None if no leader
        """
        return self.leader
    
    def is_this_node_leader(self):
        """
        Check if this node is the current leader
        
        Returns:
            bool: True if this node is the leader, False otherwise
        """
        return self.is_leader
    
    def get_cluster_size(self):
        """
        Get the number of nodes in the cluster
        
        Returns:
            int: Number of nodes in the cluster
        """
        return len(self.cluster_nodes)
    
    def get_quorum_size(self):
        """
        Get the minimum number of nodes required for quorum
        
        Returns:
            int: Minimum number of nodes required for quorum
        """
        return (len(self.cluster_nodes) // 2) + 1
    
    def stop(self):
        """Stop consensus monitoring"""
        logger.info("Stopping consensus monitoring")
        self.stop_event.set()
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

