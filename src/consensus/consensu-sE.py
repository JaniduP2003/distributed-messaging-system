import raftos
import threading #Used to run multiple operations in parallel on different threads.
import asyncio #perform tasks like network I/O, message replication, or heartbeat checks without freezing other parts of the system.
import logging #Used to print debugging and status messages to the console or log files.

logger = logging.getLogger("Consensus")

class Consensus:
    """
    Manages Raft consensus within the cluster.
    Handles leader election and monitors leadership status.
    """

    def __init__(self, cluster_nodes, self_node):
        """
        Initialize consensus manager.

        Args:
            cluster_nodes (list): List of node addresses in 'host:port' format
            self_node (str): This node's address (e.g., '127.0.0.1:5000')
        """
        self.cluster_nodes = cluster_nodes
        self.self_node = self_node
        self.leader = None
        self.is_leader = False
        self.stop_event = threading.Event()
        self.monitor_thread = None
        self.monitor_interval = 1  # seconds

        logger.info(f"Consensus manager initialized for {self_node} with {len(cluster_nodes)} nodes")

    async def start_consensus(self):
        """
        Configure Raft and start monitoring leadership.
        """
        logger.info("Starting Raft consensus")

        # Configure raftos
        raftos.configure({
            'log_path': f'./logs/{self.self_node.replace(":", "_")}',
            'serializer': raftos.serializers.JSONSerializer
        })

        # Register this node in the cluster
        await raftos.register(self.self_node, cluster=self.cluster_nodes)
        logger.info(f"Node {self.self_node} registered in Raft cluster.")

        # Start leader monitoring in background
        self.monitor_thread = threading.Thread(target=self._monitor_leadership, daemon=True)
        self.monitor_thread.start()

    def _monitor_leadership(self):
        """
        Monitor leadership status within the cluster using an asyncio event loop.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while not self.stop_event.is_set():
            try:
                leader = loop.run_until_complete(raftos.get_leader())

                if leader != self.leader:
                    logger.info(f"Leader changed to: {leader}")
                    self.leader = leader

                is_leader_now = (leader == self.self_node)

                if is_leader_now != self.is_leader:
                    self.is_leader = is_leader_now
                    if self.is_leader:
                        logger.info("This node is now the LEADER.")
                    else:
                        logger.info("This node is now a FOLLOWER.")
            except Exception as e:
                logger.error(f"Error monitoring leadership: {e}")

            self.stop_event.wait(self.monitor_interval)

    def is_this_node_leader(self):
        """
        Check if this node is the current leader.

        Returns:
            bool: True if this node is the leader, False otherwise
        """
        return self.is_leader

    def get_leader(self):
        """
        Get the current leader node.

        Returns:
            str or None: Current leader address or None if no leader
        """
        return self.leader

    def get_cluster_size(self):
        """
        Get the number of nodes in the cluster.

        Returns:
            int: Number of nodes
        """
        return len(self.cluster_nodes)

    def get_quorum_size(self):
        """
        Get the minimum number of nodes required for quorum.

        Returns:
            int: Quorum size
        """
        return (len(self.cluster_nodes) // 2) + 1

    def get_status(self):
        """
        Return the current consensus status as a dictionary.

        Returns:
            dict: Status information
        """
        return {
            "leader": self.leader,
            "is_leader": self.is_leader,
            "self_id": self.self_node,
            "quorum": self.get_quorum_size(),
            "cluster_size": self.get_cluster_size()
        }

    def stop(self):
        """
        Stop consensus monitoring.
        """
        logger.info("Stopping consensus monitoring")
        self.stop_event.set()

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

        self.leader = None
        self.is_leader = False
