import ntplib     #ntlib for ntp
import time       #time for local time00
import threading     #thredig for background sync
import logging       #logging for stetus messeges

logger = logging.getLogger("TimeSync")

class TimeSync:
    """
    Time synchronization using NTP protocol
    Ensures consistent timestamps across distributed systems
    """
    def __init__(self, ntp_servers=None):                 ## intitalization of  NTP cliaint
        """
        Initialize the TimeSync component
        
        Args:
            ntp_servers (list): List of NTP servers to use, defaults to pool.ntp.org
        """
        self.client = ntplib.NTPClient()
        self.offset = 0  # Time offset between local time and NTP time
        self.sync_lock = threading.Lock()     ##uses for locks fors sefty
        self.stop_event = threading.Event()
        self.sync_successful = False
        
        # NTP servers to try (in order)
        self.ntp_servers = ntp_servers or [
            'pool.ntp.org', 
            'time.google.com',
            'time.windows.com',
            'time.apple.com'
        ]

    def sync_time(self):       ##for syncronization time ,trys each ntp server until works
        """
        Synchronize time with NTP servers
        
        Returns:
            bool: True if time synchronization was successful, False otherwise
        """
        try:
            with self.sync_lock:
                # Try each NTP server until successful
                for server in self.ntp_servers:
                    try:
                        logger.info(f"Attempting to sync time with {server}")
                        response = self.client.request(server, timeout=5)
                        self.offset = response.offset
                        self.sync_successful = True
                        logger.info(f"Time synchronized with {server}, offset: {self.offset:.6f} seconds")
                        break
                    except Exception as e:
                        logger.warning(f"Failed to sync with {server}: {e}")
                
                if not self.sync_successful:
                    logger.warning("Failed to sync with any NTP server, using local time")
                    return False
                
                # Start a thread to periodically sync time   background thrad
                sync_thread = threading.Thread(target=self._periodic_sync, daemon=True)
                sync_thread.start()
                
                logger.info(f"Current synchronized time: {self.get_synced_time_str()}")
                return True
                
        except Exception as e:
            logger.error(f"Error during time synchronization: {e}")
            return False
    
    def _periodic_sync(self):                                #Periodically synchronize
        """Periodically synchronize time with NTP servers"""
        # Initial short delay to make sure synchronization works
        retry_delay = 60  # 1 minute for initial retry
        
        while not self.stop_event.is_set():
            # Wait for the specified delay
            if self.stop_event.wait(retry_delay):
                break
                
            try:
                with self.sync_lock:
                    for server in self.ntp_servers:
                        try:
                            response = self.client.request(server, timeout=5)
                            old_offset = self.offset
                            self.offset = response.offset
                            
                            # Log only if the offset changed significantly
                            if abs(old_offset - self.offset) > 0.1:  # 100ms threshold
                                logger.info(f"Time re-synchronized with {server}, new offset: {self.offset:.6f} seconds")
                            self.sync_successful = True
                            break
                        except Exception as e:
                            logger.debug(f"Failed periodic sync with {server}: {e}")
                    
                    if not self.sync_successful:
                        logger.warning("Failed periodic sync with any NTP server")
                
                # Success - use longer delay for future syncs
                retry_delay = 3600  # 1 hour between routine syncs
                
            except Exception as e:
                logger.error(f"Error during periodic time sync: {e}")
                retry_delay = 300  # 5 minutes on error
    
    def get_synced_time(self):        ##get the sync time ,return the current acurate time
        """
        Get current time adjusted by the NTP offset
        
        Returns:
            float: Current adjusted timestamp (seconds since epoch)
        """
        return time.time() + self.offset
    
    def get_synced_time_str(self):
        """
        Get formatted synchronized time string
        
        Returns:
            str: Current time in human-readable format
        """
        synced_time = self.get_synced_time()
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(synced_time))
    
    def stop(self):
        """Stop the periodic time synchronization"""
        logger.info("Stopping time synchronization")
        self.stop_event.set()


        ##use for sysnic for debaginh masage odaring ,fult tolarance