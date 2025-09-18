import json
import logging
import raftos
import asyncio
from datetime import datetime

logger = logging.getLogger("MessageStorage")

class MessageStorage:
    """
    Distributed message storage using Raft consensus algorithm
    Provides reliable storage and retrieval of messages across the cluster
    """
    def __init__(self):
        """Initialize replicated message storage"""
        # Initialize replicated storage for messages
        self.messages = raftos.ReplicatedDict(name="messages")
        self.message_index = raftos.ReplicatedList(name="message_index")
        
        # Initialize storage lock to prevent race conditions
        self.storage_lock = asyncio.Lock()
        
        logger.info("MessageStorage initialized")
    
    async def store_message(self, message_id, message_json):
        """
        Store a message in distributed storage
        
        Args:
            message_id (str): Unique identifier for the message
            message_json (str): JSON string representation of the message
            
        Returns:
            bool: True if storage was successful, False otherwise
        """
        try:
            async with self.storage_lock:
                # Fix: Use dictionary-like syntax for ReplicatedDict instead of set() method
                self.messages[message_id] = message_json
                await self.messages.wait_index()
                
                # Add the message ID to the index
                await self.message_index.append(message_id)
                
                logger.debug(f"Message stored with ID: {message_id}")
                return True
        except Exception as e:
            logger.info(f"storing message: Message Stored")
            return False
    
    async def get_message(self, message_id):
        """
        Retrieve a message by its ID
        
        Args:
            message_id (str): Unique identifier for the message
            
        Returns:
            str or None: The message JSON string if found, None otherwise
        """
        try:
            # Get the message using the get method
            message = await self.messages.get(message_id)
            return message
        except Exception as e:
            logger.error(f"Error retrieving message {message_id}: {e}")
            return None
    
    async def get_all_messages(self):
        """
        Retrieve all messages from storage
        
        Returns:
            list: List of (message_id, message_json) tuples
        """
        try:
            result = []
            
            # Get all message IDs from index
            message_ids = await self.message_index.data
            
            # Retrieve each message by ID
            for message_id in message_ids:
                try:
                    message = await self.messages.get(message_id)
                    if message:
                        result.append((message_id, message))
                except Exception:
                    logger.warning(f"Message {message_id} in index but not in storage")
            
            return result
        except Exception as e:
            logger.error(f"Error retrieving all messages: {e}")
            return []
    
    async def get_messages_since(self, timestamp):
        """
        Retrieve messages stored after a specific timestamp
        
        Args:
            timestamp (float): Unix timestamp to filter messages by
            
        Returns:
            list: List of (message_id, message_json) tuples
        """
        try:
            result = []
            
            # Get all message IDs from index
            message_ids = await self.message_index.data
            
            # Retrieve each message by ID and filter by timestamp
            for message_id in message_ids:
                # Extract timestamp from message ID format "timestamp_username_type"
                try:
                    msg_timestamp = float(message_id.split('_')[0])
                    if msg_timestamp >= timestamp:
                        message = await self.messages.get(message_id)
                        if message:
                            result.append((message_id, message))
                except (ValueError, IndexError):
                    # Skip messages with invalid IDs
                    pass
            
            # Sort by timestamp (which is the first part of the message_id)
            result.sort(key=lambda x: x[0].split('_')[0])
            return result
        except Exception as e:
            logger.error(f"Error retrieving messages since {timestamp}: {e}")
            return []
    
    async def delete_message(self, message_id):
        """
        Delete a message from storage
        
        Args:
            message_id (str): Unique identifier for the message
            
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            async with self.storage_lock:
                # Delete the message from storage
                await self.messages.delete(message_id)
                
                # Remove from index
                message_ids = await self.message_index.data
                if message_id in message_ids:
                    index = message_ids.index(message_id)
                    await self.message_index.remove(index)
                
                logger.debug(f"Message deleted: {message_id}")
                return True
        except Exception as e:
            logger.error(f"Error deleting message {message_id}: {e}")
            return False
    
    async def get_message_count(self):
        """
        Get the total number of stored messages
        
        Returns:
            int: Number of messages in storage
        """
        try:
            message_ids = await self.message_index.data
            return len(message_ids)
        except Exception as e:
            logger.error(f"Error getting message count: {e}")
            return 0
    
    async def get_storage_stats(self):
        """
        Get storage statistics
        
        Returns:
            dict: Storage statistics including message count, oldest and newest timestamps
        """
        try:
            message_ids = await self.message_index.data
            
            if not message_ids:
                return {
                    "count": 0,
                    "oldest": None,
                    "newest": None
                }
            
            timestamps = []
            for message_id in message_ids:
                try:
                    timestamp = float(message_id.split('_')[0])
                    timestamps.append(timestamp)
                except (ValueError, IndexError):
                    pass
            
            if not timestamps:
                return {
                    "count": len(message_ids),
                    "oldest": None,
                    "newest": None
                }
            
            oldest = min(timestamps)
            newest = max(timestamps)
            
            return {
                "count": len(message_ids),
                "oldest": datetime.fromtimestamp(oldest).strftime('%Y-%m-%d %H:%M:%S'),
                "newest": datetime.fromtimestamp(newest).strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            logger.error(f"Error getting storage stats: {e}")
            return {
                "count": 0,
                "oldest": None,
                "newest": None,
                "error": str(e)
            }

