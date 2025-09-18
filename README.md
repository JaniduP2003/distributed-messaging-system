# distributed-messaging-system
Distributed Messaging System with Fault Tolerance, Consensus, Data Replication, and Time Synchronization. Implements failover, Raft-based leader election, strong consistency with replicated storage, and synchronized timestamps for reliable message delivery across servers

# Distributed Messaging System with Fault Tolerance, Consensus & Time Synchronization

## 📌 Overview
This project is a distributed messaging system that provides:
- **Fault Tolerance** through failover mechanisms.
- **Consensus** using Raft for consistent state across nodes.
- **Data Replication & Consistency** across servers.
- **Time Synchronization** for accurate event ordering.

It ensures continuous service even in the presence of node failures, while keeping replicated data strongly consistent across the cluster.

---

## 🚀 Features
### 🔹 Fault Tolerance (Failover)
- Detects failed nodes using TCP health checks.
- Automatically redirects clients to healthy servers.
- Maintains a live list of available nodes.

### 🔹 Consensus (Raft)
- Implements leader election.
- Ensures agreement on message order across all nodes.
- Guarantees a consistent global system state.

### 🔹 Data Replication & Consistency
- Uses `raftos.ReplicatedDict` and `raftos.ReplicatedList`.
- Provides APIs to store, retrieve, and delete messages.
- Enforces strong consistency and deduplication via unique message IDs.

### 🔹 Time Synchronization
- Synchronizes node time using **NTP servers**.
- Provides accurate timestamps for message events.
- Ensures chronological consistency across distributed nodes.

---

## 🛠️ Project Structure
```
src/
│── server.py              # Main server entrypoint
│── client.py              # Client entrypoint
│── consensus/consensus.py # Consensus logic with Raft
│── message_storage/       # Replicated message storage
│     └── storage.py
│── failover/              # Failover and node health monitoring
│── timesync/              # Time synchronization module
```

---

## 👥 Contributors
- **IT23184558 - D B Y BINUWARA**  
  _Fault Tolerance & Failover_

- **IT23294998 - PABASARA T D J**  
  _Time Synchronization_

- **IT23398184 - DINUJAYA K V T**  
  _Data Replication & Consistency_

- **IT23424432 - ABEYRATHNA J G D A**  
  _Consensus Implementation_

---

## ⚙️ Setup & Running

### 1️⃣ Create and Activate Virtual Environment
```bash
python -m venv venv
.env\Scriptsctivate   # Windows
source venv/bin/activate  # Linux/Mac
```

### 2️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```

### 3️⃣ Start Servers
Run the **primary server**:
```bash
cd src
python server.py
```

Run **backup servers** in separate terminals:
```bash
python server.py --host localhost --port 5001
python server.py --host localhost --port 5002
```

### 4️⃣ Start Clients
Run clients in separate terminals:
```bash
python client.py
```

---

## 📡 How It Works
- Clients connect to the cluster through the **ClusterServer**.
- **Failover** detects and removes unhealthy servers, redirecting clients.
- **Consensus** ensures all servers agree on the order/state of messages.
- **Replication** guarantees that all messages are stored consistently across nodes.
- **TimeSync** provides consistent timestamps for all distributed events.

---



