# 🏆 Graph-Based Money Muling Detection Engine
### RIFT 2026 – Financial Graph Intelligence System

A graph-theoretic fraud detection engine designed to identify money muling rings, smurfing behavior, and layered shell transaction networks from structured CSV transaction data.

This system leverages directed graph modeling and network analysis to detect suspicious financial behavior in an explainable and scalable manner.

# 🌐 Live Demo

🔗 https://graph-fraud-detection-engine.onrender.com/


# 🧰 Tech Stack

## 🔹 Backend
- Python 3.10+
- Flask (Web Framework)
- Pandas (Data Processing & Validation)
- NetworkX (Graph Modeling & Analysis)
- NumPy (Risk Scoring & Statistical Computation)
- Gunicorn (Production WSGI Server)

## 🔹 Frontend
- HTML5
- CSS3 (Responsive Dashboard UI)
- Vanilla JavaScript
- Cytoscape.js (Interactive Graph Visualization)

  ## 🔄 System Workflow

The application follows a structured data-processing and visualization pipeline to detect fraudulent patterns in transactional data.

### 🧩 Workflow Steps

1. **User Uploads CSV**
   - Users upload transaction data in CSV format through the web interface or API.

2. **Schema Validation (Pandas)**
   - The uploaded CSV is validated using Pandas to ensure:
     - Required columns are present  
     - Correct data types  
     - No missing or malformed values  

3. **Directed Graph Construction (NetworkX)**
   - A directed graph is created where:
     - Nodes represent entities (accounts, users, etc.)
     - Edges represent transactions and relationships

4. **Fraud Detection Algorithms**
   The system applies multiple graph-based detection techniques:
   - **Cycle Detection (Strongly Connected Components)**  
     Identifies circular money movement patterns.
   - **Smurfing Detection (Fan-in / Fan-out Analysis)**  
     Detects many-to-one or one-to-many transaction behavior.
   - **Layering Detection**  
     Identifies multi-step transaction chains used to hide funds.
   - **Hub Analysis**  
     Detects high-degree nodes acting as transaction hubs.

5. **Suspicion Scoring Engine**
   - Each node and transaction is assigned a risk score based on:
     - Algorithm results  
     - Transaction frequency  
     - Network behavior patterns  

6. **JSON Response**
   - Processed graph data and suspicion scores are returned as a structured JSON response.

7. **Interactive Graph Visualization (Cytoscape.js)**
   - The JSON response is rendered as an interactive network graph.
   - Users can:
     - Explore relationships  
     - Highlight suspicious entities  
     - Filter and analyze patterns visually  

---

### 📌 Key Highlights
- Graph-based fraud detection  
- Scalable and modular pipeline  
- Real-time interactive visualization  
- Suitable for FinTech and cybersecurity use cases
### Data Flow Overview

1. CSV file uploaded by user.
2. Strict schema validation ensures compliance.
3. Transactions converted into a directed graph.
4. Fraud detection algorithms analyze network structure.
5. Suspicion scores calculated per account.
6. Results displayed in interactive dashboard.

---

# 🧠 Algorithm Approach

## 1️⃣ Graph Construction

Each account → Node  
Each transaction → Directed Edge  

Edges are aggregated between account pairs.

**Time Complexity:**  
O(T)  
Where T = number of transactions.

---

## 2️⃣ Circular Fund Routing (Cycle Detection)

Uses Strongly Connected Components (Tarjan’s Algorithm).

Detects:
- Cycles of length ≥ 3
- Fraud rings via SCC clusters

**Time Complexity:**  
O(V + E)

Where:
- V = number of accounts
- E = number of edges

---

## 3️⃣ Smurfing Detection (Fan-in / Fan-out)

Fan-In:
- ≥10 senders → 1 aggregator within 72-hour window

Fan-Out:
- 1 sender → ≥10 receivers within 72-hour window

**Time Complexity:**  
O(T log T)  
(due to time sorting per sender)

---

## 4️⃣ Layered Shell Network Detection

Identifies:
- 3+ hop transaction chains
- Intermediate accounts with low transaction count
- Balanced incoming/outgoing flow

**Time Complexity:**  
O(V + E)

---

## 5️⃣ Hub Detection

Accounts with unusually high in-degree + out-degree relative to network percentile.

Used to detect:
- Central laundering hubs
- High-risk aggregation points

---

## 📊 Overall System Complexity

For datasets up to 10,000 transactions:

O(V + E)

Performance Tests:
- 5K transactions → < 3 seconds
- 10K transactions → < 10 seconds

Meets RIFT requirement: ≤ 30 seconds.

---

# 🎯 Suspicion Score Methodology

Each account is assigned a risk score (0–100).

### Pattern Weights

| Pattern | Score Contribution |
|---------|--------------------|
| Fraud Ring Membership | +35 |
| Rapid Transfer Burst | +25 |
| High Degree Hub | +20 |
| Layering Pattern | +20 |

Additional Boosts:
- Log-scaled degree contribution
- Log-scaled total transaction flow

Final Score:

## 🔹 Deployment
- Render (Cloud Hosting)
- GitHub (Version Control)
## 🎯 Suspicion Scoring Logic

```python
score = min(100, weighted_sum + activity_boost)
🚨 Suspicion Threshold

score ≥ 40 → Suspicious

📊 Risk Tiers

0–39 → Low Risk

40–74 → Medium Risk

75–100 → High Risk

✅ This ensures:

High precision

Controlled false positives

Explainable scoring
## Installation & Setup
1️⃣ Clone Repository
git clone https://github.com/yourusername/graph-fraud-detection-engine.git
cd graph-fraud-detection-engine

2️⃣ Create Virtual Environment
python -m venv venv

Activate:

Windows
venv\Scripts\activate

Mac / Linux
source venv/bin/activate

3️⃣ Install Dependencies
pip install -r requirements.txt

4️⃣ Run Locally
python app.py

Open in browser:
http://127.0.0.1:5000