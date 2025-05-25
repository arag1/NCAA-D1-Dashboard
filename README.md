# 🥎 NCAA D1 Baseball Real-Time Dashboard & Recruiter Assistant

This project is a real-time NCAA Division 1 Baseball statistics dashboard built using **PySpark**, **Kafka**, **Flask**, and **WebSocket streaming**, featuring:

- 🔄 Live individual and team stats updated in-memory
- 📊 Interactive dashboard with dynamic filters and sorting
- 💬 Recruiter Chatbot powered by LLMs with LangChain integration
- 📦 Modular and scalable microservice setup with Docker

---

## 🚀 Features

### ✅ Real-Time Stats Dashboard

- Displays live NCAA D1 individual and team baseball statistics
- Fully sortable tables by metrics such as Batting Average, ERA, Home Runs/Game, etc.
- Filters by Team, Class (Cl), Position (Pos), and stat category
- Live updates via PySpark Structured Streaming and Kafka

### 🧠 Recruiter Chatbot (LLM-Enabled)

- Natural language assistant for scouts and recruiters
- Uses current in-memory tables to provide metric-based insights
- Automatically understands selected filters and stat type
- Built on LangChain and OpenAI-compatible APIs

---

## 🧩 Architecture Overview

Kafka Topic --> PySpark Consumer --> Memory Tables --> Flask API --> Live Dashboard + Chatbot


- **PySpark**: Consumes Kafka topics, updates `memory_tables` by `stat_category`
- **Flask App**: Serves the frontend and chatbot, exposes API endpoints
- **Dashboard UI**: Displays dynamic stat tables
- **Recruiter Chatbot**: Queries updated Spark tables for natural language Q&A

---

## 🛠️ Setup Instructions

### Prerequisites

- Python 3.8+
- Java 11+
- Apache Kafka
- Docker + Docker Compose (recommended)

### 🐳 Run with Docker Compose

docker compose up --build

### 🔧 Manual Local Setup


# Create virtualenv
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Access website
http://localhost:5001/

### For using OpenAI + Langchain
Make sure to generate and download OpenAI key. Save it to .env file. 
