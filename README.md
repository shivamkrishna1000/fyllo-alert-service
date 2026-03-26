# 📡 Fyllo Alert Notification Service

A backend service that fetches alerts and weather data from the Fyllo platform, processes them using a rule engine, and sends actionable advisories to farmers via WhatsApp.

---

## 🚀 Overview

This service automates the full pipeline:

1. Fetch plots, alerts, sensor data, and weather forecasts from Fyllo API  
2. Validate and filter alerts  
3. Apply rule-based logic to generate advisories  
4. Merge multiple alerts into a single farmer-friendly message  
5. Send notifications via WhatsApp (WATI API)  
6. Store processed, rejected, and sent alerts in PostgreSQL  

---

## 🧠 Core Concept

The system follows a **functional core + imperative shell architecture**:

- **Functional Core (Pure Logic)**
  - `alert_processor.py`
  - Deterministic, testable logic
  - No DB or API calls

- **Imperative Shell (Side Effects)**
  - `main.py`, `fyllo_client.py`, `notification_service.py`, `database.py`
  - Handles APIs, DB, and messaging

---

## ⚙️ End-to-End Pipeline

```
Fyllo API → Alerts + Sensors + Weather
        ↓
Validation + Filtering
        ↓
Rule Engine → Advisory Messages
        ↓
WhatsApp Notification
        ↓
Database Storage + Cleanup
```

---

## 📁 Project Structure

```
app/
├── main.py                 # Entry point (pipeline orchestration)
├── fyllo_client.py         # Fyllo API integration
├── alert_processor.py      # Core business logic (rules + validation)
├── notification_service.py # WhatsApp (WATI) integration
├── database.py             # PostgreSQL operations
├── config.py               # Environment + rules loader
├── exceptions.py           # Custom exceptions
├── rules.json              # Rule engine definitions

tests/
├── test_alert_processor.py
├── test_database.py
├── test_fyllo_client.py
├── test_main_additional.py
├── test_process_flow.py
```

---

## 🔑 Environment Variables

Create a `.env` file:

```
FYLLO_BASE_URL=...
FARM_USER_ID=...
FYLLO_PASSWORD=...

DATABASE_URL=...

WATI_BASE_URL=...
WATI_TENANT_ID=...
WATI_API_TOKEN=...
WATI_TEST_NUMBER=...
WATI_TEMPLATE_NAME=...
```

---

## 🛠️ Setup Instructions

### 1. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 3. Setup Database (Local)

```bash
docker-compose up -d
```

---

### 4. Run Service

```bash
python -m app.main
```

---

## 🧪 Running Tests

```bash
pytest
```

---

## 📊 Rule Engine

Rules are defined in:

```
app/rules.json
```

Example rule:

```json
{
  "trigger": "irrigation",
  "condition": "rain_prob_gt_60",
  "message": "Soil moisture is low, but rain is likely today. Wait before irrigating."
}
```

---

## 🔍 Alert Validation Logic

Each alert is validated using:

- Supported alert type check  
- Duplicate check  
- Expiry check  
- Sensor validation  

Invalid alerts are stored in:

```
rejected_alerts
```

---

## 💬 Notification System

- Uses WATI WhatsApp API  
- Template-based messaging  
- Dynamic parameters:
  - Farmer name
  - Plot ID
  - Advisory message  

---

## 🗄️ Database Schema

### processed_alerts
Stores processed alerts to prevent duplication

### rejected_alerts
Stores invalid alerts with rejection reasons

### sent_notifications
Stores history of sent messages

---

## 🧰 Development Practices

- Functional core + imperative shell  
- Small, testable functions  
- Unit testing with pytest  
- Pre-commit hooks (black, ruff, isort)  
- High test coverage  

---