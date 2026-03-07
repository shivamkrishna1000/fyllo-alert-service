# Fyllo Alert Notification Service

This service fetches live alerts from Fyllo sensor APIs and prepares notifications for farmers.  
The goal is to automatically send important alerts (e.g., irrigation requirement, crop stage issues, etc.) to farmers through WhatsApp using WATI.

Currently the system:
- Fetches live alerts from Fyllo sensors installed in R&D plots
- Filters valid alerts
- Prevents duplicate notifications
- Maps each plot to the correct farmer
- Generates notification messages
- Stores processed alerts in the database

---

## Setup

1. Clone the repository

2. Create environment file:
   cp .env.example .env

3. Fill in required environment variables

4. Install dependencies:
   pip install -r requirements.txt

5. Run Postgres locally
   docker compose up -d

5. Run tests:
   pytest

6. Run service:
   python -m app.main