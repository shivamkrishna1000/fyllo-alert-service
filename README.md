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