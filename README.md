---

## Design Overview

### 1. Architecture

The system follows a **functional core + imperative shell** design:

- Functional Core:
  - `alert_processor.py`
  - Contains deterministic logic for:
    - alert validation
    - rule evaluation
    - message generation

- Imperative Shell:
  - `main.py`, `fyllo_client.py`, `notification_service.py`, `database.py`
  - Handles:
    - API calls
    - database operations
    - external integrations

This separation ensures that core logic is testable, predictable, and free of side effects.

---

### 2. Processing Pipeline

Alert processing follows a clear pipeline:

1. Fetch alerts from Fyllo API
2. Filter and validate alerts
3. Group alerts by plot
4. Apply rule engine to generate advisories
5. Format messages for each farmer
6. Send notifications and store results

Each step is implemented as a separate function to maintain modularity.

---

### 3. Key Design Decisions

#### a. Separation of Concerns
- Validation, rule evaluation, and formatting are separated into different functions.
- This avoids large monolithic functions and improves maintainability.

#### b. Pure Functions for Core Logic
- Core processing functions do not perform database operations.
- All side effects are handled outside the core logic.

#### c. Extensibility
- Message generation is modular (`build_single_plot_message`)
- Allows future integration of AI-based message formatting without changing core logic.

#### d. Testability
- Core logic is deterministic and independent of external systems.
- Enables reliable unit testing with high coverage.

---

### 4. Trade-offs

- Processed alerts are loaded into memory as a set for duplicate checking.
  - Improves simplicity and performance for current scale
  - May require optimization for very large datasets

---

### 5. Future Improvements

- Introduce stricter typing (TypedDict / dataclasses)
- Add structured logging instead of print statements
- Optimize duplicate check using indexed queries if scale increases