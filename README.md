# Kasparro Backend & ETL System

This project implements a production-grade backend and ETL system as per the Kasparro assignment (P0 & P1).

## Features

- **ETL Pipeline**: Ingests data from CSVs and Mock APIs, normalizes it, and stores it in a database.
- **Incremental Ingestion**: 
  - Uses **Checkpoints** (P1.2) to track the last processed timestamp for API sources.
  - Uses **Deduplication** to prevent re-processing identical raw payloads.
- **API**: FastAPI-based REST API with pagination, filtering, and health checks.
- **Dockerized**: Fully containerized with Docker Compose.

## Setup & Run

### Prerequisites
- Docker & Docker Compose
- Make (optional, for Windows use `docker-compose` directly)

### Running the System

1. **Start the services:**
   ```bash
   make up
   # OR
   docker-compose up --build -d
   ```

2. **Check Health:**
   Open `http://localhost:8000/health`

3. **View Data:**
   Open `http://localhost:8000/data`

4. **API Documentation:**
   Open `http://localhost:8000/docs`

### Running Tests

```bash
make test
# OR
docker-compose run --rm app pytest
```

## Cloud Deployment (Evaluation Requirement)

To deploy this to a cloud provider (e.g., Azure, AWS, GCP):

1.  **Build & Push Image**:
    ```bash
    docker build -t your-registry/kasparro-backend:latest .
    docker push your-registry/kasparro-backend:latest
    ```
2.  **Deploy Container**: Use Azure App Service, AWS ECS, or Google Cloud Run to deploy the image.
3.  **Database**: Provision a managed PostgreSQL instance (e.g., Azure Database for PostgreSQL) and set the `DATABASE_URL` environment variable.
4.  **Cron Job**:
    *   **Azure**: Use a Logic App or Function App to hit an endpoint that triggers ETL (or run the container as a scheduled task).
    *   **AWS**: Use EventBridge Scheduler to trigger an ECS task or Lambda.
    *   **GCP**: Use Cloud Scheduler to hit a Cloud Run endpoint.

## Project Structure

- `app/api`: API endpoints and configuration.
- `app/ingestion`: ETL pipeline logic and source connectors.
- `app/core`: Database and application configuration.
- `app/schemas`: Pydantic and SQLModel definitions.
- `tests`: Pytest suite.
- `data`: Sample data files.

## Design Decisions

- **SQLModel**: Used for both ORM and Pydantic schemas to reduce duplication.
- **Raw vs Unified**: We store raw payloads first (EL) then Transform (T) to allow re-processing if logic changes.
- **Background ETL**: For this demo, the ETL runs in a background thread on startup. In production, this would be a Celery task or Cron job.
