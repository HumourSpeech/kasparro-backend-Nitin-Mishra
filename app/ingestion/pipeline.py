import json
from datetime import datetime
from sqlmodel import Session, select
from app.core.db import engine
from app.schemas.models import RawData, UnifiedUser, ETLJob, Checkpoint
from app.ingestion.sources import CSVSource, MockAPISource
from app.core.config import settings

def run_etl():
    with Session(engine) as session:
        # Start Job
        job = ETLJob(start_time=datetime.now(), status="running")
        session.add(job)
        session.commit()
        session.refresh(job)

        try:
            # 1. Ingest from CSV
            csv_source = CSVSource("data/source1.csv")
            csv_data = csv_source.fetch()
            process_raw_data(session, csv_data, "csv")

            # 2. Ingest from API (with Checkpoint)
            # Fetch last checkpoint
            ckpt = session.get(Checkpoint, "api")
            last_val = ckpt.last_processed_id if ckpt else None
            
            api_source = MockAPISource("https://api.example.com/users", settings.API_KEY)
            api_data = api_source.fetch(last_checkpoint=last_val)
            
            if api_data:
                process_raw_data(session, api_data, "api")
                # Update checkpoint to the max 'joined' date found
                max_joined = max(d["joined"] for d in api_data)
                if ckpt:
                    ckpt.last_processed_id = max_joined
                    ckpt.last_processed_timestamp = datetime.utcnow()
                else:
                    ckpt = Checkpoint(source_name="api", last_processed_id=max_joined, last_processed_timestamp=datetime.utcnow())
                session.add(ckpt)
                session.commit()

            # 3. Ingest from Quirky CSV (P1.1)
            csv2_source = CSVSource("data/source2.csv")
            csv2_data = csv2_source.fetch()
            process_raw_data(session, csv2_data, "csv_quirky")

            # 4. Normalize & Load
            processed_count = normalize_data(session)
            
            job.status = "success"
            job.end_time = datetime.utcnow()
            job.records_processed = processed_count
            session.add(job)
            session.commit()
            print(f"ETL Job {job.id} completed successfully. Processed {processed_count} records.")

        except Exception as e:
            job.status = "failed"
            job.end_time = datetime.utcnow()
            job.error_message = str(e)
            session.add(job)
            session.commit()
            print(f"ETL Job {job.id} failed: {e}")

def process_raw_data(session: Session, data: list, source_name: str):
    for item in data:
        # Check for duplicates based on payload content to avoid re-ingesting same raw data
        # In a real system, we might use a hash of the payload or a unique ID from the source
        payload_str = json.dumps(item)
        existing = session.exec(select(RawData).where(RawData.payload == payload_str)).first()
        if not existing:
            raw = RawData(source=source_name, payload=payload_str)
            session.add(raw)
    session.commit()

def normalize_data(session: Session) -> int:
    # Fetch unprocessed raw data
    unprocessed = session.exec(select(RawData).where(RawData.processed == False)).all()
    count = 0
    
    for raw in unprocessed:
        try:
            data = json.loads(raw.payload)
            unified = None
            
            if raw.source == "csv":
                unified = UnifiedUser(
                    original_id=data.get("id"),
                    name=data.get("name"),
                    email=data.get("email"),
                    role=data.get("role"),
                    signup_date=datetime.strptime(data.get("signup_date"), "%Y-%m-%d"),
                    source="csv"
                )
            elif raw.source == "api":
                unified = UnifiedUser(
                    original_id=data.get("id"),
                    name=data.get("full_name"),
                    email=data.get("contact"),
                    role=data.get("access"),
                    signup_date=datetime.strptime(data.get("joined"), "%Y-%m-%d"),
                    source="api"
                )
            elif raw.source == "csv_quirky":
                unified = UnifiedUser(
                    original_id=data.get("user_id"),
                    name=data.get("full_name"),
                    email=data.get("contact_email"),
                    role=data.get("user_role"),
                    signup_date=datetime.strptime(data.get("registered_at"), "%Y-%m-%d"),
                    source="csv_quirky"
                )
            
            if unified:
                # Upsert logic (simple check by email for now)
                existing = session.exec(select(UnifiedUser).where(UnifiedUser.email == unified.email)).first()
                if existing:
                    existing.name = unified.name
                    existing.role = unified.role
                    session.add(existing)
                else:
                    session.add(unified)
                
                raw.processed = True
                session.add(raw)
                count += 1
        except Exception as e:
            print(f"Error processing raw record {raw.id}: {e}")
            # Optionally mark as processed but with error, or leave for retry
            
    session.commit()
    return count

if __name__ == "__main__":
    # For manual testing
    from app.core.db import init_db
    init_db()
    run_etl()
