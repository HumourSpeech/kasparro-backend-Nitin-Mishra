from app.schemas.models import UnifiedUser, RawData
from datetime import datetime
import json
from sqlmodel import select, func

def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "database" in data
    assert "etl" in data

def test_get_data_empty(client):
    response = client.get("/data")
    assert response.status_code == 200
    data = response.json()
    assert data["data"] == []
    assert data["metadata"]["total_records"] == 0

def test_get_data_with_records(client, session):
    user = UnifiedUser(
        original_id="1",
        name="Test User",
        email="test@example.com",
        role="user",
        signup_date=datetime.utcnow(),
        source="test"
    )
    session.add(user)
    session.commit()

    response = client.get("/data")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 1
    assert data["data"][0]["name"] == "Test User"

def test_stats_endpoint(client):
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert "total_records_processed" in data

def test_etl_normalization_logic(session):
    # Simulate Raw Data
    raw_payload = json.dumps({
        "id": "999",
        "name": "ETL Test",
        "email": "etl@test.com",
        "role": "admin",
        "signup_date": "2023-01-01"
    })
    raw = RawData(source="csv", payload=raw_payload, processed=False)
    session.add(raw)
    session.commit()

    # Run normalization logic (importing function directly for unit test)
    from app.ingestion.pipeline import normalize_data
    count = normalize_data(session)
    
    assert count == 1
    
    # Verify UnifiedUser created
    user = session.exec(select(UnifiedUser).where(UnifiedUser.email == "etl@test.com")).first()
    assert user is not None
    assert user.name == "ETL Test"
    
    # Verify RawData marked processed
    session.refresh(raw)
    assert raw.processed is True

def test_incremental_ingestion_logic(session):
    # 1. Insert a raw record
    payload = json.dumps({"id": "1", "val": "test"})
    raw1 = RawData(source="test", payload=payload)
    session.add(raw1)
    session.commit()
    
    # 2. Try to insert same payload again (simulating process_raw_data logic)
    from app.ingestion.pipeline import process_raw_data
    process_raw_data(session, [{"id": "1", "val": "test"}], "test")
    
    # 3. Verify only 1 record exists
    count = session.exec(select(func.count(RawData.id))).one()
    assert count == 1

def test_checkpoint_logic(session):
    from app.schemas.models import Checkpoint
    from app.ingestion.sources import MockAPISource
    
    # 1. Create a checkpoint
    ckpt = Checkpoint(source_name="api", last_processed_id="2023-02-02", last_processed_timestamp=datetime.utcnow())
    session.add(ckpt)
    session.commit()
    
    # 2. Fetch from source using checkpoint
    source = MockAPISource("url", "key")
    data = source.fetch(last_checkpoint="2023-02-02")
    
    # 3. Should only return records AFTER 2023-02-02
    # Mock data has: 2023-02-01, 2023-02-02, 2023-03-01
    assert len(data) == 1
    assert data[0]["joined"] == "2023-03-01"
