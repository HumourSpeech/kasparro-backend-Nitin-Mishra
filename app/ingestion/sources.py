import csv
import json
import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime

class DataSource:
    def fetch(self, last_checkpoint: Optional[Any] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError

class CSVSource(DataSource):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def fetch(self, last_checkpoint: Optional[Any] = None) -> List[Dict[str, Any]]:
        # For CSV, a simple checkpoint could be the number of rows previously read
        # or we just read everything and let the deduplication logic handle it (as per P0).
        # For P1.2, let's assume we read everything but we could implement file-pointer logic if needed.
        # Here we stick to reading all, relying on the pipeline's payload deduplication for idempotency.
        data = []
        try:
            with open(self.file_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append(row)
        except FileNotFoundError:
            print(f"Warning: CSV file not found at {self.file_path}")
        return data

class MockAPISource(DataSource):
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key

    def fetch(self, last_checkpoint: Optional[str] = None) -> List[Dict[str, Any]]:
        # Simulating an API call. In a real scenario, we'd use httpx.get
        # headers = {"Authorization": f"Bearer {self.api_key}"}
        # params = {"since": last_checkpoint} if last_checkpoint else {}
        # response = httpx.get(self.api_url, headers=headers, params=params)
        # return response.json()
        
        # Returning mock data
        all_data = [
            {"id": "101", "full_name": "David Mock", "contact": "david@mock.com", "joined": "2023-02-01", "access": "admin"},
            {"id": "102", "full_name": "Eve Mock", "contact": "eve@mock.com", "joined": "2023-02-02", "access": "viewer"},
            {"id": "103", "full_name": "Frank New", "contact": "frank@mock.com", "joined": "2023-03-01", "access": "user"},
        ]
        
        if last_checkpoint:
            # Filter data joined AFTER the checkpoint date
            filtered_data = [
                d for d in all_data 
                if d["joined"] > last_checkpoint
            ]
            return filtered_data
            
        return all_data
