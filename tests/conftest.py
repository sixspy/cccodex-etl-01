import os

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app.db import init_db
from app.main import app


@pytest.fixture()
def client(tmp_path):
    db = tmp_path / "test.db"
    os.environ["DATABASE_PATH"] = str(db)
    os.environ["APP_SEED_SOURCE"] = "0"
    init_db(str(db), seed_source=False)
    with TestClient(app) as c:
        yield c
    db_module.set_clock(None)
    db_module.set_extract_delay(0.0)


@pytest.fixture()
def authed(client):
    r = client.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200
    return client
