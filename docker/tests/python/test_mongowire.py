"""mongowire: real MongoDB wire protocol, real pymongo client, translated to Postgres
underneath. Covers find/insert/update/delete only -- mongowire doesn't implement the aggregation
pipeline, so this test doesn't reach for one.
"""
import uuid

from pymongo import MongoClient

from conftest import HOST, MONGOWIRE_PORT


def client():
    return MongoClient(host=HOST, port=MONGOWIRE_PORT, serverSelectionTimeoutMS=5000)


def test_insert_and_find_by_id():
    c = client()
    try:
        db = c[f"smoke_{uuid.uuid4().hex[:8]}"]
        coll = db["widgets"]
        doc_id = coll.insert_one({"name": "polywire"}).inserted_id
        found = coll.find_one({"_id": doc_id})
        assert found["name"] == "polywire"
    finally:
        c.close()


def test_update_and_delete():
    c = client()
    try:
        db = c[f"smoke_{uuid.uuid4().hex[:8]}"]
        coll = db["widgets"]
        doc_id = coll.insert_one({"name": "before"}).inserted_id
        coll.update_one({"_id": doc_id}, {"$set": {"name": "after"}})
        assert coll.find_one({"_id": doc_id})["name"] == "after"
        coll.delete_one({"_id": doc_id})
        assert coll.find_one({"_id": doc_id}) is None
    finally:
        c.close()
