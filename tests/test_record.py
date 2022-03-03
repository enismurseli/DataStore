import random, string
from datastore.record import PrimitiveRecord, JsonRecord
from datastore import destination
from datastore import data

import pytest

def test_attributes_json(json_records):
    for record in json_records:
        # Check if value is in expected data types list
        assert record.key == record.key_data
        assert record.value_type in ['int', 'str', 'float', 'bool', 'dict']

        # Check if data can be converted to bytes
        assert record.store_value_data.encode('utf-8')

def test_attributes_primitive(primitive_records):
    for record in primitive_records:
        #Check if value is in expected data types list
        assert record.key == record.key_data
        assert record.value_type in ['int', 'str', 'float', 'bool']

        #Check if data can be converted to bytes
        assert record.store_value_data.encode('utf-8')

@pytest.fixture
def primitive_records():
    keys = []
    records = []
    for i in range(0, 20):
        key = random.randint(1, 1000)
        if key not in keys:
            keys.append(key)
            if random.uniform(0, 1) < 0.5:
                value = random.choice(string.ascii_letters)
            elif random.uniform(0, 1) < 0.5:
                value = random.uniform(0, 100)
            elif random.uniform(0, 1) < 0.5:
                value = random.randint(0, 10)
            else:
                value = False
            records.append(PrimitiveRecord(key, value))
    return records

@pytest.fixture
def json_records():
    keys = []
    records = []
    for i in range(0, 20):
        key = random.randint(1, 1000)
        if key not in keys:
            keys.append(key)
            if random.uniform(0, 1) < 0.5:
                value = random.choice(string.ascii_letters)
            elif random.uniform(0, 1) < 0.5:
                value = random.uniform(0, 100)
            elif random.uniform(0, 1) < 0.5:
                value = random.randint(0, 10)
            elif random.uniform(0, 1) < 0.5:
                value = False
            else:
                value = {1: 2}
            records.append(JsonRecord(key, value))
    return records
