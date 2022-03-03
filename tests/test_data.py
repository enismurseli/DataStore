import random, string
from datastore.record import PrimitiveRecord, JsonRecord
from datastore import destination
from datastore import data
import pytest


def test_data(basic_data):
    #Test if records exists
    assert basic_data.check_if_exists(key=3) == True
    assert basic_data.check_if_exists(key=50) == False

    # Test query
    qrecord = basic_data.query_record_via_key(key=5)
    assert qrecord.key == 5

    # Test update
    qrecord.update_record(key=5, value=100.0)
    basic_data.update_record(qrecord)
    qrecord = basic_data.query_record_via_key(key=5)
    assert qrecord.value == 100.0

    # Test delete
    basic_data.delete_record(key=3)
    assert basic_data.check_if_exists(key=3) == False

    #Test inserting new batch
    basic_data.insert_batch(basic_data)

@pytest.fixture
def basic_data():
    keys = []
    records = []
    ds_data = data.Data(RecordInstance=PrimitiveRecord)
    for i in range(0, 20):
        key = i
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
            ds_data.insert_record(PrimitiveRecord(key, value))
    return ds_data

