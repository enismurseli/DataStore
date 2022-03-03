import random, string
from datastore.record import PrimitiveRecord, JsonRecord
from datastore import destination
from datastore import data
from pathlib import Path
import pytest


def test_datastore(basic_data):
    assert basic_data.check_if_exists(key=3) == True

    # Save data as json locally
    local_json = destination.Local(dirpath=Path.cwd() / "input_output_data", filename="as_json.json")
    local_json.dump_data(basic_data, mode="w", format='json')
    assert True==(Path.cwd() / "input_output_data" / "as_json.json").is_file()

    # Load data json file
    ds_data=local_json.load_data(RecordInstance=PrimitiveRecord, format='json')
    assert 20==ds_data.get_size()

    # Save data as bytes locally
    local_string = destination.Local(dirpath=Path.cwd() / "input_output_data", filename="as_string.txt")
    local_string.dump_data(basic_data, mode="wb", format='bytes')
    assert True==(Path.cwd() / "input_output_data" / "as_string.txt").is_file()

    # Load data from file
    ds_data=local_string.load_data(RecordInstance=PrimitiveRecord, format='bytes')
    assert 20==ds_data.get_size()

    # Test query
    qrecord = ds_data.query_record_via_key(key='5')
    assert qrecord.key == '5'

    # Test update
    qrecord.update_record(key='5', value='100.0')
    ds_data.update_record(qrecord)
    qrecord = ds_data.query_record_via_key(key='5')
    assert qrecord.value == '100.0'

    # Test delete
    ds_data.delete_record(key='3')
    assert ds_data.check_if_exists(key='3') == False

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
