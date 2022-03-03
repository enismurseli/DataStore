from record import PrimitiveRecord, DictionaryRecord, AbstractRecord
from data import Data
from pathlib import Path
import destination

# Example 1
record1 = PrimitiveRecord(key=1, value='sadfs')
record2 = PrimitiveRecord(key=2, value=1000.0)
record3 = PrimitiveRecord(key=3, value=5)
record4 = PrimitiveRecord(key=4, value=True)
ds_data = Data(RecordInstance=PrimitiveRecord)
ds_data.insert_record(record1)
ds_data.insert_record(record2)
ds_data.insert_record(record3)
ds_data.insert_record(record4)
ds_data.print(5)

local = destination.Local(dirpath=Path.cwd() / "input_output_data", filename="data_as_dict.json")
local.dump_data(ds_data, mode="w", format='json')

# ftp=destination.Ftp(dirpath=Path.cwd() / "input_output_data",
#                     filename="data_as_byte",
#                     server='',
#                     username='',
#                     password='***')
# local.dump_data(ds_data, format='bytes')

print("Create 4 records and print main attributes of record1")
print(f"Value data:  {record1.value_data}")
print(f"Key data:  {record1.key_data}")
record1.make_for_storage()
print(f"Store value data:  {record1.store_value_data}")

print("-" * 15)
print("Update record4")
record4.update_record(key=4, value=False)

print("-" * 15)
print("Create a data object")

ds_data = Data(RecordInstance=PrimitiveRecord)
ds_data.insert_record(record1)
ds_data.insert_record(record2)
ds_data.insert_record(record3)
ds_data.insert_record(record4)
ds_data.print(5)
print("-" * 15)
print("Delete record with id=3")
ds_data.delete_record(key=3)
ds_data.print(5)

print("Create a record with json value")
record1 = PrimitiveRecord(key=1, value='sadfs')
record2 = PrimitiveRecord(key=2, value=1000.0)
record3 = DictionaryRecord(key=5, value={1: 2})
new_data = Data(RecordInstance=PrimitiveRecord)
new_data.insert_record(record1)
new_data.insert_record(record2)
new_data.insert_record(record3)

new_data.print(5)

print("Query record with id=2")
record = ds_data.query_record_via_key(key=2)
print(f"key {record.key}  value {record.value}")

current_dir = Path.cwd()
local = destination.Local(dirpath=Path.cwd() / "input_output_data", filename="test_primitive_data.txt", seperator=";")
local.dump_data(ds_data, mode="wb", format='bytes')
ldata = local.load_data(RecordInstance=PrimitiveRecord, format='bytes')
ldata.print(5)

local = destination.Local(dirpath=Path.cwd() / "input_output_data", filename="test_new_data.json", seperator=";")
local.dump_data(new_data, mode="w", format="json")
