## Description
This library will support creating a structured represantions of data
where every key maps to a primitive value 

Library supports 
- Primitive records with values of types such as string, int, float, bool
- Json records that supports primitive one and can handle in addition dictionary objects 
- Storage formats bytes, json, xml
- Destinations: local, ftp, s3 

TODO
- Enhance the library to save Numpy Arrays
- Enhance the library to save Pandas DataFrame
- Enhance the library to save Pillow Images
- Enable saving data to Azure Blob Storage
- Enable logs

## Prerequisites
- Install python 3.8
- Install poetry
    >pip install poetry 

## Using virtualenv (optional)
- Install virtual environmnet
    > py -3.8* -m pip install virtualenv
- Create virtual environmnet
    > py -3.8* -m virtualenv venv_data
- Activate virtualenv by running script activate
    >Script/activate

## Install package using **Poetry**
- Go to the main directory of the project DataStore
    >Cd DataStore
- Install package (this will install also dependencies inside pyproject.toml)
    >Poetry install 
- After installation is completed you will be able to import the packakge as below
    >from datastore import *

## Test
- Run tests using poetry
  > poetry run pytest

#### Example1 - Create primitive records and add them to datastore
Python code
```python 
from datastore.record import PrimitiveRecord, DictionaryRecord
from datastore.data import Data

record1=PrimitiveRecord(key=1, value='sadfs')
record2=PrimitiveRecord(key=2, value=1000.0)
record3=PrimitiveRecord(key=3, value=5)
record4=PrimitiveRecord(key=4, value=True)
ds_data=Data(RecordInstance=PrimitiveRecord)
ds_data.insert_record(record1)
ds_data.insert_record(record2)
ds_data.insert_record(record3)
ds_data.insert_record(record4)
ds_data.print(5)
```
Output
```
---Table format----------
1 | sadfs
2 | 1000.0
3 | 5
4 | True
``` 
#### Example2 Query, update and delete record
```python 
#Query
record=ds_data.query_record_via_key(key=2)

#Update
record4.update_record(key=4, value=False)
ds_data.update_record(record4)

#Delete
ds_data.delete_record(key=3)
```
#### Example3 Save data as bytes format on local drive
Python code
```python 
from datastore import destination
from pathlib import Path

local=destination.Local(dirpath=Path.cwd() / "input_ouput_data", filename="data_as_byte", seperator=";")
local.dump_data(ds_data, mode="wb", format='bytes')
```
File content
```
1;sadfs;str
2;1000.0;float
3;5;int
4;True;bool
``` 

#### Example4 Save data as json format on local drive
Python code
```python 
from datastore import destination
from pathlib import Path

local=destination.Local(dirpath=Path.cwd() / "input_ouput_data", filename="data_as_dict.json")
local.dump_data(ds_data, mode="w", format='json')
```
File content
```json
{"1": "sadfs", "2": "1000.0", "3": "5", "4": "True"}
``` 
#### Example5 Save data as bytes format on FTP server
Python code
```python 
from datastore import destination
from pathlib import Path

ftp=destination.Ftp(Path('test') / "input_ouput_data",
                    filename="data_as_byte",
                    server='',
                    username='',
                    password='***')
local.dump_data(ds_data, format='bytes')
```

#### Example6 Save dictionary objects (todo numpy, pandas objects) and be able to load it as dictionary 
```python 
record1 = PrimitiveRecord(key=1, value='sadfs')
record2 = PrimitiveRecord(key=2, value=1000.0)
record3 = DictionaryRecord(key=5, value={1: 2})
new_data = Data(RecordInstance=PrimitiveRecord)
new_data.insert_record(record1)
new_data.insert_record(record2)
new_data.insert_record(record3)
```
This is done by develiping minimal code by using inheritance. Clas DictionaryRecord extends PrimitiveRecord

```python
def make_for_storage_dictionary(self):
    """
    Prepare data to be stored
    :return:
    """
    try:
        if isinstance(self.value, dict):
            self.store_value=json.dumps(self.value)
    except:
        pass
``` 

#### Example7 We can expand the library to support saving different formats by using inheritance and overwriting methods
> def dump_data(**kwargs)

> def load_data(**kwargs)

> def create_connection(**kwargs)

> def close_connection(**kwargs)
