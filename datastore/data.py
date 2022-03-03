# Data is an object that defines a record where keys are primitve type such as string or integer
from record import AbstractRecord

class Data:

    def __init__(self, RecordInstance : AbstractRecord):
        self.data=[]
        self.keys=[]
        self.RecordInstance=RecordInstance
        pass

    def insert_record(self, new_record) -> bool:
        if isinstance(new_record, AbstractRecord):
            if new_record.is_valid and not self.check_if_exists(new_record.key_data):
                new_record.make_for_storage()
                self.data.append(new_record)
                self.keys.append(new_record.key_data)
            else:
                raise  ValueError("Error - record exists already in the datastore!")
        else:
            raise ValueError("Error - wrong record instance!")
        return True

    def is_key_valid(self, key:object):
        return isinstance(key, str) or isinstance(key, int)

    def check_if_exists(self, key: object) -> bool:
        if key in self.keys:
            return True
        return False

    def insert_batch(self, batch: list):
        for new_record in batch:
            self.insert_record(new_record)

    def query_record_via_key(self, key:object) -> object:
        if self.check_if_exists(key) and self.is_key_valid(key):
            return self.data[self.keys.index(key)]
        return None

    def query_record(self, rec:object) -> object:
        if isinstance(rec, self.RecordInstance):
            key=rec.key_data
            if self.check_if_exists(key):
                return [key , self.data[self.keys.index(key)]]
        return None

    def update_record(self, new_record) ->bool:
        key=new_record.key_data
        new_value=new_record.new_value
        if self.is_key_valid(key):
            index=self.keys.index(key)
            self.data[index]=self.RecordInstance(key=key, value=new_value)
            return True
        return False

    def delete_record(self, key: object) -> bool:
        if self.is_key_valid(key):
            index=self.keys.index(key)
            self.data.pop(index)
            self.keys.pop(index)
            return True
        return False

    def data_as_json(self):
        jdata={}
        for record in self.data:
            jdata[record.store_key]=record.store_value
        return jdata


    def print(self, rows: int):
        print('-'*3 + 'Table format' + '-'*10)
        for record in self.data:
            print('{0} | {1}'.format(record.store_key, record.store_value))
        pass















