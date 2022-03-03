# Data is an object that defines a record where keys are primitve type such as string or integer
from .record import AbstractRecord


class Data:

    def __init__(self, RecordInstance: AbstractRecord):
        self.data = []
        self.keys = []
        self.RecordInstance = RecordInstance
        pass

    def insert_record(self, new_record) -> bool:
        """
        Add new record the the list
        :param new_record: record instance
        :return: bool
        """
        if isinstance(new_record, AbstractRecord):
            if new_record.is_valid and not self.check_if_exists(new_record.key_data):
                new_record.make_for_storage()
                self.data.append(new_record)
                self.keys.append(new_record.key_data)
            else:
                if not new_record.is_valid:
                    raise ValueError("Error - record is not valid!")
                else:
                    print('Record exists already in the datastore!')
                    return False
        else:
            raise ValueError("Error - wrong record instance!")
        return True

    def is_key_valid(self, key: object):
        """
        Check if the record is valid
        :param key: key or record instance
        :return: bool
        """
        return isinstance(key, str) or isinstance(key, int)

    def check_if_exists(self, key: object) -> bool:
        """
        Check if a record exists based on key
        :param key:
        :return: bool
        """
        if key in self.keys:
            return True
        return False

    def insert_batch(self, batch):
        """
        Insert a number of records
        :param batch: Data instance
        :return:
        """
        for new_record in batch.data:
            self.insert_record(new_record)
        pass

    def query_record_via_key(self, key: object) -> object:
        """
        Query a record based on given key
        :param key:
        :return: None or RecordInstance
        """
        if self.check_if_exists(key) and self.is_key_valid(key):
            return self.data[self.keys.index(key)]
        return None

    def query_record(self, rec: object) -> object:
        """
        Method to query a record
        :param rec: record to be queried
        :return: RecordInstance
        """
        if isinstance(rec, self.RecordInstance):
            key = rec.key_data
            if self.check_if_exists(key):
                return self.data[self.keys.index(key)]
        return None

    def update_record(self, new_record) -> bool:
        """
        Method to update new record
        :param new_record: updated record
        :return: bool
        """
        key = new_record.key_data
        new_value = new_record.value
        if self.is_key_valid(key):
            index = self.keys.index(key)
            self.data[index] = self.RecordInstance(key=key, value=new_value)
            return True
        return False

    def delete_record(self, key: object) -> bool:
        """
        Method to delete a record
        :param key:
        :return: bool
        """
        if self.is_key_valid(key):
            index = self.keys.index(key)
            self.data.pop(index)
            self.keys.pop(index)
            return True
        return False

    def data_as_dict(self):
        """
        Methoda to convert data as json
        :return: dict
        """
        jdata = {}
        for record in self.data:
            jdata[record.store_key] = record.store_value
        return jdata

    def print(self, rows: int):
        # Method to print the data in a structured format
        print('-' * 3 + 'Table format' + '-' * 10)
        for record in self.data:
            print('{0} | {1}'.format(record.store_key, record.store_value))
        pass

    def get_size(self) -> int:
        # Method to get the number of records
        return len(self.data)
