from abc import ABC, abstractmethod
import json

class AbstractRecord(ABC):
    def __init__(self):
        self.key= None
        self.value = None
        self.store_value = None
        self.store_key = None

    @property
    def value_data(self) -> object:
        pass

    @property
    def key_data(self) -> object:
        pass

    @property
    def store_value_data(self) -> object:
        pass

    @property
    def is_valid(self) -> bool:
        pass

    @property
    def has_key(self) -> bool:
        pass

    @property
    def has_value(self) -> bool:
        pass

    @property
    def record_shape(self) -> tuple:
        return ()

    @property
    def value_type(self) -> str:
        pass

    @abstractmethod
    def update_record(self, key , value) -> bool:
        raise NotImplementedError

    @abstractmethod
    def make_for_storage(self):
        raise NotImplementedError

class PrimitiveRecord(AbstractRecord):
    def __init__(self, key: object, value: object):
        super().__init__()
        if (isinstance(key, str) or isinstance(key, int)):
            self.key=key
            self.value=value
            self.make_for_storage()
        else:
            raise ValueError("Error key is not supported format!")

    @property
    def value_data(self) -> object:
        return self.value

    @property
    def key_data(self) -> object:
        return self.key

    @property
    def store_value_data(self) -> object:
        return self.store_value

    @property
    def is_valid(self) -> bool:
        if isinstance(self.value, str) or isinstance(self.value, int) or isinstance(self.value, float) or isinstance(self.value, bool):
            return True
        return False

    @property
    def has_key(self):
        return True

    @property
    def has_value(self):
        return True

    @property
    def record_shape(self) -> tuple:
        return (self.key, self.value)

    @property
    def value_type(self) -> str:
        if isinstance(self.value, str):
            return 'str'
        elif isinstance(self.value, float):
            return 'float'
        elif isinstance(self.value, bool):
            return 'bool'
        elif isinstance(self.value, int):
            return 'int'
        else:
            return 'object'

    def update_record(self,  key : object , value: object) -> bool:
        if self.key!=None:
            self.key=key
        if self.key != None:
            self.value=value

    def make_for_storage(self):
        try:
            if isinstance(self.value, str):
                self.store_value=self.value
            elif isinstance(self.value, float):
                self.store_value=str(self.value)
            elif isinstance(self.value, bool):
                self.store_value=str(self.value)
            elif isinstance(self.value, int):
                self.store_value=str(self.value)
            else:
                raise  ValueError("Value format is not supported!")

            if isinstance(self.key, str):
                self.store_key=self.key
            elif isinstance(self.key, int):
                self.store_key=str(self.key)
            else:
                raise ValueError("Key format is not supported!")
        except:
            raise  ValueError("Error - value format is not primitive!")


class JsonRecord(PrimitiveRecord):
    def __init__(self, key: object, value: object):
        super().__init__(key=key, value=value)

    @property
    def value_type(self) -> str:
        return 'json'

    @property
    def is_valid(self) -> bool:
        if isinstance(self.value, str) or \
                isinstance(self.value, int) or \
                isinstance(self.value, float) or \
                isinstance(self.value, bool) or \
                isinstance(self.value, dict):
            return True
        return False

    @property
    def value_type(self) -> str:
        if isinstance(self.value, str):
            return 'str'
        elif isinstance(self.value, float):
            return 'float'
        elif isinstance(self.value, bool):
            return 'bool'
        elif isinstance(self.value, int):
            return 'int'
        elif isinstance(self.value, dict):
            return 'dict'
        else:
            return 'object'

    def make_for_storage(self):
        try:
            if isinstance(self.value, str):
                self.store_value=self.value
            elif isinstance(self.value, float):
                self.store_value=str(self.value)
            elif isinstance(self.value, bool):
                self.store_value=str(self.value)
            elif isinstance(self.value, int):
                self.store_value=str(self.value)
            elif isinstance(self.value, dict):
                self.store_value=json.dumps(self.value)
            else:
                raise  ValueError("Value format is not supported!")

            if isinstance(self.key, str):
                self.store_key=self.key
            elif isinstance(self.key, int):
                self.store_key=str(self.key)
            else:
                raise ValueError("Key format is not supported!")
        except:
            raise  ValueError("Error - value format is not dict!")










