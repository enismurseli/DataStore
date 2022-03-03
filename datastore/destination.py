#destionation enables to save and load data from loacl, ftp, cloud platforms,...
import json
from abc import ABC, abstractmethod
from pathlib import Path
from record import AbstractRecord
import ftplib
import data
import boto3

class AbstractDestination(ABC):
    def __init__(self):
        self.save_dirpath= None
        self.save_filename = None
        self.save_filepath=None
        self.connection=None

    @property
    def supports_byte(self) -> bool:
        pass

    @property
    def supports_json(self) -> bool:
        pass

    @property
    def supports_xml(self) -> bool:
        pass

    @property
    def is_local(self) -> bool:
        pass

    @property
    def is_ftp(self) -> bool:
        pass

    @property
    def is_cloud(self) -> bool:
        pass

    @property
    def is_connected(self) -> bool:
        pass

    @abstractmethod
    def create_connection(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def close_connection(self):
        raise NotImplementedError

    @abstractmethod
    def dump_data(self, **kwargs) -> bool:
        raise NotImplementedError

    @abstractmethod
    def load_data(self) -> data.Data:
        raise NotImplementedError

class Local(AbstractDestination):
    def __init__(self, dirpath: Path, filename: str, seperator=';'):
        super().__init__()
        if isinstance(dirpath, Path) and isinstance(filename, str):
            self.save_filename=filename
            self.save_dirpath=dirpath
            self.save_filepath=dirpath / filename
            self.seperator = seperator

    @property
    def supports_byte(self) -> bool:
        return True

    @property
    def supports_json(self) -> bool:
        return True

    @property
    def supports_xml(self) -> bool:
        return False

    @property
    def is_local(self) -> bool:
        return True

    def create_connection(self, mode : str):
        try:
            self.connection=open(self.save_filepath, mode=mode)
            return True
        except:
            return False

    @property
    def close_connection(self):
        try:
            self.connection.close()
        except:
            raise ValueError("No connection found!")
        finally:
            return True

    def dump_data(self, new_data: data.Data, mode: str , format: ['json', 'bytes', 'xml']) -> bool:
        if create_connection := self.create_connection(mode=mode):
            if format=='bytes':
                for record in new_data.data:
                    row=record.store_key +self.seperator +record.store_value +self.seperator +record.value_type + '\n'
                    self.connection.write(row.encode('utf-8'))
            elif format=='json':
                jdata=new_data.data_as_json()
                json.dump(jdata,self.connection)
            else:
                raise ValueError("This format of saving data is not supported!")
            return self.close_connection
        else:
            raise ValueError(f"Error could nop open filepath { self.save_filepath }")


    def load_data(self, RecordInstance : AbstractRecord, format: ['json', 'bytes', 'xml']) -> data.Data:
        ds_data=data.Data(RecordInstance)
        if create_connection := self.create_connection(mode='r'):
            line = self.connection.readline()
            if format=='bytes':
                split_line=line.split(self.seperator)
                record = RecordInstance(key=split_line[0], value=split_line[1])
                ds_data.insert_record(new_record=record)
                while line:
                    line = self.connection.readline()
                    if len(line) > 1:
                        split_line=line.split(self.seperator)
                        record=RecordInstance(key=split_line[0], value=split_line[1])
                        ds_data.insert_record(new_record=record)
                    else:
                        continue
            elif format=='json':
                jdata=json.load(line)
                for key in jdata.keys():
                    record = RecordInstance(key=key, value=jdata[key])
                    ds_data.insert_record(new_record=record)
            else:
                raise ValueError("Format of loading data is not supported")
        return ds_data


class Ftp(Local):
    def __init__(self, dirpath: Path, filename: str, server: str, username : str, password : str, seperator=';'):
        super().__init__(dirpath=dirpath, filename=filename, seperator=seperator)
        self.server = server
        self.username = username
        self.password = password

    @property
    def is_ftp(self) -> bool:
        return True

    def create_connection(self):
        try:
            self.connection=ftplib.FTP(self.server,self.username,self.password)
            return True
        except:
            return False

    @property
    def close_connection(self):
        try:
            self.connection.quit()
        except:
            raise ValueError("No connection found!")
        finally:
            return True

    def dump_data(self, new_data: data.Data, format: ['json', 'bytes', 'xml']) -> bool:
        if create_connection:= self.create_connection():
            if format=='bytes':
                for record in new_data.data:
                    new_line = record.key_data + self.seperator + record.store_value_data + self.seperator + record.value_type + '\n'
                    self.connection.storbinary('STOR ' + str(self.save_filepath), new_line.encode('ascii'))
                return self.close_connection
            else:
                raise ValueError("Format is not supported!")
        else:
            raise ValueError(f"Error - could not connect to the server via FTP")

    def load_data(self, RecordInstance : AbstractRecord, format: ['json', 'bytes', 'xml']) -> data.Data:
        ds_data=data.Data()
        dt=[]
        if format=='bytes':
            def handle_binary(more_data):
                dt.append(more_data)
            if create_connection:= self.create_connection():
                self.connection.retrbinary('RETR ' + str(self.save_filepath), callback=handle_binary)
            for row in dt:
                row=row.split(self.seperator)
                record=RecordInstance(key=row[0], value=row[1])
                ds_data.insert_record(new_record=record)
        else:
            raise ValueError("Format is not supported!")
        return ds_data


class S3(Local):
    def __init__(self, dirpath: Path, filename: str,  region_name: str, aws_access_key_id: str, aws_secret_access_key: str, bucket_name: str, seperator=''):
        super().__init__(dirpath=dirpath, filename=filename, seperator=seperator)
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name=bucket_name
        pass

    @property
    def is_cloud(self) -> bool:
        return True

    def create_connection(self, mode:str):
        try:
            self.connection= boto3.resource(
                            's3',
                            region_name=self.region_name,
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key
                        )
            return True
        except:
            return False

    @property
    def close_connection(self):
        try:
            self.connection.quit()
        except:
            raise ValueError("No connection found!")
        finally:
            return True

    def dump_data(self, new_data: data.Data) -> bool:
        #Todo support differnt formats
        if create_connection:= self.create_connection():
            for record in new_data.data:
                new_line = record.key_data + self.seperator + record.store_value_data + self.seperator + record.value_type + '\n'
                self.connection.Object(self.bucket_name, str(self.save_filepath)).put(Body=new_line)
            return self.close_connection
        else:
            raise ValueError(f"Error - could not create connection!")

    def load_data(self, RecordInstance : AbstractRecord) -> data.Data:
        ds_data=data.Data()
        if create_connection:= self.create_connection():
            txt_file = self.connection.Object(self.bucket_name, str(self.save_filepath)).get()['Body'].read().decode('utf-8').splitlines()
        for row in txt_file:
            row=row.split(self.seperator)
            record=RecordInstance(key=row[0], value=row[1])
            ds_data.insert_record(new_record=record)
        return ds_data