__NAME__ = "Indranil Samanta"
__VERSION__ = "0.1"

try:
    import os
    import io
    import sys
    import json
    import pandas as pd
    from pymongo import MongoClient
    from bson.objectid import ObjectId
except Exception as e:
    print("Error: {} ".format(e))


class Singleton(type):
    """This is a Singleton Design Pattern"""
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """Creates only one instance of the class"""
        if cls not in cls._instance:
            cls._instance[cls] = super(Singleton, cls).__call__(*args, **kwargs)
            return cls._instance[cls]


class Settings(metaclass=Singleton):
    def __init__(self, host=None, maxPoolSize=50, port=27010):
        self.host = host
        self.maxPoolSize = maxPoolSize
        self.port = port


class Client(metaclass=Singleton):

    def __init__(self, _settings=None):
        self.settings = _settings
        self.mclient = MongoClient(host=self.settings.host,
                                   port=self.settings.port,
                                   maxPoolSize=self.settings.maxPoolSize)


class MongoInsert(object):

    def __init__(self, _client=None, dbName=None, collectionName=None):
        self.client = _client
        self.dbname = dbName
        self.collectionName = collectionName

    def insert_one(self, record={}):
        """
        insert record in mongodb
        :param record: json
        :return: Bool
        """
        try:
            self.client.mclient[self.dbname][self.collectionName].insert_one(record)
            return True
        except Exception as e:
            return False

    def insert_pandas_df(self, df=None):
        """
        :param df: Pandas DF
        :return: Bool
        """
        try:
            self.client.mclient[self.dbname][self.collectionName].insert_many(df.dict(), ordered=False)
            return True
        except Exception as e:
            return False


class MongoInformation(object):
    def __init__(self, _client=None):
        self.client = _client

    def getAllDatabase(self):
        """
        return all databases name in mongo db
        :return: List
        """
        return self.client.mclient.list_database_names()

    def getAllColletions(self, dbname=None):
        """
        List all the collection name in a mongo db
        :param dbname: Str
        :return: list
        """
        if dbname is None:
            return []
        else:
            return self.client.mclient[dbname].list_collection_names()


class Mongoose(object):
    """
    facade design pattern
    """

    def __init__(self, host=None, port=27010, maxPoolSize=50, dbname=None, collectionName=None):
        # creates an instance of settings class
        self._settings = Settings(host=host, port=port, maxPoolSize=maxPoolSize)
        # creates a single instance of client object
        self.client = Client(_settings=self._settings)
        self.dbname = dbname
        self.collectionName = collectionName

        self.insert = MongoInsert(_client=self.client, dbName=self.dbname, collectionName=self.collectionName)

    def insert_one_record(self, record={}):
        return self.insert.insert_one(record=record)


# def main():
#     url = "mongodb://localhost:27017"
#     # Creates an instance of the settings class
#     _settings = Settings(host=url)
#
#     # Creates a single instance of client object
#     _client = Client(_settings=_settings)
#
#     _record = {"name": "indranil"}
#     dbname = "testeverything"
#     collectionName = "person"
#
#     _helper = MongoInsert(_client=_client, dbName=dbname, collectionName=collectionName)
#     res = _helper.insert_one(record=_record)
#     print(res)

def main():
    _helper = Mongoose(host="mongodb://localhost:27017", dbname="testeverything", collectionName="person")

    _record = {"name": "Newton"}
    res = _helper.insert.insert_one(record=_record)
    print(res)


if __name__ == "__main__":
    main()
