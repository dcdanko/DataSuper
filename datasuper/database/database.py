from tinydb import TinyDB
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
import os
from .database_table import *
from .file_record import *
from .result import *
from .sample_group import *
from .sample import *


class Database:
    dbName = 'datasuper.tinydb.json'
    fileTblName = 'file_record_table'
    resultTblName = 'result_record_table'
    sampleTblName = 'sample_record_table'
    sampleGroupTblName = 'sample_group_record_tbl'

    def __init__(self, repo, readOnly, tinyDB):
        self.repo = repo
        self.readOnly = readOnly
        self.tdb = tinyDB
        self.pkToNameTable = None
        self.nameToPKTable = None
        self.fileTable = DatabaseTable(self,
                                       self.readOnly,
                                       FileRecord,
                                       self.tdb.table(Database.fileTblName))

        self.resultTable = DatabaseTable(self,
                                         self.readOnly,
                                         ResultRecord,
                                         self.tdb.table(Database.resultTblName))

        self.sampleTable = DatabaseTable(self,
                                         self.readOnly,
                                         SampleRecord,
                                         self.tdb.table(Database.sampleTblName))

        self.sampleGroupTable = DatabaseTable(self,
                                              self.readOnly,
                                              SampleGroupRecord,
                                              self.tdb.table( Database.sampleGroupTblName))

    def _buildPKNameTables(self):
        if self.pkToNameTable is not None:
            return
        self.pkToNameTable = {}
        self.nameToPKTable = {}

        for rec in self.fileTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey

        for rec in self.resultTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey

        for rec in self.sampleTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey

        for rec in self.sampleGroupTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey

    def pkNotUsed(self, primaryKey):
        # check that the primary key has not already been used
        self._buildPKNameTables()
        return not (primaryKey in self.pkToNameTable)

    def nameNotUsed(self, name):
        # check that the primary key has not already been used
        self._buildPKNameTables()
        return not (name in self.nameToPKTable)

    def asPK(self, name):
        self._buildPKNameTables()
        try:
            pk = self.nameToPKTable[name]
        except KeyError as ke:
            if name in self.pkToNameTable:
                pk = name
            else:
                try:
                    pk = name.primaryKey
                except AttributeError:
                    raise ke
        return pk

    def asPKs(self, names):
        # convert a list of names into a set of pks
        self._buildPKNameTables()
        pks = set()
        for name in names:
            pks.add(self.asPK(name))
        return pks

    def asName(self, pks):
        self._buildPKNameTables()
        try:
            name = self.pkToNameTable[pk]
        except KeyError as ke:
            if pk in self.nameToPKTable:
                name = pk
            else:
                raise ke
        return name

    def asNames(self, pks):
        # convert a list of pks into a list of names
        self._buildPKNameTables()
        names = []
        for pk in pks:
            names.append(self.asName(name))
        return names

    def getTable(self, recType):
        if recType == FileRecord:
            return self.fileTable
        elif recType == ResultRecord:
            return self.resultTable
        elif recType == SampleRecord:
            return self.sampleTable
        elif recType == SampleGroupRecord:
            return self.sampleGroupTable

    def close(self):
        self.tdb.close()

    @staticmethod
    def loadDatabase(repo, path, readOnly):
        dbPath = os.path.join(repo.abspath, Database.dbName)
        tinydbDB = TinyDB(dbPath, storage=CachingMiddleware(JSONStorage))
        return Database(repo, readOnly, tinydbDB)
