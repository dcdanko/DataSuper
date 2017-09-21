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
    
    def __init__(self, repo, readOnly, tinydbDB):
        self.repo = repo
        self.readOnly = readOnly
        self.tdb = tinydbDB
        self.pkToNameTable = {}
        self.nameToPKTable = {}        
        self.fileTable = DatabaseTable( self.repo,
                                        self.readOnly,
                                      FileRecord,
                                      self.tdb.table( Database.fileTblName))
        for rec in self.fileTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey
            
        self.resultTable = DatabaseTable( self.repo,
                                        self.readOnly,                                          
                                        ResultRecord,
                                        self.tdb.table( Database.resultTblName))
        for rec in self.resultTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey

        self.sampleTable = DatabaseTable( self.repo,
                                        self.readOnly,                                          
                                        SampleRecord,
                                        self.tdb.table( Database.sampleTblName))
        for rec in self.sampleTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey
        
        self.sampleGroupTable = DatabaseTable( self.repo,
                                        self.readOnly,                                               
                                             SampleGroupRecord,
                                             self.tdb.table( Database.sampleGroupTblName))
        for rec in self.sampleGroupTable.getAll():
            self.pkToNameTable[rec.primaryKey] = rec.name
            self.nameToPKTable[rec.name] = rec.primaryKey
            
        
    def pkNotUsed(self, primaryKey):
        # check that the primary key has not already been used
        return not (primaryKey in self.pkToNameTable)

    def nameNotUsed(self, name):
        # check that the primary key has not already been used
        return not (name in self.nameToPKTable)
    
    def asPKs(self, names):
        # convert a list of names into a list of pks
        pks = []
        for name in names:
            try:
                pk = self.nameToPKTable[name]
            except KeyError as ke:
                if pk in self.pkToNameTable:
                    pk = name
                else:
                    raise ke
            pks.append(pk)
        return pks


    def asNames(self, pks):
        # convert a list of pks into a list of names
        names = []
        for pk in pks:
            try:
                name = self.pkToNameTable[pk]
            except KeyError as ke:
                if pk in self.nameToPKTable:
                    name = pk
                else:
                    raise ke
            names.append(name)
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
        dbPath = os.path.join( repo.abspath, Database.dbName)
        tinydbDB = TinyDB(dbPath, storage=CachingMiddleware(JSONStorage))
        return Database(repo, readOnly, tinydbDB)
