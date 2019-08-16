from tinydb import TinyDB
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
import os
from .database_table import DatabaseTable
from .file_record import FileRecord
from .result import ResultRecord
from .sample_group import SampleGroupRecord
from .sample import SampleRecord


class Database:
    '''Represents a database that stores tables of records.'''
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
                                              self.tdb.table(Database.sampleGroupTblName))

    def _buildPKNameTables(self):
        if self.pkToNameTable is not None:
            return
        self.pkToNameTable = {}
        self.nameToPKTable = {}

        def handleTable(tbl):
            for rec in tbl.getAllRaw():
                rec_pk, rec_name = rec['primary_key'], rec['name']
                self.pkToNameTable[rec_pk] = rec_name
                self.nameToPKTable[rec_name] = rec_pk

        handleTable(self.fileTable)
        handleTable(self.resultTable)
        handleTable(self.sampleTable)
        handleTable(self.sampleGroupTable)

    def pkNotUsed(self, primaryKey):
        '''Return True if `primaryKey` has not been used, else False.'''
        self._buildPKNameTables()
        return not (primaryKey in self.pkToNameTable)

    def nameNotUsed(self, name):
        '''Return True if `name` has not been used, else False.'''
        self._buildPKNameTables()
        return not (name in self.nameToPKTable)

    def asPK(self, name):
        '''Return a primary key corresponding to name.
        If `name` is actually a primary key return `name`.
        '''
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
        '''Return a set of primary keys corresponding to `names`.'''
        self._buildPKNameTables()
        pks = set()
        for name in names:
            pks.add(self.asPK(name))
        return pks

    def asName(self, pk):
        '''Return a (human readable) name corresponding to `pk`.'''
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
        '''Return a list of names corresponding to `pks`.'''
        self._buildPKNameTables()
        names = []
        for pk in pks:
            names.append(self.asName(name))
        return names

    def getTable(self, recType):
        '''Return the table appropriate for `recType`.'''
        if recType == FileRecord:
            return self.fileTable
        elif recType == ResultRecord:
            return self.resultTable
        elif recType == SampleRecord:
            return self.sampleTable
        elif recType == SampleGroupRecord:
            return self.sampleGroupTable

    def flush(self):
        """Write data to disk."""
        self.tdb._storage.flush()

    def close(self):
        '''Close the database.'''
        self.tdb.close()

    def checkStatus(self):
        out = {
            'sample_groups': self.sampleGroupTable.checkStatus(),
            'samples': self.sampleTable.checkStatus(),
            'results': self.resultTable.checkStatus(),
            'file_records': self.fileTable.checkStatus()
        }
        return out

    @staticmethod
    def loadDatabase(repo, path, readOnly):
        '''Load the database from a repo directory.'''
        dbPath = os.path.join(repo.abspath, Database.dbName)
        storage = CachingMiddleware(JSONStorage)
        storage.WRITE_CACHE_SIZE = 100 * 1000
        tinydbDB = TinyDB(dbPath, storage=storage)
        return Database(repo, readOnly, tinydbDB)
