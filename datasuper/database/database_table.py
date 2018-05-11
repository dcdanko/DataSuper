from tinydb import where
from random import choice as rchoice
import string
from .database_exceptions import *


class DatabaseTable:
    '''Stores and manipulates database records of a given type.'''

    def __init__(self, db, readOnly, typeStored, tinydbTbl):
        self.repo = db.repo
        self.db = db
        self.tbl = tinydbTbl
        self.typeStored = typeStored

    def _newPrimaryKey(self):
        '''Return a new random string for use as a primary key.'''
        N = 20
        chars = string.ascii_uppercase + string.digits
        pk = [rchoice(chars) for _ in range(N)]
        pk = ''.join(pk)
        return pk

    def rename(self, primaryKey, newName):
        '''Change name of `primaryKey` to `newName` then return the record.'''
        if self.repo.readOnly:
            raise RepoReadOnlyError()
        rawRec = self.getRaw(primaryKey)
        oldName = rawRec['name']
        rawRec['name'] = newName
        self.tbl.update(rawRec, eids=[rawRec.eid])

        del self.db.nameToPKTable[oldName]
        self.db.nameToPKTable[newName] = primaryKey
        self.db.pkToNameTable[primaryKey] = newName

        return self.get(primaryKey)

    def exists(self, primaryKey):
        '''Return True if `primaryKey` is in the table, else False.'''
        try:
            primaryKey = self.db.asPK(primaryKey)
        except KeyError:
            return False
        rec = self.tbl.get(where('primary_key') == primaryKey)
        return rec is not None

    def getRaw(self, primaryKey):
        '''Return the dict backing `primaryKey`'''
        primaryKey = self.db.asPK(primaryKey)
        return self.tbl.get(where('primary_key') == primaryKey)

    def get(self, primaryKey):
        '''Return the record corresponding to `primaryKey`'''
        primaryKey = self.db.asPK(primaryKey)
        rawRec = self.tbl.get(where('primary_key') == primaryKey)
        rec = self.typeStored(self.repo, **rawRec)
        return rec

    def getMany(self, primaryKeys):
        '''Return a list of records corresponding to `priamryKeys`.'''
        primaryKeys = self.db.asPKs(primaryKeys)
        recs = [self.get(pk) for pk in primaryKeys]
        return recs

    def size(self):
        '''Return the number of records in the table.'''
        return len(self.tbl.all())

    def getAll(self):
        '''Return a list of all records in the table.'''
        rawRecs = self.tbl.all()
        recs = [self.typeStored(self.repo, **rawRec) for rawRec in rawRecs]
        return recs

    def getAllRaw(self):
        '''Return a list of all raw records in the table.'''
        rawRecs = self.tbl.all()
        return rawRecs

    def getAllLazily(self):
        '''Return a generator of tuples of name and a record loader.'''
        rawRecs = self.tbl.all()
        recs = ((rawRec['name'], lambda: self.typeStored(self.repo, **rawRec))
                for rawRec in rawRecs)
        return recs

    def insert(self, newRecord):
        '''Add a record to the table. Return the new record.'''
        if self.repo.readOnly:
            raise RepoReadOnlyError()
        assert newRecord['primary_key'] is None  # idiot check myself
        newRecord['primary_key'] = self._newPrimaryKey()

        if not self.db.pkNotUsed(newRecord['primary_key']):
            raise RecordExistsError(newRecord)
        if not self.db.nameNotUsed(newRecord['name']):
            raise RecordExistsError(newRecord)

        self.tbl.insert(newRecord)
        self.db.nameToPKTable[newRecord['name']] = newRecord['primary_key']
        self.db.pkToNameTable[newRecord['primary_key']] = newRecord['name']

        return self.get(newRecord['primary_key'])

    def update(self, primaryKey, updatedRecord):
        '''Change `primaryKey` to updatedRecord. Return the new record.'''
        primaryKey = self.db.asPK(primaryKey)
        if self.repo.readOnly:
            raise RepoReadOnlyError
        rawRec = self.getRaw(primaryKey)
        assert rawRec['name'] == updatedRecord['name']
        self.tbl.update(updatedRecord, eids=[rawRec.eid])
        return self.get(primaryKey)

    def remove(self, primaryKey):
        '''Remove `primaryKey` from the table.'''
        primaryKey = self.db.asPK(primaryKey)
        if self.repo.readOnly:
            raise RepoReadOnlyError

        self.tbl.remove(where('primary_key') == primaryKey)

    def getInvalids(self):
        '''Return a list of primary keys for records that cannot be built.'''
        out = []
        for rawRec in self.tbl.all():
            try:
                rec = self.typeStored(self.repo, **rawRec)
                if not rec.validStatus():
                    out.append(rawRec['primary_key'])
            except InvalidRecordStateError:
                out.append(rawRec['primary_key'])
            except SchemaMismatchError:
                out.append(rawRec['primary_key'])

        return out

    def removeInvalids(self):
        '''Remove all records that cannot be built.'''
        if self.repo.readOnly:
            raise RepoReadOnlyError
        toRemove = []
        for rawRec in self.tbl.all():
            try:
                rec = self.typeStored(self.repo, **rawRec)
                if not rec.validStatus():
                    toRemove.append(rawRec['primary_key'])
            except InvalidRecordStateError:
                toRemove.append(rawRec['primary_key'])
            except SchemaMismatchError:
                toRemove.append(rawRec['primary_key'])

        for pk in toRemove:
            self.tbl.remove(where('primary_key') == pk)

    def checkStatus(self):
        '''Return a map of record names to valid status.'''
        out = {}
        for name, recfunc in self.getAllLazily():
            try:
                rec = recfunc()
            except InvalidRecordStateError:
                out[name] = False
                continue
            if not rec.validStatus():
                out[name] = False
            else:
                out[name] = True
        return out
