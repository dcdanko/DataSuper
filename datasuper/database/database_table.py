from tinydb import where, Query
from random import choice as rchoice
import string
import sys
from .database_exceptions import *

class RepoReadOnlyError( Exception):
    pass

class DatabaseTable:

    def __init__(self, db, readOnly, typeStored, tinydbTbl):
        self.repo = db.repo
        self.db = db
        self.tbl = tinydbTbl
        self.typeStored = typeStored

    def _newPrimaryKey(self):
        N = 20
        chars = string.ascii_uppercase + string.digits
        pk = [rchoice(chars) for _ in range(N)]
        pk = ''.join(pk)
        return pk

    def rename(self, primaryKey, newName):
        if self.readOnly:
            raise RepoReadOnlyError()
        rawRec = self.getRaw( primaryKey)
        oldName = rawRec.name
        rawRec.name = newName
        self.tbl.update( rawRec, eids=[rawRec.eid])

        del self.db.nameToPKTable[oldName]
        self.db.nameToPKTable[newName] = primaryKey
        self.db.pkToNameTable[priamryKey] = newName
        
        return self.get(primaryKey)

    
    def exists(self, primaryKey):
        try:
            primaryKey = self.db.asPK( primaryKey)
        except KeyError:
            return False
        return self.tbl.get(where('primary_key') == primaryKey) != None

    def getRaw(self, primaryKey):
        primaryKey = self.db.asPK( primaryKey)        
        return self.tbl.get(where('primary_key') == primaryKey)
    
    def get(self, primaryKey):
        primaryKey = self.db.asPK( primaryKey)        
        rawRec = self.tbl.get(where('primary_key') == primaryKey)
        rec = self.typeStored(self.repo, **rawRec)
        return rec
        
    def getMany(self, primaryKeys):
        primaryKeys = self.db.asPKs(primaryKeys)
        '''
        print(primaryKeys)
        Q = Query()
        rawRecs = self.tbl.search(where('primary_key').all(primaryKeys))
        print(rawRecs)
        if rawRecs is None:
            return []
        recs = [self.typeStored( self.repo, **rawRec) for rawRec in rawRecs]
        '''
        recs = [self.get(pk) for pk in primaryKeys]
        return recs

    def size(self):
        return len(self.tbl.all())
    
    def getAll(self):
        rawRecs = self.tbl.all()
        recs = [self.typeStored(self.repo, **rawRec) for rawRec in rawRecs]
        return recs

    def getAllLazily(self):
        rawRecs = self.tbl.all()
        recs = ((rawRec['name'], lambda: self.typeStored(self.repo, **rawRec))
                for rawRec in rawRecs)
        return recs
    
    def insert(self, newRecord):
        if self.repo.readOnly:
            raise RepoReadOnlyError()
        assert newRecord['primary_key'] is None # idiot check myself
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
        primaryKey = self.db.asPK(primaryKey)
        if self.repo.readOnly:
            raise RepoReadOnlyError
        rawRec = self.getRaw( primaryKey)
        assert rawRec['name'] == updatedRecord['name']
        self.tbl.update( updatedRecord, eids=[rawRec.eid])
        return self.get(primaryKey)
    
    def remove(self, primaryKey):
        primaryKey = self.db.asPK(primaryKey)
        if self.repo.readOnly:
            raise RepoReadOnlyError
        
        self.tbl.remove(where('primary_key') == primaryKey)

    def getInvalids(self):
        out = []
        for rawRec in self.tbl.all():
            try:
                self.typeStored(self.repo, **rawRec)
            except InvalidRecordStateError:
                out.append(rawRec['primary_key'])
        return out

    def removeInvalids(self):
        if self.repo.readOnly:
            raise RepoReadOnlyError
        toRemove = []
        for rawRec in self.tbl.all():
            try:
                self.typeStored(self.repo, **rawRec)
            except InvalidRecordStateError:
                toRemove.append(rawRec['primary_key'])
        for pk in toRemove:
            self.tbl.remove(where('primary_key') == pk)



    





