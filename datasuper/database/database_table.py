from tinydb import where, Query
from random import choice as rchoice
import string
import sys

class RepoReadOnlyError( Exception):
    pass

class DatabaseTable:

    def __init__(self, repo, readOnly, typeStored, tinydbTbl):
        self.repo = repo
        self.readOnly = readOnly
        self.tbl = tinydbTbl
        self.typeStored = typeStored

    def _newPrimaryKey(self):
        N = 20
        chars = string.ascii_uppercase + string.digits
        pk = [rchoice(chars) for _ in range(N)]
        pk = ''.join(pk)
        return pk
    
    def exists(self, primaryKey):
        return self.tbl.get(where('primary_key') == primaryKey) != None

    def getRaw(self, priamryKey):
        return self.tbl.get(where('primary_key') == primaryKey) 
    
    def get(self, primaryKey):
        rawRec = self.tbl.get(where('primary_key') == primaryKey)
        rec = self.typeStored( self.repo, **rawRec)
        return rec
        
    def getMany(self, primaryKeys):
        Q = Query()
        rawRecs = self.tbl.get(Q.primary_key.any(primaryKeys))
        if rawRecs is None:
            return []
        recs = [self.typeStored( self.repo, **rawRec) for rawRec in rawRecs]
        return recs
    
    def getAll(self):
        rawRecs = self.tbl.all()
        recs = [self.typeStored( self.repo, **rawRec) for rawRec in rawRecs]
        return recs
    
    def insert(self, newRecord):
        if self.readOnly:
            raise RepoReadOnlyError
        assert newRecord['primary_key'] is None # idiot check myself
        newRecord['primary_key'] = self._newPrimaryKey()
        self.tbl.insert( newRecord)
        return self.get(newRecord['primary_key'])
        
    def update(self, primaryKey, updatedRecord):
        if self.readOnly:
            raise RepoReadOnlyError
        rawRec = self.getRaw( primaryKey)
        self.tbl.update( updatedRecord, eids=[rawRec.eid])
        return self.get(primaryKey)
    
    def remove(self, primaryKey):
        if self.readOnly:
            raise RepoReadOnlyError            
        rawRecs = self.getRaw( primaryKey)
        self.tbl.remove(eids=[rawRec.eid])


    





