



class DatabaseTable:

    def __init__(self, repo, typeStored, tinydbTbl):
        self.repo = repo
        self.tbl = tinydbTbl
        self.typeStored = typeStored

    def _newPrimaryKey(self):
        pass
        
    def exists(self, primaryKey):
        return self.tbl.get(where('primary_key') == primaryKey) != None

    def getRaw(self, priamryKey):
        return self.tbl.get(where('primary_key') == primaryKey) 
    
    def get(self, primaryKey):
        rawRec = self.tbl.get(where('primary_key') == primaryKey) 
        rec = self.typeStored( self.repo.db, **rawRec)
        return rec
        
    def getMany(self, primaryKeys):
        rawRecs = self.tbl.get(where('primary_key') in primaryKeys) 
        recs = [self.typeStored( self.repo.db, **rawRec) for rawRec in rawRecs]
        return recs
    
    def getAll(self):
        rawRecs = self.tbl.all()
        recs = [self.typeStored( self.repo.db, **rawRec) for rawRec in rawRecs]
        return recs
    
    def insert(self, newRecord):
        assert newRecord['primary_key'] is None # idiot check myself
        newRecord['primary_key'] = self._newPrimaryKey()
        self.tbl.insert( newRecord)
        return self.get(newRecord['primary_key'])
        
    def update(self, primaryKey, updatedRecord):
        rawRec = self.getRaw( primaryKey)
        self.tbl.update( updatedRecord, eids=[rawRec.eid])
        return self.get(primaryKey)
    
    def remove(self, primaryKey):
        rawRecs = self.getRaw( primaryKey)
        self.tbl.remove(eids=[rawRec.eid])


    





