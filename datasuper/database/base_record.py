from .database_exceptions import *

class BaseRecord:
    def __init__(self, repo, **kwargs):
        self.repo = repo
        self.db = repo.db
        self.dbTable = self.db.getTable(type(self))
        self.name = kwargs['name']
        try:
            self.primaryKey = kwargs['primary_key']
        except KeyError:
            self.primaryKey = None
        try:
            self.metadata = kwargs['metadata']
        except KeyError:
            self.metadata = {}

    def exists(self):
        return self.dbTable.exists(self.primaryKey)

    def nameExists(self):
        return self.dbTable.exists(self.name)

    def save(self, modify=False):
        if not self.validStatus():
            raise InvalidRecordStateError()

        pkExists = self.exists()
        nameExists = self.nameExists()
        if (pkExists or nameExists) and not modify:
            raise RecordExistsError()
        elif pkExists and modify:
            rec = self.dbTable.get(self.primaryKey).to_dict()
            rec = self._mergeDicts(rec)
            self.dbTable.update(self.primaryKey, rec)
            return self.dbTable.get(self.primaryKey)
        elif nameExists and modify:
            other = self.dbTable.get(self.name)
            rec = other._mergeDicts(self.to_dict())
            other.dbTable.update(other.primaryKey, rec)
            return other.dbTable.get(other.primaryKey)
        else:
            savedSelf = self.dbTable.insert(self.to_dict())
            self.primaryKey = savedSelf.primaryKey
            return savedSelf

            
    def _mergeDicts(self, rec):
        mydict = self.to_dict()
        for k, v in mydict.items():
            if k in rec and type(v) == dict and type(rec[k]) == dict:
                for subk, subv in v.items():
                    rec[k][subk] = subv
            else:
                rec[k] = v
        return rec

    def rename(self, newName):
        self.dbTable.rename( self.primaryKey, newName)
        self.name = newName


    def remove(self):
        raise NotImplementedError()
        
    def atomicDelete(self):
        self.dbTable.remove(self.primaryKey)

    def raw(self):
        return self.dbTable.getRaw( self.primaryKey)

    def validStatus(self):
        try:
            return self._validStatus()
        except Exception:
            return False

    def _validStatus(self):
        raise NotImplementedError()

    
    def to_dict(self):
        out = {
            'primary_key' : self.primaryKey,
            'name': self.name,
            'metadata': str(self.metadata)
            }
        return out
    
    
