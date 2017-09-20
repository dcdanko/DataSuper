
class BaseRecord:
    def __init__(self, db, **kwargs):
        self.db = db
        self.dbTable = db.getTable( type(self))
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

    def save(self, modify=False):
        if not self.validStatus():
            raise InvalidRecordStateError()

        alreadyExists = self.exists()
        if alreadyExists and not modify:
            raise RecordExistsError()
        elif alreadyExists and modify:
            rec = self.dbTable.get(self.primaryKey).to_dict()
            mydict = self.to_dict()
            for k,v in mydict.items():
                if k in rec and type(v) == dict and type(rec[k]) == dict:
                    for subk, subv in v.items():
                        rec[k][subk] = subv
                else:
                    rec[k] = v
            self.dbTable.update(rec, eids=[rec.eid])
            return self.dbTable.get(self.primaryKey)
        else:
            self.dbTable.insert(self.to_dict())
            return self.dbTable.get(self.primaryKey)

    
    def delete(self):
        self.dbTable.remove(self.primaryKey)

    def raw(self):
        return self.dbTable.getRaw( self.primaryKey)

    def validStatus(self):
        raise NotImplementedError()

    def to_dict(self):
        out = {
            'primary_key' : self.primaryKey,
            'name': self.name,
            'metadata': str(self.metadata)
            }
        return out
    

    
