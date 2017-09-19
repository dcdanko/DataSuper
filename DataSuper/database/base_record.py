from tinydb import TinyDB, Query, where
import meta_ultra.config as config
from meta_ultra.data_type import DataType, DataTypeNotFoundError
from meta_ultra.sample_type import SampleType, SampleTypeNotFoundError
from meta_ultra.utils import *
from os.path import basename
import json

projectTbl = config.db_project_table
sampleTbl = config.db_sample_table
dataTbl = config.db_data_table
experimentTbl = config.db_experiment_table
resultTbl = config.db_result_table
confTbl = config.db_conf_table

################################################################################
#
# Classes
#
################################################################################

class RecordExistsError(Exception):
    pass

class NoSuchRecordError(Exception):
    pass

class InvalidRecordStateError(Exception):
    pass

class BaseRecord:
    tblName = None
    def __init__(self, **kwargs):
        try:
            self.primaryKey = kwargs['primary_key']
        except KeyError:
            self.primaryKey = self.generatePrimaryKey()
        self.name = kwargs['name']
        try:
            self.metadata = kwargs['metadata']
        except KeyError:
            self.metadata = {}
        
    def record(self, repo=None):
        rec = type(self).dbTbl(repo).get(where('name') == self.name)
        if not rec:
            raise NoSuchRecordError()
        return rec

    def saved(self, repo=None):
        return type(self).exists(self.name, repo=repo)

    def generatePrimaryKey(self):
        raise NotImplementedError()
    
    def save(self, modify=False, repo=None):
        if not self.validStatus():
            raise InvalidRecordStateError()
        
        if self.saved() and not modify:
            raise RecordExistsError()
        elif self.saved() and modify:
            rec = self.record()
            mydict = self.to_dict()
            for k,v in mydict.items():
                if k in rec and type(v) == dict and type(rec[k]) == dict:
                    for subk, subv in v.items():
                        rec[k][subk] = subv
                else:
                    rec[k] = v
            type(self).dbTbl(repo).update(rec, eids=[rec.eid])
            return type(self).get(self.name)
        else:
            type(self).dbTbl(repo).insert(self.to_dict())
            return type(self).get(self.name)

    
    def remove(self, repo=None):
        record = self.record()
        type(self).dbTbl(repo).remove(eids=[record.eid])

    def validStatus(self):
        raise NotImplementedError()

    def to_dict(self):
        out = {
            'primary_key' : self.primaryKey,
            'name': self.name,
            'metadata': str(self.metadata)
            }
        return out
    
    
    @classmethod
    def build(ctype, *args, **kwargs):
        return ctype(**kwargs)

        
    @classmethod
    def get(ctype, primaryKey, repo=None):
        rec = ctype.dbTbl(repo).get(where('primary_key') == primaryKey)
        if not rec:
            raise NoSuchRecordError()
        return ctype.build(**rec)

    @classmethod
    def exists(ctype, name, repo=None):
        return ctype.dbTbl(repo).get(where('name') == name) != None

    @classmethod
    def all(ctype, repo=None):
        recs = ctype.dbTbl(repo).all()
        recs = [ctype.build(**rec) for rec in recs]
        return recs

    @classmethod
    def where(ctype, repo=None, **kwargs):
        q = Query()
        for k,v in kwargs.items():
            q &= Query()[k] == v
        recs = ctype.dbTbl(repo).search(q)
        recs = [ctype.build(**rec) for rec in recs]
        return recs

    @classmethod
    def search(ctype, query, repo=None):
        recs = ctype.dbTbl(repo).search(query)
        recs = [ctype.build(**rec) for rec in recs]
        return recs

    @classmethod
    def dbTbl(ctype, repo):
        if repo != None:
            return repo.table( ctype.tableName())
        return Repo.getRepo(caching=False).table( ctype.tableName())

    
