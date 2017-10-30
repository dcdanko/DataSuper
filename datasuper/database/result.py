from .base_record import *
from pyarchy import archy

class ResultRecord( BaseRecord):
    def __init__(self, repo, **kwargs):
        super(ResultRecord, self).__init__(repo, **kwargs)
        try:
            self._previousResults = self.db.asPKs(kwargs['previous_results'])
        except KeyError:
            self._previousResults = []
        try:
            self._provenance = kwargs['provenance']
        except KeyError: 
            self._provenance = []           
        self._resultType = self.repo.validateResultType( kwargs['result_type'])
        try:
            fileRecs = kwargs['file_records']
            try:
                fileRecs = { k: self.db.asPK(v) for k, v in fileRecs.items()}
            except AttributeError:
                fileRecs = [ self.db.asPK(el) for el in fileRecs]
        except KeyError:
            fileRecs = None
        # this will return a list of primary keys or a
        # map of identifiers -> primary keys (as a dict)
        self._fileRecords = self.instantiateResultSchema( fileRecs)

    def to_dict(self):
        out = super(ResultRecord, self).to_dict()
        out['previous_results'] = self._previousResults
        out['provenance'] = self._provenance
        out['file_records'] = self._fileRecords
        out['result_type'] = self._resultType
        return out

    def files(self):
        if type(self._fileRecords) == dict:
            out = {}
            for k, fr in self._fileRecords.items():
                out[k] = self.db.fileTable.get(fr)
            return [(k,v) for k,v in out.items()]
        else:
            return self.db.fileTable.getMany( self._fileRecords)
    
    def validStatus(self):
        fs = self.files()
        if type(fs) == dict:
            fs = fs.values()
        for fileRec in fs:
            if not fileRec.validStatus():
                return False

        # TODO: check that it matches schema
        return True

    def resultType(self):
        return self._resultType
    

    def instantiateResultSchema(self, fileRecs):
        schema = self.repo.getResultSchema(self.resultType())
        if type(schema) == list:
            if fileRecs is None:
                return [None for _ in schema]
            else:
                assert len(fileRecs) == len(schema)
                return fileRecs
        elif type(schema) == dict:
            if fileRecs is None:
                return {k:None for k in schema.keys()}
            else:
                for k,v in fileRecs.items():
                    assert k in schema
                return fileRecs
        else:
            assert type(fileRecs) == str
            return fileRecs


    
    def __str__(self):
        out = '{}\t{}'.format(self.name, self._resultType)
        return out

    def tree(self, raw=False):
        out = {'label': self.name, 'nodes':[]}
        try:
            for key, fr in self.files().items():
                out['nodes'].append( '{} {}'.format(key,fr.name))
        except AttributeError:
            for fr in self.files():
                out['nodes'].append( '{}'.format(fr.name))
        if raw:
            return out
        else:
            return archy(out)

