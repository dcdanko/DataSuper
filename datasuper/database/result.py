from .base_record import *

class ResultRecord( BaseRecord):
    def __init__(self, repo, **kwargs):
        super(ResultRecord, self).__init__(repo, **kwargs)
        try:
            self._previousResults = kwargs['previous_results']
        except KeyError:
            self._previousResults = []
        try:
            self._provenance = kwargs['provenance']
        except KeyError: 
            self._provenance = []           
        self.resultType = self.repo.validateResultType( kwargs['result_type'])
        self._sample = kwargs['sample'] # primary key not object
        try:
            fileRecs = kwargs['file_records']
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
        out['result_type'] = self.resultType
        return out

    def files(self):
        if type(self._fileRecords) == dict:
            out = {}
            for k, fr in self._fileRecords:
                out[k] = self.db.fileTable.get(fr)
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

    def sample(self):
        return self.db.sampleTable.get( self._sample)

    def instantiateResultSchema(self, fileRecs):
        schema = self.repo.getResultSchema[self.resultType]
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
        out = '{}\t{}'.format(self.name, self.resultType)
        return out


