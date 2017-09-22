from .base_record import *
from pyarchy import archy
from .result import *

class SampleRecord( BaseRecord):
    def __init__(self, repo, **kwargs):
        super(SampleRecord, self).__init__(repo, **kwargs)
        try:
            self._results = self.db.asPKs(kwargs['results']) # n.b. these are keys not objects
        except KeyError:
            self._results = []
        self.sampleType = self.repo.validateSampleType( kwargs['sample_type'])
        
    def to_dict(self):
        out = super(SampleRecord,  self).to_dict()
        out['results'] = self._results
        out['sample_type'] = str(self.sampleType)
        return out

    def validStatus(self):
        for res in self.results():
            if type(res) != ResultRecord:
                return False
            if not res.validStatus():
                return False
        return True

    def addResult(self, result):
        if issubclass( type(result), BaseRecord):
            result = result.primaryKey
        result = self.db.asPK( result)
        self._results.append( result)

    
    def results(self):
        return self.db.resultTable.getMany(self._results)
            
    
    def __str__(self):
        out = '{}\t{}'.format(self.name, self.sampleType)
        return out

    def tree(self, raw=False):
        out = {'label': self.name, 'nodes':[]}
        for res in self.results():
            out['nodes'].append( res.tree(raw=True))
        if raw:
            return out
        else:
            return archy(out)

