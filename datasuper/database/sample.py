from .base_record import *
from pyarchy import archy
from .result import *


class SampleRecord(BaseRecord):
    def __init__(self, repo, **kwargs):
        super(SampleRecord, self).__init__(repo, **kwargs)
        try:
            _results = kwargs['results']
        except KeyError:
            _results = []
        try:
            self._results = self.db.asPKs(_results)
        except KeyError:
            raise InvalidRecordStateError('Could not convert key to result')
        # n.b. these are keys not objects
        self.sampleType = self.repo.validateSampleType(kwargs['sample_type'])

    def to_dict(self):
        out = super(SampleRecord, self).to_dict()
        out['results'] = self._results
        out['sample_type'] = str(self.sampleType)
        return out

    def _validStatus(self):
        for res in self.results():
            if type(res) != ResultRecord:
                return False
            if not res.validStatus():
                return False
        return True

    def addResult(self, result):
        if issubclass(type(result), BaseRecord):
            result = result.primaryKey
        result = self.db.asPK(result)
        self._results.add(result)
        return self

    def dropResult(self, result):
        # still requires the sample to be saved!
        if issubclass(type(result), BaseRecord):
            result = result.primaryKey
        result = self.db.asPK(result)
        self._results.remove(result)


    def results(self, resultTypes=None):

        results = self.db.resultTable.getMany(self._results)
        if resultTypes is not None:
            filtered = []
            rTypes = {rtype for rtype in resultTypes}
            for result in results:
                if result.resultType() in rTypes:
                    filtered.append(result)
            results = filtered

        return results
            
    
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

