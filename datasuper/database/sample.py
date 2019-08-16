from .base_record import BaseRecord
from pyarchy import archy
from .result import ResultRecord


class SampleRecord(BaseRecord):
    '''Class that keeps track of a sample, essentially a set of results.'''

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
        '''Create a dict that serializes this sample.'''
        out = super(SampleRecord, self).to_dict()
        out['results'] = self._results
        out['sample_type'] = str(self.sampleType)
        return out

    def _validStatus(self):
        return self.detailedStatus()[0]

    def _detailedStatus(self):
        try:
            results = self.results()
        except KeyError:
            return False, 'one_or_more_results_is_missing'
        for res in results:
            if not isinstance(res, ResultRecord):
                return False, 'result_is_not_actually_a_result'
            resStatus, resStatusMsg = res.detailedStatus()
            if not resStatus:
                return False, 'result_has_bad_status' + ':' + resStatusMsg
        return True, 'all_good'

    def addResult(self, result):
        '''Add a result to this sample. Return this sample.'''
        if issubclass(type(result), BaseRecord):
            result = result.primaryKey
        result = self.db.asPK(result)
        self._results.add(result)
        return self

    def dropResult(self, result):
        '''Remove a result from this sample but do not commit to disk.'''
        if issubclass(type(result), BaseRecord):
            result = result.primaryKey
        result = self.db.asPK(result)
        self._results.remove(result)

    def results(self, resultTypes=None):
        '''Return a list of results in this sample.'''
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
        '''Returns a JSONable tree starting at this sample.'''
        out = {'label': self.name, 'nodes': []}
        for res in self.results():
            out['nodes'].append(res.tree(raw=True))
        if raw:
            return out
        return archy(out)
