from .base_record import BaseRecord
from .sample import SampleRecord
from .result import ResultRecord
from pyarchy import archy


class SampleGroupRecord(BaseRecord):
    '''Class that tracks a group, a set of groups, samples, and results.'''

    def __init__(self, repo, **kwargs):
        super(SampleGroupRecord, self).__init__(repo, **kwargs)
        try:
            self._subgroups = self.db.asPKs(kwargs['subgroups'])
        except KeyError:
            self._subgroups = []

        try:
            self._directSamples = self.db.asPKs(kwargs['direct_samples'])
        except KeyError:
            self._directSamples = []

        try:
            self._directResults = self.db.asPKs(kwargs['direct_results'])
        except KeyError:
            self._directResults = []

    def _validStatus(self):
        return self._validStatus()[0]

    def _detailedStatus(self):
        for subgroup in self.subgroups():
            if subgroup.primaryKey == self.primaryKey:
                return False, 'recursive_group'
            if not isinstance(subgroup, SampleGroupRecord):
                return False, 'subgroup_is_not_group'
            subStatus, subStatusMsg = subgroup.detailedStatus()
            if not subStatus:
                return False, 'bad_subgroup_status' + ':' + subStatusMsg
        for sample in self.directSamples():
            if not isinstance(sample, SampleRecord):
                return False, 'sample_is_not_sample'
            sampStatus, sampStatusMsg = sample.detailedStatus()
            if not sampStatus:
                return False, 'bad_sample_status' + ':' + sampStatusMsg
        for result in self.directResults():
            if not isinstance(result, ResultRecord):
                return False, 'result_is_not_result'
            resStatus, resMsg = result.detailedStatus()
            if not resStatus:
                return False, 'bad_result_status' + ':' + resMsg
        return True, 'all_good'

    def to_dict(self):
        '''Return a dict that serializes this sample.'''
        out = super(SampleGroupRecord, self).to_dict()
        out['subgroups'] = self._subgroups
        out['direct_samples'] = self._directSamples
        out['direct_results'] = self._directResults
        return out

    def addSample(self, sample):
        '''Add a sample to this group.'''
        if issubclass(type(sample), BaseRecord):
            sample = sample.primaryKey
        sample = self.db.asPK(sample)
        self._directSamples.add(sample)

    def addResult(self, result):
        '''Add a result to this group.'''
        if issubclass(type(result), BaseRecord):
            result = result.primaryKey
        result = self.db.asPK(result)
        self._directResults.add(result)

    def directSamples(self):
        '''Return a list of samples attached to this group directly.'''
        return self.db.sampleTable.getMany(self._directSamples)

    def allSamples(self):
        '''Yield samples from this group or from subgroups.'''
        for sample in self.directSamples():
            yield sample
        for subgroup in self.subgroups():
            for sample in subgroup.allSamples():
                yield sample

    def directResults(self):
        '''Return a list of results directly attached to this group.'''
        return self.db.resultTable.getMany(self._directResults)

    def allResults(self, resultTypes=None):
        '''Return a list of results from this group or from subgroups.'''
        results = []
        for res in self.directResults():
            results.append(res)
        for sample in self.directSamples():
            for res in sample.results():
                results.append(res)
        for subgroup in self.subgroups():
            for res in subgroup.allResults():
                results.append(res)

        if resultTypes is not None:
            filtered = []
            rTypes = {rtype for rtype in resultTypes}
            for result in results:
                if result.resultType() in rTypes:
                    filtered.append(result)
            results = filtered
        return results

    def subgroups(self):
        '''Return a list of subgroups attached to this group.'''
        return self.dbTable.getMany(self._subgroups)

    def __str__(self):
        out = '{}'.format(self.name)
        for k, v in self.metadata.items():
            if ' ' in str(v):
                out += '\t{}="{}"'.format(k, v)
            else:
                out += '\t{}={}'.format(k, v)
        return out

    def tree(self, raw=False):
        '''Returns a JSONable tree starting at this group.'''
        out = {'label': self.name, 'nodes': []}
        resOut = {'label': 'direct_results', 'nodes': []}
        for res in self.directResults():
            resOut['nodes'].append(res.tree(raw=True))
        if resOut['nodes']:
            out['nodes'].append(resOut)

        sOut = {'label': 'direct_samples', 'nodes': []}
        for sample in self.directSamples():
            sOut['nodes'].append(sample.tree(raw=True))
        if sOut['nodes']:
            out['nodes'].append(sOut)

        gOut = {'label': 'subgroups', 'nodes': []}
        for subgroup in self.subgroups():
            gOut['nodes'].append(subgroup.tree(raw=True))
        if gOut['nodes']:
            out['nodes'].append(gOut)
        if raw:
            return out
        return archy(out)
