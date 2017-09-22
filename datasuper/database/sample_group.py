from .base_record import *
from .sample import *
from .result import *
from .file_record import *
from pyarchy import archy

class SampleGroupRecord( BaseRecord):
    def __init__(self, repo, **kwargs):
        super(SampleGroupRecord, self).__init__(repo, **kwargs)
        try:
            self._subgroups = self.db.asPKs(kwargs['subgroups']) # n.b. these are keys not objects
        except KeyError:
            self._subgroups = []

        try:
            self._directSamples = self.db.asPKs(kwargs['direct_samples']) # n.b. these are keys not objects
        except KeyError:
            self._directSamples = []

        try:
            self._directResults = self.db.asPKs(kwargs['direct_results']) # n.b. these are keys not objects
        except KeyError:
            self._directResults = []
            
    def validStatus(self):
        for subgroup in self.subgroups():
            if subgroup.primaryKey == self.primaryKey:
                return False
            if type(subgroup) != SampleGroupRecord:
                return False
            if not subgroup.validStatus():
                return False
        for sample in self.directSamples():
            if type(sample) != SampleRecord:
                return False
            if not sample.validStatus():
                return False
        for result in self.directResults():
            if type(result) != ResultRecord:
                return False
            if not result.validStatus():
                return False
        return True
            
    def to_dict(self):
        out = super(SampleGroupRecord, self).to_dict()
        out['subgroups'] = self._subgroups
        out['direct_samples'] = self._directSamples
        out['direct_results'] = self._directResults
        return out

    def addSample(self, sample):
        if issubclass( type(sample), BaseRecord):
            sample = sample.primaryKey
        sample = self.db.asPK(sample)
        self._directSamples.append(sample)

    def addResult(self, result):
        if issubclass( type(result), BaseRecord):
            result = result.primaryKey
        result = self.db.asPK( result)
        self._directResults.append( result)

        
    def directSamples(self):
        return self.db.sampleTable.getMany( self._directSamples)
        
    def allSamples(self):
        for sample in self.directSamples():
            yield sample
        for subgroup in self.subgroups():
            for sample in subgroup.allSamples():
                yield sample
                
    def directResults(self):
        return self.db.resultTable.getMany( self._directResults)

    def allResults(self):
        for res in self.directResults():
            yield res
        for sample in self.directSamples():
            for res in sample.results():
                yield res
        for subgroup in self.subgroups():
            for res in subgroup.allResults():
                yield res
    
    def subgroups(self):
        return self.dbTable.getMany( self._subgroups)
    
    def __str__(self):
        out = '{}'.format(self.name)
        for k, v in self.metadata.items():
            if ' ' in str(v):
                out += '\t{}="{}"'.format(k,v)
            else:
                out += '\t{}={}'.format(k,v)
        return out

    
    def tree(self, raw=False):
        out = {'label' : self.name, 'nodes': []}
        resOut = {'label': 'direct_results', 'nodes': []}
        for res in self.directResults():
            resOut['nodes'].append( res.tree(raw=True))
        if len(resOut['nodes']) > 0:
            out['nodes'].append(resOut)

        sOut = {'label': 'direct_samples', 'nodes': []}
        for sample in self.directSamples():
            sOut['nodes'].append( sample.tree(raw=True))
        if len(sOut['nodes']) > 0:
            out['nodes'].append(sOut)

        gOut = {'label': 'subgroups', 'nodes': []}
        for subgroup in self.subgroups():
            gOut['nodes'].append( subgroup.tree(raw=True))
        if len(gOut['nodes']) > 0:
            out['nodes'].append(gOut)
        if raw:
            return out
        return archy(out)
        
