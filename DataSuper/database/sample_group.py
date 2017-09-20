
class SampleGroupRecord( BaseRecord):
    def __init__(self,**kwargs):
        super(SampleGroup, self).__init__(**kwargs)
        self._subgroups = kwargs['subgroups'] # n.b. these are keys not objects
        self._directSamples = kwargs['direct_samples'] # n.b. these are keys not objects
        self._directResults = kwargs['direct_results'] # n.b. these are keys not objects

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

    def directSamples(self):
        return self.db.sampleTbl.getMany( self._directSamples)
        
    def allSamples(self):
        for sample in self.directSamples():
            yield sample
        for subgroup in self.subgroups():
            for sample in subgroup.allSamples():
                yield sample
                
    def directResults(self):
        return self.db.resultTbl.getMany( self._directResults)

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
        return self.dbTbl.getMany( self._subgroups)
    
    def __str__(self):
        out = '{}'.format(self.name)
        for k, v in self.metadata.items():
            if ' ' in str(v):
                out += '\t{}="{}"'.format(k,v)
            else:
                out += '\t{}={}'.format(k,v)
        return out

    
    
