        
class SampleRecord( BaseRecord):
    def __init__(self,**kwargs):
        super(Sample, self).__init__(**kwargs)
        self._results = kwargs['results'] # n.b. these are keys not objects
        self.sampleType = self.repo.validateSampleType( kwargs['sample_type'])
        
    def to_dict(self):
        out = super(Sample, self).to_dict()
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

    def results(self):
        return self.db.resultTbl.getMany(self._results):
            
    
    def __str__(self):
        out = '{}\t{}'.format(self.name, self.projectName)
        for k, v in self.metadata.items():
            if ' ' in str(v):
                out += '\t{}="{}"'.format(k,v)
            else:
                out += '\t{}={}'.format(k,v)
        return out

