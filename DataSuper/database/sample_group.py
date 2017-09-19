import meta_ultra.config as config

class SampleGroup( BaseRecord):
    def __init__(self,**kwargs):
        super(SampleGroup, self).__init__(**kwargs)
        self._subgroups = kwargs['subgroups'] # n.b. these are keys not objects
        self._directSamples = kwargs['direct_samples'] # n.b. these are keys not objects
        self._directResults = kwargs['direct_results'] # n.b. these are keys not objects


    def validStatus(self):
        for subgroup in self.subgroups():
            if type(subgroup) != SampleGroup:
                return False
            if not subgroup.validStatus():
                return False
        for sample in self.directSamples():
            if type(sample) != Sample:
                return False
            if not sample.validStatus():
                return False
        for result in self.directResults():
            if type(result) != Result:
                return False
            if not result.validStatus():
                return False
        return True
            
    def to_dict(self):
        out = super(Sample, self).to_dict()
        out['subgroups'] = self._subgroups
        out['direct_samples'] = self._directSamples
        out['direct_results'] = self._directResults
        return out

    def __str__(self):
        out = '{}'.format(self.name)
        for k, v in self.metadata.items():
            if ' ' in str(v):
                out += '\t{}="{}"'.format(k,v)
            else:
                out += '\t{}={}'.format(k,v)
        return out

    def directSamples(self):
        raise NotImplementedError()

    def directResults(self):
        raise NotImplementedError()        

    def subgroups(self):
        raise NotImplementedError()        
    
    @classmethod
    def tableName():
        return config.sample_group_table_name

    
    
