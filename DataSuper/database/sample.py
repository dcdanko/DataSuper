import meta_ultra.config as config
from .sample_type import *

        
class Sample( BaseRecord):
    def __init__(self,**kwargs):
        super(Sample, self).__init__(**kwargs)
        self._results = kwargs['results'] # n.b. these are keys not objects
        self.sampleType = SampleType( kwargs['sample_type'])
        
    def to_dict(self):
        out = super(Sample, self).to_dict()
        out['results'] = self._results
        out['sample_type'] = str(self.sampleType)
        return out

    def validStatus(self):
        for result 
        try:
            Project.get(self.projectName)
        except NoSuchRecordError:
            return False
        return True

    def results(self):
        raise NotImplementedError()

    
    def __str__(self):
        out = '{}\t{}'.format(self.name, self.projectName)
        for k, v in self.metadata.items():
            if ' ' in str(v):
                out += '\t{}="{}"'.format(k,v)
            else:
                out += '\t{}={}'.format(k,v)
        return out

    @classmethod
    def tableName(ctype):
        return config.sample_table_name

