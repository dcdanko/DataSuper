
class FileRecord( BaseRecord):
    
    def __init__(self, **kwargs):
        super(Result, self).__init__(**kwargs)
        self.filepath = kwargs['filepath']
        self.fileType = self.repo.validateFileType( kwargs['file_type'])        
        try:
            self.checksum = kwargs['checksum']
        except KeyError:
            self.checksum = self._currentChecksum()
        self.cachedValid = None

    def to_dict(self):
        out = super(Sample, self).to_dict()
        out['filepath'] = self.filepath
        out['checksum'] = self.checksum
        out['file_type'] = self.fileType
        return out

    def _currentChecksum(self):
        raise NotImplementedError()
    
    def validStatus(self):
        if self.cachedValid is None:
            self.cachedValid = self.checksum == self._currentChecksum()
        return self.cachedValid
    
    def __str__(self):
        out = '{}\t{}'.format(self.checksum,
                              self.filepath)
        return out

