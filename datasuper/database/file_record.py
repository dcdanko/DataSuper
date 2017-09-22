from .base_record import *
import hashlib

class FileRecord( BaseRecord):
    
    def __init__(self, repo, **kwargs):
        super(FileRecord, self).__init__(repo, **kwargs)
        self._filepath = self.repo.pathFromRepo(kwargs['filepath'])
        self.fileType = self.repo.validateFileType( kwargs['file_type'])        
        try:
            self.checksum = kwargs['checksum']
        except KeyError:
            self.checksum = self._currentChecksum()
        self.cachedValid = None

    def to_dict(self):
        out = super(FileRecord, self).to_dict()
        out['filepath'] = self._filepath
        out['checksum'] = self.checksum
        out['file_type'] = self.fileType
        return out

    def filepath(self):
        return self.repo.toAbspath(self._filepath)
    
    def _currentChecksum(self):
        return hashlib.sha256(open(self.filepath(), 'rb').read(4096)).hexdigest()
    
    def validStatus(self):
        if self.cachedValid is None:
            self.cachedValid = self.checksum == self._currentChecksum()
        return self.cachedValid
    
    def __str__(self):
        out = '{}\t{}'.format(self.name, self.filepath())
        return out

