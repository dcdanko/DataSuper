from .base_record import BaseRecord
import hashlib
from os import rename, path, makedirs
from shutil import copy2


class FileRecord(BaseRecord):
    '''Class that keeps track of an actual file.'''

    def __init__(self, repo, **kwargs):
        super(FileRecord, self).__init__(repo, **kwargs)

        self._filepath = self.repo.pathFromRepo(kwargs['filepath'])
        self.fileType = self.repo.validateFileType(kwargs['file_type'])
        self.cached_current_checksum = None
        try:
            self.checksum = kwargs['checksum']
        except KeyError:
            self.checksum = self._currentChecksum()
        self.cachedValid = None
        self.cachedMsg = None

    def to_dict(self):
        '''Create a dict that serializes this file record.'''
        out = super(FileRecord, self).to_dict()
        out['filepath'] = self._filepath
        out['checksum'] = self.checksum
        out['file_type'] = self.fileType
        return out

    def move(self, new_path):
        '''Move the file to the new path, provided the new path is unoccupied.'''
        abs_new_path = path.abspath(new_path)
        if not path.isfile(abs_new_path):
            rename(self.filepath(), abs_new_path)

    def filepath(self):
        '''Return the abspath of the actual file.'''
        return self.repo.toAbspath(self._filepath)

    def _currentChecksum(self):
        if self.cached_current_checksum:
            return self.cached_current_checksum
        bits = open(self.filepath(), 'rb').read(4096)
        self.cached_current_checksum = hashlib.sha256(bits).hexdigest()
        return self.cached_current_checksum

    def _validStatus(self):
        return self._detailedStatus()[0]

    def _detailedStatus(self):
        '''
        msg = 'all_good'
        if self.cachedValid is None:
            try:
                self.cachedValid = self.checksum == self._currentChecksum()
                if not self.cachedValid:
                    msg = 'bad_checksum' + ':' + self.filepath()
                self.cachedMsg = msg
            except FileNotFoundError:
                self.cachedValid = False
                self.cachedMsg = 'file_not_found' + ':' + self.filepath()
        return self.cachedValid, self.cachedMsg
        '''
        if self.cachedValid is not None:
            return self.cachedValid, self.cachedMsg
        if not path.isfile(self.filepath()):
            self.cachedValid = False
            self.cachedMsg = 'file_not_found' + ':' + self.filepath()
        else:
            self.cachedValid = True
            self.cachedMsg = 'all_good'
        return self.cachedValid, self.cachedMsg

    def __str__(self):
        out = '{}\t{}'.format(self.name, self.filepath())
        return out

    def remove(self, atomic=False):
        '''Remove the file record but don't touch anything else.'''
        if not atomic:
            raise NotImplementedError()
        self.atomicDelete()

    def copy(self, new_path):
        """Copy the file to new_path provided new_path does not exist.

        Set internal filepath to point at the new path.

        Make any intermediate dirs.
        """
        if not path.isfile(self.filepath()):
            return
        new_path = self.repo.toAbspath(new_path)
        if path.isfile(new_path):
            raise FileExistsError(new_path)
        makedirs(path.dirname(new_path), exist_ok=True)
        copy2(self.filepath(), new_path)  # preserves file metadata
        self._filepath = new_path
        return self
