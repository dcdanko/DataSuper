from .database_exceptions import (
    InvalidRecordStateError,
    RecordExistsError,
)


class BaseRecord:
    '''Abstract class providing functions common to all records.

    Attributes:
        repo (datasuper.Repo): The repo that contains this record.
        db (datasuper.Database): The database that contains this record.
        dbTable (datasuper.DatabaseTable): The table that contains this record.
        name (str): The human readable name of this record.
        primaryKey (str): The invariant machine readable name of this record.
        metadata (dict): A key value map storing metadata. Must be able to
            convert to JSON.

    '''

    def __init__(self, repo, **kwargs):
        self.repo = repo
        self.db = repo.db
        self.dbTable = self.db.getTable(type(self))
        self.name = kwargs['name']
        try:
            self.primaryKey = kwargs['primary_key']
        except KeyError:
            self.primaryKey = None
        try:
            self.metadata = kwargs['metadata']
        except KeyError:
            self.metadata = {}
        self.cachedStatus = None

    def exists(self):
        '''Return True if this record has been saved at some point.'''
        return self.dbTable.exists(self.primaryKey)

    def nameExists(self):
        '''Return True if the current name exists in the table.'''
        return self.dbTable.exists(self.name)

    def save(self, modify=False):
        '''Save this record to the table and return the record.

        Args:
            modify (:obj:`bool`, optional): Flag to modify this record
                if it already exists. Defaults to False.

        '''
        status, statusMsg = self.detailedStatus()
        if not status:
            raise InvalidRecordStateError(statusMsg)

        pkExists = self.exists()
        nameExists = self.nameExists()
        if pkExists and not modify:
            raise RecordExistsError('pk_exists:{}'.format(self.primary_key))
        elif nameExists and not modify:
            raise RecordExistsError('name_exists:{}'.format(self.name))
        elif pkExists and modify:
            rec = self.dbTable.getRaw(self.primaryKey)
            rec = self._mergeDicts(rec)
            self.dbTable.update(self.primaryKey, rec)
            return self.dbTable.get(self.primaryKey)
        elif nameExists and modify:
            other = self.dbTable.get(self.name)
            rec = other._mergeDicts(self.to_dict())
            other.dbTable.update(other.primaryKey, rec)
            return other.dbTable.get(other.primaryKey)
        else:
            savedSelf = self.dbTable.insert(self.to_dict())
            self.primaryKey = savedSelf.primaryKey
            return savedSelf

    def _mergeDicts(self, rec):
        mydict = self.to_dict()
        for k, v in mydict.items():
            if k in rec and isinstance(v, dict) and isinstance(rec[k], dict):
                for subk, subv in v.items():
                    rec[k][subk] = subv
            else:
                rec[k] = v
        return rec

    def rename(self, newName):
        '''Change the human readbale name of this record. Return self.'''
        self.dbTable.rename(self.primaryKey, newName)
        self.name = newName
        return self

    def remove(self):
        raise NotImplementedError()

    def atomicDelete(self):
        '''Remove just this record.'''
        self.dbTable.remove(self.primaryKey)

    def raw(self):
        '''Return the dict corresponding to this record.

        This function is different than `to_dict()`. It returns
        the dict that is actually stored in the table.

        '''
        return self.dbTable.getRaw(self.primaryKey)

    def validStatus(self):
        '''Return True if this record is valid, else False.'''
        try:
            return self._validStatus()
        except Exception:
            return False

    def _validStatus(self):
        raise NotImplementedError()

    def detailedStatus(self):
        if self.cachedStatus is not None:
            return self.cachedStatus
        try:
            self.cachedStatus = self._detailedStatus()
            return self.cachedStatus
        except Exception:
            raise

    def _detailedStatus(self):
        raise NotImplementedError()

    def to_dict(self):
        '''Create a dict that serializes this record.

        This function is different than `raw()`. It returns
        the dict that corresponds to this records current state.

        '''
        out = {
            'primary_key': self.primaryKey,
            'name': self.name,
            'metadata': str(self.metadata)
        }
        return out
