
class RecordExistsError(Exception):
    pass


class NoSuchRecordError(Exception):
    pass


class InvalidRecordStateError(Exception):
    pass


class RepoReadOnlyError(Exception):
    pass
