

class RecordExistsError(Exception):
    pass


class NoSuchRecordError(Exception):
    pass


class InvalidRecordStateError(Exception):
    pass


class RepoReadOnlyError(Exception):
    pass


class SchemaMismatchError(Exception):

    @classmethod
    def raise_with_message(cls, resultType, pk, schema, fileRecs):
        msg = ('Could not build schema for result type {}.\n'
               '\tPrimary Key: {}\n'
               '\tSchema:\n{}\n'
               '\tFile Record:\n{}\n')
        msg = msg.format(resultType, pk, schema, fileRecs)
        raise cls(msg)
