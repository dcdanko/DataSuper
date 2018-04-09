

class NoRepoFoundError(Exception):
    pass


class RepoAlreadyExistsError(Exception):
    pass


class TypeNotFoundError(Exception):
    pass


class SchemaMismatchError(Exception):
    pass

    @classmethod
    def raise_with_message(cls, resultType, schema, fileRecs):
        msg = ('Could not build schema for result type {}.\n'
               'Schema:\n{}\n'
               'File Record:\n{}\n')
        msg = msg.format(resultType, schema, fileRecs)
        raise cls(msg)
