

class NoRepoFoundError(Exception):
    pass


class RepoAlreadyExistsError(Exception):
    pass


class RepoReadOnlyError(Exception):
    pass


class TypeNotFoundError(Exception):
    pass
