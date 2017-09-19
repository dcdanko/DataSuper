from tinydb import TinyDB
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
import sys

class MURepoNotFoundError( Exception):
    pass

class Repo:

    def __init__(self, repoPath, caching=True):

        self.repoPath = repoPath
        if caching:
            self.repo = TinyDB(self.repoPath, storage=CachingMiddleware(JSONStorage))
        else:
            self.repo = TinyDB(self.repoPath)

    def close(self):
        self.repo.close()

    def table(self, tableName):
        return self.repo.table( tableName)


    @staticmethod
    def getRepo(searchDir='.', caching=True):
        searchDir = os.path.abspath(searchDir)
        if config.mu_repo_dir in os.listdir(searchDir):
            repoPath = os.path.join(searchDir,mu_repo_path)
            return Repo(repoPath, caching=caching)
        else:
            # recurse up
            up = os.path.dirname(searchDir)
            if up == searchDir:
                raise MURepoNotFoundError()
            else:
                return getRepo(searchDir=up, caching=caching)
        
