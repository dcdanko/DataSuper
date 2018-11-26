import os.path
from yaml_backed_structs import *
from datasuper.database import *
from .errors import *


class Repo:
    '''Represents a repo that stores records and config values.

    Attributes:
        db (datasuper.Database): The database that contains all records.
        fileTable (datasuper.DatabaseTable): Alias to db.fileTable. The
            table that contains file records.
        resultTable (datasuper.DatabaseTable): Alias to db.resultTable. The
            table that contains result records.
        sampleTable (datasuper.DatabaseTable): Alias to db.sampleTable. The
            table that contains sample records.
        sampleGroupTable (datasuper.DatabaseTable): Alias to
            db.sampleGroupTable. The table that contains sample-group records.

    '''
    repoDirName = '.datasuper'
    dbRoot = 'datasuper.tinydb.json'
    repoMetaRoot = 'repo-metadata.yml'
    resultSchemaRoot = 'result-schemas.yml'
    fileTypesRoot = 'file-types.yml'
    sampleTypesRoot = 'sample-types.yml'

    def __init__(self, abspath):
        self.closed = False
        self.abspath = abspath
        self.readOnly = True

        dbPath = os.path.join(self.abspath, Repo.dbRoot)
        self.db = Database.loadDatabase(self, dbPath, self.readOnly)
        self.fileTable = self.db.fileTable
        self.resultTable = self.db.resultTable
        self.sampleTable = self.db.sampleTable
        self.sampleGroupTable = self.db.sampleGroupTable

        repoMetaPath = os.path.join(self.abspath, Repo.repoMetaRoot)
        self.repoMeta = PersistentDict(repoMetaPath)

        resultSchemaPath = os.path.join(self.abspath, Repo.resultSchemaRoot)
        self.resultSchema = PersistentDict(resultSchemaPath)

        fileTypesPath = os.path.join(self.abspath, Repo.fileTypesRoot)
        self.fileTypes = PersistentDict(fileTypesPath)

        sampleTypesPath = os.path.join(self.abspath, Repo.sampleTypesRoot)
        self.sampleTypes = PersistentSet(sampleTypesPath)

    def repoId(self):
        '''Return a random id for this repo.

        Generate and save that id if it does not already exist.
        '''
        try:
            return self.repoMeta['repo_id']
        except KeyError:
            N = 20
            chars = string.ascii_uppercase + string.digits
            repoid = [rchoice(chars) for _ in range(N)]
            repoid = ''.join(repoid)
            self.repoMeta['repo_id'] = repoid
            return self.repoMeta['repo_id']

    def flush(self):
        """Flush new data to disk."""
        self.db.flush()

    def close(self):
        '''Close the repo to further write operations.'''
        self.db.close()
        self.closed = True

    def _notReadOnly(self):
        if self.readOnly:
            raise RepoReadOnlyError()
        return True

    def addSampleType(self, sampleType):
        '''Add a new sampleType (str) to the repo.'''
        if self._notReadOnly():
            self.sampleTypes.add(sampleType)

    def getSampleTypes(self):
        '''Return a list of sample types in the repo.'''
        return [el for el in self.sampleTypes]

    def addFileType(self, fileType, ext=None):
        '''Add a new fileType to the repo with optional extension.'''
        if self._notReadOnly():
            if type(fileType) is dict:
                assert 'name' in fileType
                name = fileType['name']
                try:
                    ext = fileType['ext']
                except KeyError:
                    ext = name
                self.fileTypes[name] = ext
            else:
                if ext is None:
                    ext = fileType
                self.fileTypes[fileType] = ext

    def getFileTypes(self):
        '''Return a list of file types in the repo.'''
        fTypes = self.fileTypes.items()
        return [{'name': name, 'ext': ext} for name, ext in fTypes]

    def getFileTypeExt(self, fileType):
        '''Return the extension for a given fileType.'''
        try:
            return self.fileTypes[fileType]
        except KeyError:
            raise TypeNotFoundError(fileType)

    def addResultSchema(self, resultType, resultSchema, modify=False):
        '''Set a resultSchema for resultType.'''
        if self._notReadOnly():
            if modify or (resultType not in self.resultSchema):
                self.resultSchema[resultType] = resultSchema

    def getResultTypes(self):
        '''Return a list of result types in the repo.'''
        return [el for el in self.resultSchema.keys()]

    def validateSampleType(self, sampleType):
        '''Return sampleType if it is in the repo, else raise an error.'''
        if sampleType in self.sampleTypes:
            return sampleType
        raise TypeNotFoundError(str(sampleType))

    def validateResultType(self, resType):
        '''Return resultType if it is in the repo, else raise an error.'''
        if resType in self.resultSchema:
            return resType
        raise TypeNotFoundError(resType)

    def validateFileType(self, fileType):
        '''Return fileType if it is in the repo, else raise an error.'''
        if fileType in self.fileTypes:
            return fileType
        raise TypeNotFoundError(fileType)

    def getResultSchema(self, resType):
        '''Return the schema associated with a resType.'''
        schema = self.resultSchema[resType]
        return schema

    def pathFromRepo(self, fpath):
        '''Returns an absolute path found from the top of the repo.

        This function converts a relative path from the top of the repo
        into an absolute path. If the filepath requested is not in the
        repo this function just returns an absolute path for that file.

        Args:
            fpath (str): The relative path to convert.

        Returns:
            An absolute filepath.
        '''

        pathToRepo = os.path.dirname(self.abspath)  # get the repo abspath
        abspath = os.path.abspath(os.path.join(pathToRepo, fpath))
        return abspath

    def toAbspath(self, repopath):
        pathToRepo = os.path.dirname(self.abspath)
        abspath = os.path.join(pathToRepo, repopath)
        return abspath

    def checkStatus(self):
        out = self.db.checkStatus()
        return out

    @staticmethod
    def loadRepo(startDir='.', recurse=True):
        '''Build and return the repo initialized in or above startRepo.

        Search for a repo in `startDir`. If no repo is found recurse to
        the parent directory unless `recurse` is False. If no repo is
        found deeper recursion is impossible throw an error:

        Args:
            startDir (:obj:`str`, optional): The dir to search in. Defaults
                to the current working directory.
            recurse (:obj:`bool`, optional): Flag specifying if the fucntion
                should recurse up.

        Returns:
            The repo if successful, None otherwise.

        Raises:
            NoRepoFoundError: If no repo is found.
        '''

        startPath = os.path.abspath(startDir)
        if Repo.repoDirName in os.listdir(startPath):
            repoPath = os.path.join(startPath, Repo.repoDirName)
            return Repo(repoPath)

        up = os.path.dirname(startPath)
        if up == startPath:
            raise NoRepoFoundError()
        if recurse:
            return Repo.loadRepo(startDir=up)
        else:
            raise NoRepoFoundError()

    @staticmethod
    def initRepo(targetDir='.'):
        '''Create a new repo in `targetDir`. Return the new repo.'''
        try:
            p = os.path.abspath(targetDir)
            p = os.path.join(p, Repo.repoDirName)
            os.makedirs(p)
            newRepo = Repo.loadRepo(startDir=targetDir, recurse=False)
            return newRepo
        except FileExistsError:
            pass
        raise RepoAlreadyExistsError()

    def __enter__(self):
        '''Set the repo to read/write mode.'''
        if self.closed:
            reopen = Repo.loadRepo(startDir=self.abspath)
            reopen.readOnly = False
            return reopen
        self.readOnly = False
        return self

    def __exit__(self, *args):
        '''Close the repo.'''
        self.flush()
        self.readOnly = True

