import os.path
from yaml_backed_structs import *
from datasuper.database import *


class NoRepoFoundError(Exception):
    pass


class RepoAlreadyExistsError(Exception):
    pass


class TypeNotFoundError(Exception):
    pass


class Repo:
    repoDirName = '.datasuper'
    dbRoot = 'datasuper.tinydb.json'
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

        resultSchemaPath = os.path.join(self.abspath, Repo.resultSchemaRoot)
        self.resultSchema = PersistentDict(resultSchemaPath)

        fileTypesPath = os.path.join(self.abspath, Repo.fileTypesRoot)
        self.fileTypes = PersistentDict(fileTypesPath)

        sampleTypesPath = os.path.join(self.abspath, Repo.sampleTypesRoot)
        self.sampleTypes = PersistentSet(sampleTypesPath)

    def close(self):
        self.db.close()
        self.closed = True

    def _notReadOnly(self):
        if self.readOnly:
            raise RepoReadOnlyError()
        return True

    def addSampleType(self, sampleType):
        if self._notReadOnly():
            self.sampleTypes.add(sampleType)

    def getSampleTypes(self):
        return [el for el in self.sampleTypes]

    def addFileType(self, fileType, ext=None):
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
        fTypes = self.fileTypes.items()
        return [{'name': name, 'ext': ext} for name, ext in fTypes]

    def getFileTypeExt(self, ftype):
        try:
            return self.fileTypes[ftype]
        except KeyError:
            raise TypeNotFoundError(ftype)

    def addResultSchema(self, resultType, resultSchema):
        if self._notReadOnly():
            self.resultSchema[resultType] = resultSchema

    def getResultTypes(self):
        return [el for el in self.resultSchema.keys()]

    def validateSampleType(self, sampleType):
        if sampleType in self.sampleTypes:
            return sampleType
        raise TypeNotFoundError(str(sampleType))

    def validateResultType(self, resType):
        if resType in self.resultSchema:
            return resType
        raise TypeNotFoundError(resType)

    def validateFileType(self, fileType):
        if fileType in self.fileTypes:
            return fileType
        raise TypeNotFoundError(fileType)

    def getResultSchema(self, resType):
        schema = self.resultSchema[resType]
        return schema

    def pathFromRepo(self, fpath):
        '''
        returns an absolute path found from the top of the repo
        
        this means that the correct path will be found even if this
        is called from a subdirectory
        '''
        # get a path that starts at the top of the repo
        pathToRepo = os.path.dirname(self.abspath)
        abspath = os.path.abspath(os.path.join(pathToRepo, fpath))
#       pathFromRepo = abspath[ len(pathToRepo)+1:]
        return abspath

    def toAbspath(self, repopath):
        pathToRepo = os.path.dirname(self.abspath)
        abspath = os.path.join(pathToRepo, repopath)
        return abspath

    @staticmethod
    def loadRepo(startDir='.', recurse=True):
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
        if self.closed:
            reopen = Repo.loadRepo(startDir=self.abspath)
            reopen.readOnly = False
            return reopen
        self.readOnly = False
        return self

    def __exit__(self, *args):
        self.close()
