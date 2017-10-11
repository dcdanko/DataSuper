import os.path
from yaml_backed_structs import *
from datasuper.database import *

class NoRepoFoundError( Exception):
    pass

class RepoAlreadyExistsError( Exception):
    pass

class TypeNotFoundError( Exception):
    pass

class Repo:
    repoDirName = '.datasuper'
    dbRoot = 'datasuper.tinydb.json'
    resultSchemaRoot = 'result-schemas.yml'
    fileTypesRoot = 'file-types.yml'
    sampleTypesRoot = 'sample-types.yml'
    
    def __init__(self, abspath):
        self.abspath = abspath
        self.readOnly = True
        
        dbPath = os.path.join(self.abspath, Repo.dbRoot)
        self.db = Database.loadDatabase(self, dbPath, self.readOnly)

        resultSchemaPath = os.path.join(self.abspath, Repo.resultSchemaRoot)
        self.resultSchema = PersistentDict( resultSchemaPath)

        fileTypesPath = os.path.join(self.abspath, Repo.fileTypesRoot)
        self.fileTypes = PersistentDict( fileTypesPath)

        sampleTypesPath = os.path.join(self.abspath, Repo.sampleTypesRoot)
        self.sampleTypes = PersistentSet( sampleTypesPath)


    def close(self):
        self.db.close()

    def _notReadOnly(self):
        if self.readOnly:
            raise RepoReadOnlyError()
        return True
        
    def addSampleType( self, sampleType):
        if self._notReadOnly():
            self.sampleTypes.add(sampleType)

    def getSampleTypes( self):
        return [el for el in self.sampleTypes]

    def addFileType( self, fileType, ext=None):
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
        return [{'name': name, 'ext': ext} for name, ext in self.fileTypes.items()]
    
    def addResultSchema( self, resultType, resultSchema):
        if self._notReadOnly():
            self.resultSchema[resultType] = resultSchema

    def getResultTypes(self):
        return [el for el in self.resultSchema.keys()]        
        
        
    def validateSampleType(self, sampleType):
        if sampleType in self.sampleTypes:
            return sampleType
        raise TypeNotFoundError()

    def validateResultType(self, resType):
        if resType in self.resultSchema:
            return resType
        raise TypeNotFoundError()
        
    def validateFileType(self, fileType):
        if fileType in self.fileTypes:
            return fileType
        raise TypeNotFoundError()
        
    def getResultSchema(self, resType):
        schema = self.resultSchema[resType]
        return schema
        
    def pathFromRepo(self, abspath):
        # get a path that starts at the top of the repo
        pathToRepo = os.path.dirname( self.abspath)
        abspath = os.path.abspath(abspath)
        assert abspath.index( pathToRepo) == 0
        pathFromRepo = abspath[ len(pathToRepo)+1:]
        return pathFromRepo

    def toAbspath(self, repopath):
        pathToRepo = os.path.dirname( self.abspath)
        abspath = os.path.join( pathToRepo, repopath)
        return abspath

    @staticmethod
    def loadRepo(startDir='.', recurse=True):
        startPath = os.path.abspath( startDir)
        if Repo.repoDirName in os.listdir( startPath):
            repoPath = os.path.join( startPath, Repo.repoDirName)
            return Repo(repoPath)
        up = os.path.dirname(abspath)
        if up == startPath:
            raise NoRepoFoundError()
        if recurse:
            return loadRepo(startDir=up)
        else:
            raise NoRepoFoundError()
        
    @staticmethod
    def initRepo(targetDir='.'):
        try:
            p = os.path.abspath(targetDir)
            p = os.path.join(p, Repo.repoDirName)
            os.makedirs( p)
            newRepo = Repo.loadRepo(startDir=targetDir, recurse=False)
            return newRepo
        except FileExistsError:
            pass
        raise RepoAlreadyExistsError()


    def __enter__(self):
        self.readOnly = False
        return self
    
    def __exit__(self, *args):
        self.close()
            
