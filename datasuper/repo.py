import os.path
from datasuper.persistent import *
from datasuper.database import *

class NoRepoFoundError( Exception):
    pass

class RepoAlreadyExistsError( Exception):
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
        self.fileTypes = PersistentSet( fileTypesPath)

        sampleTypesPath = os.path.join(self.abspath, Repo.sampleTypesRoot)
        self.sampleTypes = PersistentSet( sampleTypesPath)


    def close(self):
        self.db.close()
        
    def addSampleType( self, sampleType):
        pass

    def getSampleTypes( self):
        pass

    def addFileType( self, fileType):
        pass

    def addResultSchema( self, resultType, resultSchema):
        pass
        
        
    def validateSampleType(self, sampleType):
        if sampleType in self.sampleTypes:
            return sampleType
        # throw error

    def validateResultType(self, resType):
        if resType in self.resultSchema:
            return resType
        # throw error
        
    def validateFileType(self, fileType):
        if fileType in self.fileTypes:
            return fileType
        # throw error        
        
    def getResultSchema(self, resType):
        schema = self.resultSchema[resType]
        
    def pathFromRepo(self, abspath):
        # get a path that starts at the top of the repo
        pass

    @staticmethod
    def loadRepo(startDir='.'):
        startPath = os.path.abspath( startDir)
        if Repo.repoDirName in os.listdir( startPath):
            repoPath = os.path.join( startPath, Repo.repoDirName)
            return Repo(repoPath)
        up = os.path.dirname(abspath)
        if up == startPath:
            raise NoRepoFoundError()
        return loadRepo(startDir=up)
                
    @staticmethod
    def initRepo(targetDir='.'):
        try:
            os.makedirs( Repo.repoDirName)
            newRepo = Repo.loadRepo(startDir=targetDir)
            assert newRepo.abspath == os.path.abspath(targetDir)
            return newRepo
        except FileExistsError:
            pass
        raise RepoAlreadyExistsError()


    def __enter__(self):
        self.readOnly = False
        return self
    
    def __exit__(self, *args):
        self.close()
            
