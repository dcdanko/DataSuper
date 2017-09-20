import os.path
from datasuper.persistent import *

class Repo:

    dbRoot = 'datasuper.tinydb.json'
    resultSchemaRoot = 'result-schemas.yml'
    fileTypesRoot = 'file-types.yml'
    sampleTypesRoot = 'sample-types.yml'
    
    def __init__(self, abspath):
        self.abspath = abspath
        
        dbPath = os.path.join(self.abspath, dbRoot)
        self.db = Database.loadDatabase(self, dbPath)

        resultSchemaPath = os.path.join(self.abspath, resultSchemaRoot)
        self.resultSchema = PersistentDict( resultSchemaPath)

        fileTypesPath = os.path.join(self.abspath, fileTypesRoot)
        self.fileTypes = PersistentSet( fileTypesPath)

        sampleTypesPath = os.path.join(self.abspath, sampleTypesRoot)
        self.sampleTypes = PersistentSet( sampleTypesPath)


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
        
                    
            
