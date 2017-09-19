import os
from datasuper.persistent import *

class Repo:

    dbRoot = 'datasuper.tinydb.json'
    resultSchemaRoot = 'result-schemas.yml'
    fileTypesRoot = 'file-types.yml'
    sampleTypesRoot = 'sample-types.yml'
    
    def __init__(self, abspath):
        self.abspath = abspath
        
        dbPath = os.path.join(self.abspath, dbRoot)
        self.db = Database.loadDatabase(dbPath)

        resultSchemaPath = os.path.join(self.abspath, resultSchemaRoot)
        self.resultSchema = PersistentDict( resultSchemaPath)

        fileTypesPath = os.path.join(self.abspath, fileTypesRoot)
        self.resultSchema = PersistentSet( fileTypesPath)

        sampleTypesPath = os.path.join(self.abspath, sampleTypesRoot)
        self.resultSchema = PersistentDict( sampleTypesPath)


        
