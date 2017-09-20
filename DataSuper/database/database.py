from tinydb import TinyDB
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware

class Database:

    fileTblName = 'file_record_table'
    resultTblName = 'result_record_table'
    sampleTblName = 'sample_record_table'
    sampleGroupTblName = 'sample_group_record_tbl'
    
    def __init__(self, repo, tinydbDB):
        self.repo = repo
        self.db = db
        self.fileTbl = DatabaseTable( self.repo,
                                      FileRecord,
                                      self.db.table(fileTblName))
        self.resultTbl = DatabaseTable( self.repo,
                                        ResultRecord,
                                        self.db.table(resultTblName))
        self.sampleTbl = DatabaseTable( self.repo,
                                        SampleRecord,
                                        self.db.table(sampleTblName))
        self.sampleGroupTbl = DatabaseTable( self.repo,
                                             SampleGroupRecord,
                                             self.db.table(sampleGroupTblName))
        
    def getTable( recType):
        if recType == FileRecord:
            return self.fileTbl
        elif recType == ResultRecord:
            return self.resultTbl
        elif recType == SampleRecord:
            return self.sampleTbl
        elif recType == SampleGroupRecord:
            return self.sampleGroupTbl

    def close(self):
        self.db.close()

    @staticmethod
    def loadDatabase(repo, path):
        tinydbDB = TinyDB(self.repoPath, storage=CachingMiddleware(JSONStorage))
        return Database(repo, tinydbDB)
