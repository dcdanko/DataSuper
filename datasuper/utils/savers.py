

def saveGroup(repo, name, subgroups=[], samples=[], results=[]):
    pass

def saveSample(repo, name, sampleType, results=[]):
    pass

def addResultToSample(repo, sampleName, result):
    pass

def getResult

def saveResult(repo, resultName, resultType, fileRecords={}, provenance=[], previousResultTypes=[]):
    # convert previousResultTypes into (name, type) pairs then into primary keys

    # convert fileRecords into primary keys

    
    res = ResultRecord(repo,
                       name=resultName,
                       result_type=resultType,
                       file_records=fileRecords,
                       provenance=provenance,
                       previous_results=previousResults)
    res.save()

def saveFile(repo, filepath, fileType):
    pass

