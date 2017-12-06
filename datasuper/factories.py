from datasuper.database import *


def makeSample(repo, name, sampleType, modify=False):
    sample = SampleRecord(repo,
                          name=name,
                          sample_type=sampleType)
    sample = sample.save(modify=modify)
    return sample


def getOrMakeSample(repo, name, sampleType):
    try:
        sample = repo.sampleTable.get(name)
    except KeyError:
        sample = makeSample(repo, name, sampleType)
    return sample


def makeFile(repo, name, filepath, filetype, modify=False):
    fr = FileRecord(repo,
                       name=name,
                       filepath=filepath,
                       file_type=filetype)
    fr.save(modify=modify)
    return fr


def getOrMakeFile(repo, name, filepath, filetype):
    try:
        fr = repo.fileTable.get(name)
    except KeyError:
        fr = makeFile(repo, name, filepath, filetype)
    return fr


def makeResult(repo, name, resultType, fileRecs, modify=False):
    result = ResultRecord(repo,
                          name=name,
                          result_type=resultType,
                          file_records=fileRecs)
    result = result.save(modify=modify)
    return result


def getOrMakeResult(repo, name, resultType, fileRecs):
    try:
        result = repo.resultTable.get(name)
    except KeyError:
        result = makeResult(repo, name, resultType, fileRecs)
    return result
