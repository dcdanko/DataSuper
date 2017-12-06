from datasuper.database import *


def getOrMakeSample(repo, name, sampleType):
    try:
        sample = repo.sampleTable.get(name)
    except KeyError:
        sample = SampleRecord(repo,
                              name=name,
                              sample_type=sampleType)
        sample = sample.save()
    return sample


def getOrMakeFile(repo, name, filepath, filetype):
    try:
        fr = repo.fileTable.get(name)
    except KeyError:
        fr = ds.FileRecord(repo,
                           name=name,
                           filepath=filepath,
                           file_type=filetype)
        fr.save()
    return fr


def getOrMakeResult(repo, name, resultType, fileRecs):
    try:
        result = repo.resultTable.get(rname)
    except KeyError:
        result = ds.ResultRecord(repo,
                                 name=name,
                                 result_type=resultType,
                                 file_records=fileRecs)
        result = result.save()
    return result
