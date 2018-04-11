from datasuper.database import *


def makeSample(repo, name, sampleType, modify=False):
    '''Make a sample with the given properties.

    Do not change an existing sample unless modify is true.

    Args:
        repo (datasuper.repo): The repo where the result will be made.
        name (str): The name of the result, used to find existing results.
        sampleType (str): The type of the sample, that must already exist
            in the repo.
        modify (:obj:`bool`, optional): Flag to modify an existing result
            with `name`. Defaults to False.

    Returns:
        The sample if successful, None otherwise.

    Raises:
        RecordExistsError: If name exists in the repo and `modify` is False.
        RepoReadOnlyError: If the repo is in read only mode.

    '''

    sample = SampleRecord(repo,
                          name=name,
                          sample_type=sampleType)
    sample = sample.save(modify=modify)
    return sample


def getOrMakeSample(repo, name, sampleType):
    '''Get a sample with the given name or make one then return the sample.'''
    try:
        sample = repo.sampleTable.get(name)
    except KeyError:
        sample = makeSample(repo, name, sampleType)
    return sample


def makeFile(repo, name, filepath, filetype, modify=False):
    '''Make a file record with the given properties.

    Do not change an existing file record unless modify is true.

    Args:
        repo (datasuper.repo): The repo where the result will be made.
        name (str): The name of the result, used to find existing results.
        filepath (str): The path to the file.
        fileype (str): The type of the file, that must already exist
            in the repo.
        modify (:obj:`bool`, optional): Flag to modify an existing result
            with `name`. Defaults to False.

    Returns:
        The file record if successful, None otherwise.

    Raises:
        RecordExistsError: If name exists in the repo and `modify` is False.
        RepoReadOnlyError: If the repo is in read only mode.

    '''
    fr = FileRecord(repo,
                    name=name,
                    filepath=filepath,
                    file_type=filetype)
    fr.save(modify=modify)
    return fr


def getOrMakeFile(repo, name, filepath, filetype):
    '''Get a file with the name and path or make one then return the file.'''
    try:
        fr = repo.fileTable.get(name)
    except KeyError:
        fr = makeFile(repo, name, filepath, filetype)
    assert fr.detailedStatus()
    return fr


def makeResult(repo, name, resultType, fileRecs, modify=False):
    '''Make a result with the given properties.

    Do not change an existing result unless modify is true.

    Args:
        repo (datasuper.repo): The repo where the result will be made.
        name (str): The name of the result, used to find existing results.
        resultType (str): The type of the result, that must already exist
            in the repo.
        fileRecs: A list of name-strings, primary-key-strings or file
            record objects.
        modify (:obj:`bool`, optional): Flag to modify an existing result
            with `name`. Defaults to False.

    Returns:
        The result if successful, None otherwise.

    Raises:
        RecordExistsError: If name exists in the repo and `modify` is False.
        RepoReadOnlyError: If the repo is in read only mode.

    '''
    result = ResultRecord(repo,
                          name=name,
                          result_type=resultType,
                          file_records=fileRecs)
    result = result.save(modify=modify)
    return result


def getOrMakeResult(repo, name, resultType, fileRecs):
    '''Get a result with the given name or make one then return the result.'''
    try:
        result = repo.resultTable.get(name)
    except KeyError:
        result = makeResult(repo, name, resultType, fileRecs)
    return result
