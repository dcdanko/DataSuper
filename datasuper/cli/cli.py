import click
from datasuper import *
from datasuper.database import *
import sys
from json import dumps as jdumps

from ..version import __version__


@click.group()
@click.version_option(__version__)
def main():
    pass


@main.command()
def init():
    '''Create a repo in the current directory.'''
    try:
        Repo.initRepo()
    except RepoAlreadyExistsError:
        print('Repo already exists.', file=sys.stderr)


@main.command()
def id():
    '''Get the repo id of the current repo.'''
    repo = Repo.loadRepo()
    print(repo.repoId())

###############################################################################


def tableStatus(tbl, tblName):
    '''Check if records in a table are valid and print a report to stdout.'''
    ngrps = tbl.size()
    sys.stdout.write('\n{} {}... '.format(ngrps, tblName))
    allGood = True
    for name, (status, msg) in tbl.checkStatus().items():
        if not status:
            allGood = False
            sys.stdout.write(f'\n{name} failed: {msg}')
    if allGood:
        sys.stdout.write('all good.')


@main.command()
def status():
    '''Check if all records are valid and print a report to stdout.'''
    repo = Repo.loadRepo()
    sys.stdout.write('Checking status')
    tableStatus(repo.db.sampleGroupTable, 'sample groups')
    tableStatus(repo.db.sampleTable, 'samples')
    tableStatus(repo.db.resultTable, 'results')
    tableStatus(repo.db.fileTable, 'files')
    sys.stdout.write('\nDone\n')


###############################################################################


@main.group()
def add():
    pass


@add.command(name='group')
@click.argument('name', nargs=1)
@click.argument('samples', nargs=-1)
def addGroup(name, samples):
    '''Add a sample group to the repo.'''
    with Repo.loadRepo() as repo:
        sg = SampleGroupRecord(repo, name=name)
        sg.save()


@add.command(name='sample')
@click.argument('name', nargs=1)
@click.argument('sample_type', default=None, nargs=1)
def addSample(name, sample_type):
    '''Add a sample to the repo.'''
    with Repo.loadRepo() as repo:
        sample = SampleRecord(repo, name=name, sample_type=sample_type)
        sample.save()


@add.command(name='file')
@click.argument('name', nargs=1)
@click.argument('filepath', nargs=1)
@click.argument('file_type', default=None, nargs=1)
def addFile(name, filepath, file_type):
    '''Add a file record to the repo.'''
    with Repo.loadRepo() as repo:
        sample = FileRecord(repo,
                            name=name,
                            filepath=filepath,
                            file_type=file_type)
        sample.save()


@add.command(name='result')
@click.argument('name', nargs=1)
@click.argument('result_type', nargs=1)
@click.argument('fields', nargs=-1)
def addResult(name, result_type, fields):
    '''Add a result records to the repo.'''
    if ':' in fields[0]:
        fileRecs = {}
        for field in fields:
            k, v = field.split(':')
            fileRecs[k] = v
    else:
        fileRecs = fields

    with Repo.loadRepo() as repo:
        result = ResultRecord(repo,
                              name=name,
                              result_type=result_type,
                              file_records=fileRecs)
        result.save()


@add.command(name='samples-to-group')
@click.argument('group_name', nargs=1)
@click.argument('sample_names', nargs=-1)
def addSamplesToGroup(group_name, sample_names):
    '''Add sample records to a particular group.'''
    with Repo.loadRepo() as repo:
        group = repo.db.sampleGroupTable.get(group_name)
        for sampleName in sample_names:
            group.addSample(sampleName)
        group.save(modify=True)


@add.command(name='results-to-group')
@click.argument('group_name', nargs=1)
@click.argument('result_names', nargs=-1)
def addResultsToGroup(group_name, result_names):
    '''Add result records to a particular group.'''
    with Repo.loadRepo() as repo:
        group = repo.db.sampleGroupTable.get(group_name)
        for resultName in result_names:
            group.addResult(resultName)
        group.save(modify=True)


@add.command(name='results-to-sample')
@click.argument('sample_name', nargs=1)
@click.argument('result_names', nargs=-1)
def addResultsToSample(sample_name, result_names):
    '''Add result records to a particular sample.'''
    with Repo.loadRepo() as repo:
        sample = repo.db.sampleTable.get(sample_name)
        for resultName in result_names:
            sample.addResult(resultName)
        sample.save(modify=True)

###############################################################################


@add.group()
def type():
    pass


@type.command(name='sample')
@click.argument('type_names', nargs=-1)
def addSampleTypes(type_names):
    '''Add several sample types to the repo.'''
    with Repo.loadRepo() as repo:
        for typeName in type_names:
            repo.addSampleType(typeName)


@type.command(name='file')
@click.argument('type_names', nargs=-1)
def addFileTypes(type_names):
    '''Add several file types to the repo.'''
    with Repo.loadRepo() as repo:
        for typeName in type_names:
            repo.addFileType(typeName)


@type.command(name='result')
@click.argument('type_name', nargs=1)
@click.argument('fields', nargs=-1)
def addResultSchema(type_name, fields):
    '''Add a result type and schema to the repo.'''
    processedFields = []
    fieldList = True
    if ':' in fields[0]:
        fieldList = False
        processedFields = {}
    for field in fields:
        if not fieldList:
            name, fileType = field.split(':')
            processedFields[name] = fileType
        else:
            processedFields.append(field)
    with Repo.loadRepo() as repo:
        repo.addResultSchema(type_name, processedFields)


###############################################################################

@main.group()
def view():
    pass


@view.command(name='groups')
def viewGroups():
    '''Print a list of all sample groups to the screen.'''
    repo = Repo.loadRepo()
    for sg in repo.db.sampleGroupTable.getAll():
        print(sg.name)


@view.command(name='samples')
def viewSamples():
    '''Print a list of all samples to the screen.'''
    repo = Repo.loadRepo()
    for sample in repo.db.sampleTable.getAll():
        print(sample)


@view.command(name='files')
def viewFiles():
    '''Print a list of all file records to the screen.'''
    repo = Repo.loadRepo()
    for fileRec in repo.db.fileTable.getAll():
        print(fileRec)


@view.command(name='results')
def viewResults():
    '''Print a list of all results to the screen.'''
    repo = Repo.loadRepo()
    for result in repo.db.resultTable.getAll():
        print(result)


@view.command(name='sample-types')
def viewSampleTypes():
    '''Print a list of all sample types to the screen.'''
    repo = Repo.loadRepo()
    for st in repo.getSampleTypes():
        print(st)


@view.command(name='file-types')
def viewFileTypes():
    '''Print a list of all file types to the screen.'''
    print('name\text')
    repo = Repo.loadRepo()
    for ft in repo.getFileTypes():
        print('{}\t{}'.format(ft['name'], ft['ext']))


@view.command(name='result-types')
def viewResultSchema():
    '''Print a list of all result types and schemas to the screen.'''
    repo = Repo.loadRepo()
    for rt in repo.getResultTypes():
        schema = repo.getResultSchema(rt)
        sout = '{} '.format(rt)
        try:
            for k, v in schema.items():
                sout += '{}:{} '.format(k, v)
        except AttributeError:
            sout += ' '.join(schema)
        print(sout)

###############################################################################


@main.group()
def rename():
    pass


@rename.command(name='sample')
@click.argument('old_name')
@click.argument('new_name')
def renameSample(old_name, new_name):
    """Change the human readable name of a sample."""
    def switch(val):
        return new_name.join(val.split(old_name))

    with Repo.loadRepo() as repo:
        sample = repo.db.sampleTable.get(old_name)
        print(f'{old_name} -> {new_name}', file=sys.stderr)
        sample.rename(new_name)
        old_file_paths = []
        for result in sample.results():
            new_result_name = switch(result.name)
            result.rename(new_result_name)
            for _, filerec in result.files():
                new_file_name = switch(filerec.name)
                filerec.rename(new_file_name)
                old_file_paths.append(filerec.filepath())
                new_file_path = switch(filerec.filepath())
                filerec.copy(new_file_path)
                filerec.save(modify=True)
            result.save(modify=True)
        sample.save(modify=True)

    for old_path in old_file_paths:
        print(old_path)


###############################################################################


@main.group()
def tree():
    pass


@tree.command(name='groups')
def treeGroups():
    '''Print a tree diagram of all records starting at the group level.'''
    repo = Repo.loadRepo()
    for sg in repo.db.sampleGroupTable.getAll():
        sys.stdout.write(sg.tree())


@tree.command(name='samples')
def treeSamples():
    '''Print a tree diagram of records starting at the sample level.'''
    repo = Repo.loadRepo()
    for s in repo.db.sampleTable.getAll():
        sys.stdout.write(s.tree())


###############################################################################


@main.group()
def detail():
    pass


@detail.command(name='sample')
@click.argument('name')
def detailSample(name):
    repo = Repo.loadRepo()
    try:
        sample = repo.db.sampleTable.get(name)
        print(sample)
    except InvalidRecordStateError:
        print('Failed to build sample.')
    rawSample = repo.db.sampleTable.getRaw(name)
    print(jdumps(rawSample, sort_keys=True, indent=4))

    for resPK in rawSample['results']:
        try:
            resName = repo.db.asName(resPK)
            print('{} {}'.format(resPK, resName))
        except KeyError:
            print('Could not find name for {}'.format(resPK))


###############################################################################


@main.group()
def repair():
    pass


@repair.command(name='samples')
def repairSamples():
    '''
    Repairs sample records by removing links to 'bad' results

    'bad' results are results that either do not exist or throw
    an InvalidRecordStateError

    Does not remove any result records, just delinks them from
    samples

    Checks all samples by default
    '''
    with Repo.loadRepo() as repo:
        for rawRec in repo.db.sampleTable.getAllRaw():
            keyExists = []
            for rawResultPK in rawRec['results']:
                try:
                    resultPK = repo.db.asPK(rawResultPK)
                    keyExists.append(resultPK)
                except KeyError:
                    pass
            keyIsGood = []

            for resultPK in keyExists:
                try:
                    res = repo.db.resultTable.get(resultPK)
                    if res.validStatus():
                        keyIsGood.append(resultPK)
                except InvalidRecordStateError:
                    pass

            if len(rawRec['results']) == len(keyIsGood):
                print('No change for sample {}'.format(rawRec['name']))
                continue
            rawRec['results'] = keyIsGood
            try:
                samp = repo.db.sampleTable.typeStored(repo, **rawRec)
                try:
                    samp.save(modify=True)
                    print('Fixed sample {}'.format(samp.name))
                except InvalidRecordStateError:
                    print('Failed to save sample {}'.format(rawRec['name']))
                    raise
            except InvalidRecordStateError:
                print('Failed to fix sample {}'.format(rawRec['name']))
                raise

###############################################################################


@main.group()
def remove():
    pass


@remove.command(name='invalids')
@click.option('--confirmed/--not-confirmed', default=False)
def removeInvalids(confirmed):
    '''Remove all records that are not valid. Won't run if not confirmed.'''
    with Repo.loadRepo() as repo:
        toRemove = {}
        toRemove['groups'] = repo.db.sampleGroupTable.getInvalids()
        toRemove['samples'] = repo.db.sampleTable.getInvalids()
        toRemove['results'] = repo.db.resultTable.getInvalids()
        toRemove['files'] = repo.db.fileTable.getInvalids()
        for k, val in toRemove.items():
            print('Removing {} {} objects'.format(len(val), k),
                  file=sys.stderr)
        if not confirmed:
            print('Remove not confirmed. Aborting.', file=sys.stderr)
            return
        repo.db.sampleGroupTable.removeInvalids()
        repo.db.sampleTable.removeInvalids()
        repo.db.resultTable.removeInvalids()
        repo.db.fileTable.removeInvalids()


@remove.command(name='results')
@click.argument('result_names', nargs=-1)
def removeResults(result_names):
    '''Remove the given results.'''
    with Repo.loadRepo() as repo:
        for result in repo.db.resultTable.getMany(result_names):
            result.remove()


@remove.command(name='results-by-type')
@click.argument('result_types', nargs=-1)
def removeResultsOfType(result_types):
    '''Remove the given results.'''
    result_types = set(result_types)
    with Repo.loadRepo() as repo:
        for result in repo.db.resultTable.getAll():
            if result.resultType() in result_types:
                result.remove()


@remove.command(name='files')
@click.argument('file_names', nargs=-1)
def removeFiles(file_names):
    '''Remove the given files.'''
    with Repo.loadRepo() as repo:
        for fileRec in repo.db.fileTable.getMany(file_names):
            fileRec.remove(atomic=True)


if __name__ == '__main__':
    main()
