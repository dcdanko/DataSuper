import click
from datasuper import *
from datasuper.database import *
import sys


@click.group()
def main():
    pass


@main.command()
def init():
    try:
        Repo.initRepo()
    except RepoAlreadyExistsError:
        print('Repo already exists.', file=sys.stderr)

###############################################################################


@main.command()
def status():
    repo = Repo.loadRepo()
    grps = repo.db.sampleGroupTable.getAll()
    sys.stdout.write('{} sample groups... '.format(len(grps)))
    allGood = True
    for grp in grps:
        if not grp.validStatus():
            allGood = False
            sys.stdout.write('\n{} failed'.format(grp.name))
    if allGood:
        sys.stdout.write('all good.')

    samples = repo.db.sampleTable.getAll()
    sys.stdout.write('\n{} samples... '.format(len(samples)))
    allGood = True
    for sample in samples:
        if not sample.validStatus():
            allGood = False
            sys.stdout.write('\n{} failed'.format(sample.name))
    if allGood:
        sys.stdout.write('all good.')

    results = repo.db.resultTable.getAll()
    sys.stdout.write('\n{} results... '.format(len(results)))
    allGood = True
    for result in results:
        if not result.validStatus():
            allGood = False
            sys.stdout.write('\n{} failed'.format(result.name))
    if allGood:
        sys.stdout.write('all good.')

    fileRecs = repo.db.fileTable.getAll()
    sys.stdout.write('\n{} files... '.format(len(fileRecs)))
    allGood = True
    for fileRec in fileRecs:
        if not fileRec.validStatus():
            allGood = False
            sys.stdout.write('\n{} failed'.format(fileRec.name))
    if allGood:
        sys.stdout.write('all good.')
    sys.stdout.write('\n')


###############################################################################


@main.group()
def add():
    pass


@add.command(name='group')
@click.argument('name', nargs=1)
@click.argument('samples', nargs=-1)
def addGroup(name, samples):
    with Repo.loadRepo() as repo:
        sg = SampleGroupRecord(repo, name=name)
        sg.save()


@add.command(name='sample')
@click.argument('name', nargs=1)
@click.argument('sample_type', default=None, nargs=1)
def addSample(name, sample_type):
    with Repo.loadRepo() as repo:
        sample = SampleRecord(repo, name=name, sample_type=sample_type)
        sample.save()


@add.command(name='file')
@click.argument('name', nargs=1)
@click.argument('filepath', nargs=1)
@click.argument('file_type', default=None, nargs=1)
def addFile(name, filepath, file_type):
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
    with Repo.loadRepo() as repo:
        group = repo.db.sampleGroupTable.get(group_name)
        for sampleName in sample_names:
            group.addSample(sampleName)
        group.save(modify=True)


@add.command(name='results-to-group')
@click.argument('group_name', nargs=1)
@click.argument('result_names', nargs=-1)
def addResultsToGroup(group_name, result_names):
    with Repo.loadRepo() as repo:
        group = repo.db.sampleGroupTable.get(group_name)
        for resultName in result_names:
            group.addResult(resultName)
        group.save(modify=True)


@add.command(name='results-to-sample')
@click.argument('sample_name', nargs=1)
@click.argument('result_names', nargs=-1)
def addResultsToSample(sample_name, result_names):
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
    with Repo.loadRepo() as repo:
        for typeName in type_names:
            repo.addSampleType(typeName)


@type.command(name='file')
@click.argument('type_names', nargs=-1)
def addFileTypes(type_names):
    with Repo.loadRepo() as repo:
        for typeName in type_names:
            repo.addFileType(typeName)


@type.command(name='result')
@click.argument('type_name', nargs=1)
@click.argument('fields', nargs=-1)
def addResultSchema(type_name, fields):
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
    repo = Repo.loadRepo()
    for sg in repo.db.sampleGroupTable.getAll():
        print(sg.name)


@view.command(name='samples')
def viewSamples():
    repo = Repo.loadRepo()
    for sample in repo.db.sampleTable.getAll():
        print(sample)


@view.command(name='files')
def viewFiles():
    repo = Repo.loadRepo()
    for fileRec in repo.db.fileTable.getAll():
        print(fileRec)


@view.command(name='results')
def viewResults():
    repo = Repo.loadRepo()
    for result in repo.db.resultTable.getAll():
        print(result)


@view.command(name='sample-types')
def viewSampleTypes():
    repo = Repo.loadRepo()
    for st in repo.getSampleTypes():
        print(st)


@view.command(name='file-types')
def viewFileTypes():
    print('name\text')
    repo = Repo.loadRepo()
    for ft in repo.getFileTypes():
        print('{}\t{}'.format(ft['name'], ft['ext']))


@view.command(name='result-types')
def viewResultSchema():
    repo = Repo.loadRepo()
    for rt in repo.getResultTypes():
        schema = repo.getResultSchema(rt)
        out = '{} '.format(rt)
        try:
            for k, v in schema.items():
                out += '{}:{} '.format(k, v)
        except AttributeError:
            out += ' '.join(schema)
        print(out)


###############################################################################


@main.group()
def tree():
    pass


@tree.command(name='groups')
def treeGroups():
    repo = Repo.loadRepo()
    for sg in repo.db.sampleGroupTable.getAll():
        sys.stdout.write(sg.tree())


@tree.command(name='samples')
def treeSamples():
    repo = Repo.loadRepo()
    for s in repo.db.sampleTable.getAll():
        sys.stdout.write(s.tree())


###############################################################################


@main.group()
def remove():
    pass


@remove.command(name='results')
@click.argument('result_names', nargs=-1)
def removeResults(result_names):
    with Repo.loadRepo() as repo:
        for result in repo.db.resultTable.getMany(result_names):
            result.remove()


if __name__ == '__main__':
    main()
