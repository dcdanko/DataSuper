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
################################################################################

@main.group()
def add():
    pass

@add.command(name='group')
@click.argument('name', nargs=1)
@click.argument('samples', nargs=-1)
def addGroup( name, samples):
    with Repo.loadRepo() as repo:
        sg = SampleGroupRecord(repo, name=name)
        sg.save()

@add.command(name='sample')
@click.argument('name', nargs=1)
@click.argument('sample_type', default=None, nargs=1)
def addSample( name, sample_type):
    with Repo.loadRepo() as repo:
        sample = SampleRecord(repo, name=name, sample_type=sample_type)
        sample.save()

@add.command(name='samples-to-group')
@click.argument('group_name', nargs=1)
@click.argument('sample_names', nargs=-1)
def addSamplesToGroup(group_name, sample_names):
    with Repo.loadRepo() as repo:
        group = repo.db.sampleGroupTable.get(group_name)
        for sampleName in sample_names:
            group.addSample(sampleName)
        group.save(modify=True)

        
@add.group()
def type():
    pass

@type.command(name='sample')
@click.argument('type_name', nargs=1)
def addSampleType(type_name):
    with Repo.loadRepo() as repo:
        repo.addSampleType(type_name)

        
        
################################################################################

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

@view.command(name='sample-types')
def viewSampleTypes():
    repo = Repo.loadRepo()
    for st in repo.getSampleTypes():
        print(st)

################################################################################

@main.group()
def tree():
    pass

@tree.command(name='groups')
def treeGroups():
    repo = Repo.loadRepo()
    for sg in repo.db.sampleGroupTable.getAll():
        sys.stdout.write(sg.tree())
    
        
    
################################################################################
    
if __name__ == '__main__':
    main()
