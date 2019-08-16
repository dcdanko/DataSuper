
from .cli import main
import click
from datasuper import *


@main.group('list')
@click.option('-s/-n', '--sample-names/--no-sample-names', default=False)
@click.argument('operands', nargs=-1)
def file_list(sample_names, operands):
    operands = [tuple(operand.split('.')) for operand in operands]
    repo = Repo.loadRepo()
    for sample in repo.db.sampleTable.getAll():
        line = f'{sample.name}' if sample_names else ''
        for result_type, file_type in operands:
            for result in sample.results(resultTypes=[result_type]):
                for file_rec in result.files():
                    if file_rec.fileType == file_type:
                        line += f'{file_rec.filepath()}\t'
        print(line)

