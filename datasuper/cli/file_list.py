
from .cli import main
import click
from datasuper import *


@main.command('list')
@click.option('-s/-n', '--sample-names/--no-sample-names', default=False)
@click.argument('operands', nargs=-1)
def file_list(sample_names, operands):
    operands = [tuple(operand.split('.')) for operand in operands]
    repo = Repo.loadRepo()
    for sample in repo.db.sampleTable.getAll():
        line = f'{sample.name}\t' if sample_names else ''
        for result_type, file_type in operands:
            for result in sample.results(resultTypes=[result_type]):
                for my_file_type, file_rec in result.files():
                    if my_file_type == file_type:
                        line += f'{file_rec.filepath()}\t'
        print(line)

