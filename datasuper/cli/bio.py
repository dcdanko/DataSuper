from .cli import main
import click
from os.path import basename
from datasuper import *


@main.group()
def bio():
    pass


###############################################################################


def groupFastqs(fastqs, forwardSuffix, reverseSuffix):
    grouped = {}
    for fastqf in fastqs:
        fastqBase = basename(fastqf)
        if forwardSuffix in fastqf:
            root = fastqBase.split(forwardSuffix)[0]
            try:
                grouped[root][0] = fastqf
            except KeyError:
                grouped[root] = [fastqf, None]
        elif reverseSuffix in fastqf:
            root = fastqBase.split(reverseSuffix)[0]
            try:
                grouped[root][1] = fastqf
            except KeyError:
                grouped[root] = [None, fastqf]
        else:
            continue
    return grouped


@bio.command(name='add-single-fastqs')
@click.option('-U', '--suffix', default='.fastq.gz')
@click.option('-n', '--name-prefix', default='')
@click.argument('sample_type')
@click.argument('fastqs', nargs=-1)
def addSingleFastqs(suffix, name_prefix, sample_type, fastqs):
    with Repo.loadRepo() as repo:
        for fq in fastqs:
            root = fq.split('/')[-1].split(suffix)[0]
            print('{}: {}'.format(root, fq))
            fname = name_prefix + root + '_single'
            frec = getOrMakeFile(repo, fname, fq, 'gz_fastq')
            res = getOrMakeResult(repo,
                                  name_prefix + root + '_rsrds',
                                  'raw_short_read_dna_single',
                                  {'reads': frec})
            sample = getOrMakeSample(repo,
                                     name_prefix + root,
                                     sample_type)
            sample.addResult(res)
            sample.save(modify=True)


@bio.command(name='add-fastqs')
@click.option('-d', '--delim', default=None)
@click.option('-1', '--forward-suffix', default='_1.fastq.gz')
@click.option('-2', '--reverse-suffix', default='_2.fastq.gz')
@click.option('-n', '--name-prefix', default='')
@click.argument('sample_type')
@click.argument('fastqs', nargs=-1)
def addFastqs(delim, forward_suffix, reverse_suffix,
              name_prefix, sample_type, fastqs):
    groups = groupFastqs(fastqs, forward_suffix, reverse_suffix)
    with Repo.loadRepo() as repo:
        for root, (fq1, fq2) in groups.items():
            if delim:
                root = root.split(delim)[0]
            print('{}: {}, {}'.format(root, fq1, fq2))
            if (fq1 is None) or (fq2 is None):
                continue
            fname1 = name_prefix + root + '_1'
            fr1 = getOrMakeFile(repo, fname1, fq1, 'gz_fastq')
            fname2 = name_prefix + root + '_2'
            fr2 = getOrMakeFile(repo, fname2, fq2, 'gz_fastq')
            res = getOrMakeResult(repo,
                                  name_prefix + root + '_rsrd',
                                  'raw_short_read_dna',
                                  {'read1': fr1, 'read2': fr2})
            sample = getOrMakeSample(repo,
                                     name_prefix + root,
                                     sample_type)
            sample.addResult(res)
            sample.save(modify=True)
