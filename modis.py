import os
import re
import luigi
import numpy as np
from osgeo import gdal

TILE = '/g/data/v27/water_smr/confidence/modis/tiles/MODIS_%s.tif'


def rm_inputs_afterwards(cls):
    """
    Class decorator to remove input files after a run.
    """
    def decorate(fn):
        def rm_inputs_after_run(self):
            ret = fn(self)
            for f in self.input():
                os.unlink(f.path)
            return ret
        return rm_inputs_after_run
    cls.run = decorate(cls.run)
    return cls


def parse_lat_lon(filename, pattern=re.compile(r'([-+]?\d+|\d+)_([-+]?\d+|\d+)')):
    m = pattern.search(filename)
    return (int(m.group(1)), int(m.group(2)))


class ImageTarget(luigi.LocalTarget):

    def open(self, mode='r'):
        if mode == 'w':
            # Create folder if it does not exist
            normpath = os.path.normpath(self.path)
            parentfolder = os.path.dirname(normpath)
            if parentfolder and not os.path.exists(parentfolder):
                os.makedirs(parentfolder)
            return gdal.Open(self.path, gdal.GA_Update)
        elif mode == 'r':
            return gdal.Open(self.path, gdal.GA_ReadOnly)
        else:
            raise Exception('mode must be r/w')


class WorkUnit(luigi.ExternalTask):

    filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filename)


class ModisTile(luigi.ExternalTask):

    lat = luigi.IntParameter()
    lon = luigi.IntParameter()

    def output(self):
        loc = '%i_%+04i' % (self.lat, self.lon)
        return ImageTarget(TILE % loc)

    def run(self):
        pass


class ModisPixelThresholdCount(luigi.Task):

    lat = luigi.IntParameter()
    lon = luigi.IntParameter()
    value = luigi.FloatParameter()

    def output(self):
        cell = '%i_%+04i' % (self.lat, self.lon)
        return luigi.LocalTarget('data/thresh_%s_%s.txt' % (cell, self.value))

    def requires(self):
        return [ModisTile(self.lat, self.lon)]

    def run(self):
        count = 0
        for target in self.input():
            image = target.open()
            data = image.GetRasterBand(1).ReadAsArray()
            count = np.count_nonzero(data > self.value)

        with self.output().open('w') as outfile:
            outfile.write('{}\n'.format(count))

@rm_inputs_afterwards
class AggregateCount(luigi.Task):

    todo_filename = luigi.Parameter()
    value = luigi.FloatParameter()

    def output(self):
        return luigi.LocalTarget('thresh_%s.txt' % self.value)

    def requires(self):
        with WorkUnit(self.todo_filename).output().open('r') as infile:
            filenames = infile.readlines()
            tiles = [parse_lat_lon(filename) for filename in filenames]
            return [ModisPixelThresholdCount(*tile, value=self.value) for tile
                    in tiles]

    def run(self):
        total = 0
        for input in self.input():
            with input.open('r') as infile:
                line = infile.readlines()[0]
                total += int(line.strip())

        with self.output().open('w') as outfile:
            outfile.write('{}'.format(total))


if __name__ == '__main__':
    import sys
    luigi.build([
            AggregateCount(todo_filename='tiles.txt',
                           value=70)
        ],
        workers=1,
        scheduler_host=sys.argv[1])
