import tempfile
import logging
import luigi
import time
import random
import os

from datetime import datetime
from os.path import join as pjoin, dirname, exists, basename, abspath

CONFIG = luigi.configuration.get_config()
CONFIG.add_config_path(pjoin(dirname(__file__), 'loadconfig.cfg'))

# Set global config
TEMPDIR = tempfile.mkdtemp()
ACQ_MIN = datetime(*map(int, CONFIG.get('data', 'acq_min').split('-')))
ACQ_MAX = datetime(*map(int, CONFIG.get('data', 'acq_max').split('-')))

logging.basicConfig()
log = logging.getLogger()

class DummyTask(luigi.Task):
    id = luigi.Parameter()

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(TEMPDIR, str(self.id)))


class SleepyTask(luigi.Task):
    id = luigi.Parameter()

    def run(self):
        time.sleep(random.uniform(0,2))
        f = self.output().open('w')
        f.close()


    def output(self):
        return luigi.LocalTarget(os.path.join(TEMPDIR, str(self.id)))


class ChainedSleepyTask(SleepyTask):
    id = luigi.Parameter()

    def requires(self):
        if int(self.id) > 0:
            return [ChainedSleepyTask(int(self.id)-1)]
        else:
            return []


if __name__ == '__main__':
    log.warning('temp directory: ' + TEMPDIR)
    log.warning('acq_min: ' + str(ACQ_MIN))

    tasks = [DummyTask(id) for id in range(20)]
    tasks.extend([SleepyTask(id) for id in range(20, 30)])
    tasks.extend([ChainedSleepyTask(35)])

    luigi.build(tasks, local_scheduler=True)
