import luigi
import random
import subprocess


class Software(luigi.Task):
    """A software dependency"""

    version_cmd = luigi.Parameter()

    def requires(self):
        return []
    
    def run(self):
        cmd = str(self.version_cmd)
        output = subprocess.check_output(cmd, shell=True)
        with self.output().open('w') as outfile:
            outfile.write(output)

    def output(self):
        cmd = str(self.version_cmd).split()[0]
        return luigi.LocalTarget('/tmp/versions/%s' % cmd)


class InputNumbers(luigi.ExternalTask):
    
    def requires(self):
        return [Software('git --version'), Software('uname -a')]

    def run(self):
        with self.output().open('w') as outfile:
            for i in (random.randrange(1, 100) for x in range(1000)):
                print >> outfile, str(i)
        
    def output(self):
        return luigi.LocalTarget('/tmp/numbers')

class NumberCount(luigi.Task):

    def requires(self):
        return [InputNumbers()] 

    def output(self):
        return luigi.LocalTarget('/tmp/numcount')

    def run(self):
        count = {}
        for file in self.input():
            for line in file.open('r'):
                for word in line.strip().split():
                    count[word] = count.get(word, 0) + 1

        f = self.output().open('w')
        for word, count in count.iteritems():
            f.write("%s\t%d\n" % (word, count))
        f.close()

if __name__ == '__main__':
    luigi.run(main_task_cls=NumberCount)
