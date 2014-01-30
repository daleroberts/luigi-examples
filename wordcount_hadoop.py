import luigi, luigi.hadoop, luigi.hdfs

class InputText(luigi.ExternalTask):
    def output(self):
        return luigi.hdfs.HdfsTarget('/tmp/words')

class WordCount(luigi.hadoop.JobTask):

    def requires(self):
        return [InputText()]

    def output(self):
        return luigi.hdfs.HdfsTarget('/tmp/count')

    def mapper(self, line):
        for word in line.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    luigi.run(main_task_cls=WordCount)
