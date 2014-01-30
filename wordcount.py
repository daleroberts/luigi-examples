import luigi


class InputText(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('/tmp/words')
        

class WordCount(luigi.Task):

    def requires(self):
        return [InputText()] 

    def output(self):
        return luigi.LocalTarget('/tmp/count')

    def run(self):
        count = {}
        for file in self.input(): # The input() method is a wrapper around requires() that returns Target objects
            for line in file.open('r'): # Target objects are a file system/format abstraction and this will return a file stream object
                for word in line.strip().split():
                    count[word] = count.get(word, 0) + 1

        # output data
        f = self.output().open('w')
        for word, count in count.iteritems():
            f.write("%s\t%d\n" % (word, count))
            
        # Note that this is essential because file system
        # operations are atomic
        f.close() 
        

if __name__ == '__main__':
    luigi.run(main_task_cls=WordCount)
