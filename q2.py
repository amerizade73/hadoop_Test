from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class Q2(MRJob):

    def steps(self):
            return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer),
                MRStep(reducer = self.secondreducer)
                ]

    def mapper(self, _, line):
        line = line.strip().translate(str.maketrans('', '', string.punctuation))
        words = line.lower().split()
        for word in words:
            yield word, 1

    def reducer(self, word, counts):
        yield None, (sum(counts),word)

    def secondreducer(self, _, wordAndCounts):
        type(wordAndCounts)
        i = 0
        for count, key in sorted(wordAndCounts, reverse=True):
            if i == 10:
                break
            yield (count, key)
            i+=1

if __name__ == '__main__':
    Q2.run()