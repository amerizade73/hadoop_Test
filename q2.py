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
        stop_words = frozenset(['ourselves', 'hers', 'between', 'yourself', 
        'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 
        'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 
        'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off',
        'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 
        'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his',
        'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 
        'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both',
        'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any',
        'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 
        'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why',
        'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 
        'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i',
        'after', 'few', 'whom', 't', 
        'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it',
        'how', 'further', 'was', 'here', 'than'])

        line = line.strip().translate(str.maketrans('', '', string.punctuation))
        words = line.lower().split()
        words = [w for w in words if not w.lower() in stop_words]
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
