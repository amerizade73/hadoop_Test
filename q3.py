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
        stopwords = ['i', 'me', 'my', 'myself', 'we', 'our',
        'ours', 'ourselves', 'you', "you're", "you've",
        "you'll", "you'd", 'your', 'yours', 'yourself',
        'yourselves', 'he', 'him', 'his', 'himself',
        'she', "she's", 'her', 'hers', 'herself',
        'it', "it's", 'its', 'itself', 'they', 'them',
        'their', 'theirs', 'themselves', 'what', 'which',
        'who', 'whom', 'this', 'that', "that'll", 'these',
        'those', 'am', 'is', 'are', 'was', 'were', 'be',
        'been', 'being', 'have', 'has', 'had', 'having',
        'do', 'does', 'did', 'doing', 'a', 'an', 'the',
        'and', 'but', 'if', 'or', 'because', 'as', 'until',
        'while', 'of', 'at', 'by', 'for', 'with', 'about',
        'against', 'between', 'into', 'through', 'during',
        'before', 'after', 'above', 'below', 'to', 'from',
        'up', 'down', 'in', 'out', 'on', 'off', 'over',
        'under', 'again', 'further', 'then', 'once', 'here',
         'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both',
        'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor',
        'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't',
        'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now',
        'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
        "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn',
        "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't",
        'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't",
        'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]
        
        line = line.strip().translate(str.maketrans('', '', string.punctuation))
        words = line.lower().split()
        for word in words:
            # remove stop words!
            if word not in stopwords:
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
