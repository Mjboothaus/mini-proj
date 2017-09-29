import re

#from mrjob.compat import get_jobconf_value
from mrjob.job import MRJob

from heapq import heappush, heappop, nlargest


WORD_RE = re.compile(r"\w+")

# TODO: Can use something like
# num = get_jobconf_value("my.job.settings.num")
# to get command line argument e.g. for num-ber of elements to put in top list

class MRWordCount(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))


# Second job takes a collection of pairs (word, count) and filter for
# only the highest N e.g. 100


class MRTopN(MRJob):
    def mapper_init(self):
        self.num = 100
        self.heap = []

    # Mapper nodes should each have a heap each with a top 100
    def mapper(self, word, count):
        for (key, value) in (word, count):
            heappush(self.heap, (key, value))
            topN = nlargest(self.num, self.heap)
        yield (topN.pop(), None)

    #def mapper_final(self, topN, None):
    #    yield  here


    # Reducer class needs to take the top 100 of the set of top 100's
    def reducer(self, topN, _):
        yield nlargest(self.num, topN)


class MRTop100(MRJob):

    # Combine the word count and top N classes

    def steps(self):
        return MRWordCount.steps() + MRTopN.steps()

if __name__ == '__main__':
    MRWordCount.run()