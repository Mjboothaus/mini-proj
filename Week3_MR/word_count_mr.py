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
    def __init__(self):
        self.num = 100
        self.h = []

    # Mapper nodes should each have a heap each with a top 100
    def mapper(self, key, value):
        yield nlargest(self.num, (value, key))

    # Reducer class needs to take the top 100 of the set of top 100's
    def reducer(self, top100, _):
        yield nlargest(self.num, top100)


class MRTop100(MRJob):

    # Combine the word count and top N classes

    def steps(self):
        return MRWordCount.steps() + MRTopN.steps()

if __name__ == '__main__':
    MRWordCount.run()