from mrjob.job import MRJob
import re
#from mrjob.compat import get_jobconf_value
from heapq import heappush, heappop


WORD_RE = re.compile(r"\w+")


class MRWordCount(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))


# Second job takes a collection of pairs (word, count) and filter for only the highest N e.g. 100

class MRTopN(MRJob):

    def __init__(self):
        #num = get_jobconf_value("my.job.settings.num")
        self.num = 100
        self.h = []
        return self

    def mapper(self, key, value):
        heappush(self.h, (value, key))
        return self

    def reducer(self):
        i = 0
        while i < self.num:
            yield heappop(self.h)

class MRTop100(MRJob):

    # Combine the word count and top N classes

    def steps(self):
        return MRWordCount.steps() + MRTopN.steps()


if __name__ == '__main__':
    MRWordCount.run()