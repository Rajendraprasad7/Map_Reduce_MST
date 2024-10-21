from mrjob.job import MRJob
from mrjob.step import MRStep

class MTXReader(MRJob):
    def mapper(self, _, line):
        if line.startswith('%'):
            return
        u, v, w = map(int, line.split())
        yield (u, v), w
        yield (v, u), w  

    def reducer(self, key, values):
        u, v = key
        w = min(values)  # In case of multiple edges, take the minimum weight
        yield u, (v, w)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]