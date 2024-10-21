from mrjob.job import MRJob
from mrjob.step import MRStep

from dsu import DisjointSetUnion

class Boruvka(MRJob):
    def configure_args(self):
        super(Boruvka, self).configure_args()
        self.add_passthru_arg('--adjacency_list', type=str, help='Adjacency list')
        self.add_passthru_arg('--dsu_rank', type=str, help='DSU rank')
        self.add_passthru_arg('--dsu_parent', type=str, help='DSU parent')

    def __init__(self, *args, **kwargs):
        super(Boruvka, self).__init__(*args, **kwargs)
        self.adjacency_list = eval(self.options.adjacency_list)
        self.dsu_rank = eval(self.options.dsu_rank)
        self.dsu_parent = eval(self.options.dsu_parent)
        self.dsu = DisjointSetUnion(len(self.adjacency_list)+1)
        self.dsu.rank = self.dsu_rank
        self.dsu.parent = self.dsu_parent

    def mapper(self, _, line):
        for vertex in self.adjacency_list:
            for edge in self.adjacency_list[vertex]:
                u, (v, w) = vertex, edge
                root_u = self.dsu.find(u)
                root_v = self.dsu.find(v)
                if root_u != root_v:
                    yield self.dsu.find(u), (u, v, w) 

    def reducer(self, key, values):
        min_edge = min(values, key=lambda x: x[2])
        u, v, w = min_edge
        yield key, (u, v, w)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

