from mrjob.job import MRJob
from mrjob.step import MRStep

from collections import defaultdict
from dsu import DisjointSetUnion
from mtx_reader import MTXReader
from boruvka import Boruvka

import sys 

def read_graph(file_path):
    adjacency_list = defaultdict(list)
    mr_job = MTXReader(args=[file_path])
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            v, w = value
            adjacency_list[key].append((v, w))
    return adjacency_list

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Usage: python main.py <mtx_file>")
        sys.exit(1)    

    adjacency_list = read_graph(sys.argv[1])
    dsu = DisjointSetUnion(len(adjacency_list)+1)
    adjacency_list_str = str({key: adjacency_list[key] for key in adjacency_list})

    mst_edges = []

    while len(mst_edges) < len(adjacency_list) - 2:
        current_mst_edges = []
        boruvka_job = Boruvka(args=["graphs/1line.txt", '--adjacency_list', adjacency_list_str, '--dsu_rank', str(dsu.rank), '--dsu_parent', str(dsu.parent)])
        with boruvka_job.make_runner() as runner:
            runner.run()
            for key, value in boruvka_job.parse_output(runner.cat_output()):
                u, v, w = value
                if dsu.find(u) != dsu.find(v):
                    dsu.union(u, v)
                    current_mst_edges.append((u, v, w))
        mst_edges.extend(current_mst_edges)
    #     print(current_mst_edges)
    # print(mst_edges)


    mst_weight = sum([w for _, _, w in mst_edges])
    print(mst_weight)