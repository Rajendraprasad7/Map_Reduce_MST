package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spicavigo/gomr"
)

type edge struct {
	V, W int
}

type Graph struct {
	AdjacencyList map[int][]edge
	DSU           *DisjointSetUnion
}

type DisjointSetUnion struct {
	Parent []int
	Rank   []int
}

func NewDisjointSetUnion(n int) *DisjointSetUnion {
	parent := make([]int, n)
	rank := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	return &DisjointSetUnion{
		Parent: parent,
		Rank:   rank,
	}
}

func (dsu *DisjointSetUnion) Find(u int) int {
	if dsu.Parent[u] != u {
		dsu.Parent[u] = dsu.Find(dsu.Parent[u])
	}
	return dsu.Parent[u]
}

func (dsu *DisjointSetUnion) Union(u, v int) {
	rootU, rootV := dsu.Find(u), dsu.Find(v)
	if rootU != rootV {
		if dsu.Rank[rootU] > dsu.Rank[rootV] {
			dsu.Parent[rootV] = rootU
		} else if dsu.Rank[rootU] < dsu.Rank[rootV] {
			dsu.Parent[rootU] = rootV
		} else {
			dsu.Parent[rootV] = rootU
			dsu.Rank[rootU]++
		}
	}
}

type BoruvkaJob struct {
	gomr.Job
	Graph *Graph
}

func (j *BoruvkaJob) Configure() {
	j.AddFlags()
	j.AddFlag("adjacency_list", "string", "Adjacency list", "")
	j.AddFlag("dsu_rank", "string", "DSU rank", "")
	j.AddFlag("dsu_parent", "string", "DSU parent", "")
}

func (j *BoruvkaJob) Setup() {
	j.Graph = &Graph{
		AdjacencyList: make(map[int][]edge),
	}

	adjacencyList := make(map[int][]edge)
	json.Unmarshal([]byte(j.GetFlag("adjacency_list")), &adjacencyList)
	j.Graph.AdjacencyList = adjacencyList

	dsuRank := make([]int, len(adjacencyList)+1)
	json.Unmarshal([]byte(j.GetFlag("dsu_rank")), &dsuRank)
	dsuParent := make([]int, len(adjacencyList)+1)
	json.Unmarshal([]byte(j.GetFlag("dsu_parent")), &dsuParent)
	j.Graph.DSU = &DisjointSetUnion{
		Parent: dsuParent,
		Rank:   dsuRank,
	}
}

func (j *BoruvkaJob) Map(key, value gomr.Pair) ([]gomr.Pair, error) {
	var pairs []gomr.Pair
	for vertex, neighbors := range j.Graph.AdjacencyList {
		for _, neighbor := range neighbors {
			u, v, w := vertex, neighbor.V, neighbor.W
			rootU, rootV := j.Graph.DSU.Find(u), j.Graph.DSU.Find(v)
			if rootU != rootV {
				pairs = append(pairs, gomr.Pair{Key: j.Graph.DSU.Find(u), Value: []int{u, v, w}})
			}
		}
	}
	return pairs, nil
}

func (j *BoruvkaJob) Reduce(key gomr.Key, values []gomr.Value) ([]gomr.Pair, error) {
	var pairs []gomr.Pair
	minEdge := []int{-1, -1, 1 << 31 - 1}
	for _, value := range values {
		edge := value.([]int)
		if edge[2] < minEdge[2] {
			minEdge = edge
		}
	}
	pairs = append(pairs, gomr.Pair{Key: key, Value: minEdge})
	return pairs, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <mtx_file>")
		return
	}

	filePath := os.Args[1]

	adjacencyList := make(map[int][]edge)
	file, _ := os.Open(filePath)
	scanner := gomr.NewLineScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line[0] == '%' {
			continue
		}
		parts := strings.Split(line, " ")
		u, _ := strconv.Atoi(parts[0])
		v, _ := strconv.Atoi(parts[1])
		w, _ := strconv.Atoi(parts[2])
		adjacencyList[u] = append(adjacencyList[u], edge{v, w})
		adjacencyList[v] = append(adjacencyList[v], edge{u, w})
	}

	dsu := NewDisjointSetUnion(len(adjacencyList) + 1)
	dsuRank, _ := json.Marshal(dsu.Rank)
	dsuParent, _ := json.Marshal(dsu.Parent)
	adjacencyListJSON, _ := json.Marshal(adjacencyList)

	job := &BoruvkaJob{}
	job.SetFlags(os.Args[1:])
	job.SetFlag("adjacency_list", string(adjacencyListJSON))
	job.SetFlag("dsu_rank", string(dsuRank))
	job.SetFlag("dsu_parent", string(dsuParent))

	result, err := job.Run()
	if err != nil {
		panic(err)
	}

	var mstEdges [][]int
	for _, pair := range result {
		mstEdges = append(mstEdges, pair.Value.([]int))
	}

	sort.Slice(mstEdges, func(i, j int) bool {
		return mstEdges[i][2] < mstEdges[j][2]
	})

	var mstWeight int
	for _, edge := range mstEdges {
		mstWeight += edge[2]
	}

	fmt.Println("MST weight:", mstWeight)
}