package main

import (
	"encoding/json"
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)


type DisjointSetUnion struct {
	parent []int
	rank   []int
}


func NewDSU(n int) *DisjointSetUnion {
	parent := make([]int, n)
	rank := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	return &DisjointSetUnion{parent, rank}
}

func (dsu *DisjointSetUnion) Find(u int) int {
	if dsu.parent[u] != u {
		dsu.parent[u] = dsu.Find(dsu.parent[u])
	}
	return dsu.parent[u]
}

func (dsu *DisjointSetUnion) Union(u, v int) {
	rootU := dsu.Find(u)
	rootV := dsu.Find(v)

	if rootU != rootV {
		if dsu.rank[rootU] > dsu.rank[rootV] {
			dsu.parent[rootV] = rootU
		} else if dsu.rank[rootU] < dsu.rank[rootV] {
			dsu.parent[rootU] = rootV
		} else {
			dsu.parent[rootV] = rootU
			dsu.rank[rootU]++
		}
	}
}

type Edge struct {
	u, v, w int
}

type MapReduceFramework struct {
	numWorkers int
	dsu        *DisjointSetUnion
}

func NewMapReduceFramework(numWorkers int, dsu *DisjointSetUnion) *MapReduceFramework {
	return &MapReduceFramework{
		numWorkers: numWorkers,
		dsu:        dsu,
	}
}

func (mr *MapReduceFramework) Map(adjList map[int][]Edge) map[int][]Edge {
	intermediateResults := make(map[int][]Edge)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for vertex, edges := range adjList {
		wg.Add(1)
		go func(v int, es []Edge) {
			defer wg.Add(-1)
			for _, edge := range es {
				rootU := mr.dsu.Find(v)
				rootV := mr.dsu.Find(edge.v)
				if rootU != rootV {
					mu.Lock()
					intermediateResults[rootU] = append(intermediateResults[rootU], Edge{v, edge.v, edge.w})
					mu.Unlock()
				}
			}
		}(vertex, edges)
	}
	wg.Wait()
	return intermediateResults
}

func (mr *MapReduceFramework) Reduce(mappedData map[int][]Edge) []Edge {
	var result []Edge
	var mu sync.Mutex
	var wg sync.WaitGroup

	edgeSet := make(map[Edge]struct{})

	for key, edges := range mappedData {
		wg.Add(1)
		go func(k int, es []Edge) {
			defer wg.Done()
			if len(es) == 0 {
				return
			}
			minEdge := es[0]
			for _, edge := range es[1:] {
				if edge.w < minEdge.w {
					minEdge = edge
				}
			}
			mu.Lock()
			if _, exists := edgeSet[minEdge]; !exists {
				edgeSet[minEdge] = struct{}{}
				result = append(result, minEdge)
			}
			mu.Unlock()
		}(key, edges)
	}
	wg.Wait()
	return result
}

func readMTXFile(filename string) (map[int][]Edge, int) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	const (
		numWorkers = 4
		chunkSize  = 1000
	)

	type MTXLine struct {
		u, v, w int
		valid   bool
	}
	mappedLines := make(chan MTXLine, numWorkers*chunkSize)

	var chunks [][]string
	var currentChunk []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		currentChunk = append(currentChunk, scanner.Text())
		if len(currentChunk) == chunkSize {
			chunks = append(chunks, currentChunk)
			currentChunk = []string{}
		}
	}
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	var wg sync.WaitGroup
	for _, chunk := range chunks {
		wg.Add(1)
		go func(lines []string) {
			defer wg.Done()
			for _, line := range lines {
				if strings.HasPrefix(line, "%") || len(strings.TrimSpace(line)) == 0 {
					continue
				}

				fields := strings.Fields(line)
				if len(fields) < 2 {
					continue
				}

				u, err1 := strconv.Atoi(fields[0])
				v, err2 := strconv.Atoi(fields[1])
				w := 1
				if len(fields) == 3 {
					w, _ = strconv.Atoi(fields[2])
				}
				if w < 0 {
					w = -w
				}

				if err1 == nil && err2 == nil {
					mappedLines <- MTXLine{u, v, w, true}
					mappedLines <- MTXLine{v, u, w, true} 
				}
			}
		}(chunk)
	}

	go func() {
		wg.Wait()
		close(mappedLines)
	}()

	adjList := make(map[int][]Edge)
	maxVertex := 0
	var mu sync.Mutex

	for line := range mappedLines {
		if !line.valid {
			continue
		}

		mu.Lock()
		if line.u > maxVertex {
			maxVertex = line.u
		}
		if line.v > maxVertex {
			maxVertex = line.v
		}

		adjList[line.u] = append(adjList[line.u], Edge{line.u, line.v, line.w})
		adjList[line.v] = append(adjList[line.v], Edge{line.v, line.u, line.w})
		mu.Unlock()
	}

	return adjList, maxVertex
}


func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run boruvka.go <mtx_file>")
		os.Exit(1)
	}

	totalStartTime := time.Now()

	debugOutput := struct {
		InputFile         string `json:"input_file"`
		FileReadTime      float64 `json:"file_read_time"`
		Iterations        int `json:"total_iterations"`
		TotalMapTime      float64 `json:"total_map_time"`
		TotalReduceTime   float64 `json:"total_reduce_time"`
		TotalTime         float64 `json:"total_time"`
		MSTWeight         int `json:"mst_weight"`
		FinalComponents   int `json:"final_components"`
		IterationDetails  []struct {
			IterationNumber     int     `json:"iteration_number"`
			MapTime            float64 `json:"map_time"`
			ReduceTime         float64 `json:"reduce_time"`
			PreComponentCount  int     `json:"pre_component_count"`
			EdgesAdded         int     `json:"edges_added"`
			PartialMSTWeight   int     `json:"partial_mst_weight"`
		} `json:"iteration_details"`
	}{
		InputFile: os.Args[1],
	}

	startRead := time.Now()
	adjList, maxVertex := readMTXFile(os.Args[1])
	readTime := time.Since(startRead)
	debugOutput.FileReadTime = readTime.Seconds()
	fmt.Printf("File Read Time: %f\n", readTime.Seconds())

	dsu := NewDSU(maxVertex + 1)
	
	mr := NewMapReduceFramework(4, dsu) 
	
	mstEdges := make([]Edge, 0)
	
	iteration := 0
	var totalMapTime, totalReduceTime time.Duration
	
	for len(mstEdges) < len(adjList)-1 {
		iteration++
		fmt.Printf("\nIteration %d:\n", iteration)

		startMap := time.Now()
		mappedData := mr.Map(adjList)
		mapTime := time.Since(startMap)
		totalMapTime += mapTime
		fmt.Printf("Map Phase Time: %f\n", mapTime.Seconds())

		startReduce := time.Now()
		currentMSTEdges := mr.Reduce(mappedData)
		reduceTime := time.Since(startReduce)
		totalReduceTime += reduceTime
		fmt.Printf("Reduce Phase Time: %f\n", reduceTime.Seconds())

		componentCount := make(map[int]bool)
		for v := range adjList {
			componentCount[dsu.Find(v)] = true
		}
		preComponentCount := len(componentCount)
		fmt.Printf("Connected Components before iteration: %d\n", preComponentCount)

		addedEdges := 0
		for _, edge := range currentMSTEdges {
			if dsu.Find(edge.u) != dsu.Find(edge.v) {
				dsu.Union(edge.u, edge.v)
				mstEdges = append(mstEdges, edge)
				addedEdges++
			}
		}
		fmt.Printf("Edges added in this iteration: %d\n", addedEdges)

		partialMSTWeight := 0
		for _, edge := range mstEdges {
			partialMSTWeight += edge.w
		}
		fmt.Printf("Partial MST Weight in iteration %d: %d\n", iteration, partialMSTWeight)

		debugOutput.IterationDetails = append(debugOutput.IterationDetails, struct {
			IterationNumber     int     `json:"iteration_number"`
			MapTime            float64 `json:"map_time"`
			ReduceTime         float64 `json:"reduce_time"`
			PreComponentCount  int     `json:"pre_component_count"`
			EdgesAdded         int     `json:"edges_added"`
			PartialMSTWeight   int     `json:"partial_mst_weight"`
		}{
			IterationNumber:    iteration,
			MapTime:           mapTime.Seconds(),
			ReduceTime:        reduceTime.Seconds(),
			PreComponentCount: preComponentCount,
			EdgesAdded:        addedEdges,
			PartialMSTWeight:  partialMSTWeight,
		})

		if len(currentMSTEdges) == 0 {
			break 
		}
	}

	mstWeight := 0
	for _, edge := range mstEdges {
		mstWeight += edge.w
	}
	
	fmt.Printf("\nFinal MST Statistics:\n")
	fmt.Printf("Total Iterations: %d\n", iteration)
	fmt.Printf("Total Map Time: %f\n", totalMapTime.Seconds())
	fmt.Printf("Total Reduce Time: %f\n", totalReduceTime.Seconds())
	fmt.Printf("Total MST Weight: %d\n", mstWeight)

	componentCount := make(map[int]bool)
	for v := range adjList {
		componentCount[dsu.Find(v)] = true
	}
	finalComponents := len(componentCount)
	fmt.Printf("Final Connected Components: %d\n", finalComponents)

	totalTime := time.Since(totalStartTime)

	debugOutput.Iterations = iteration
	debugOutput.TotalMapTime = totalMapTime.Seconds()
	debugOutput.TotalReduceTime = totalReduceTime.Seconds()
	debugOutput.MSTWeight = mstWeight
	debugOutput.FinalComponents = finalComponents
	debugOutput.TotalTime = totalTime.Seconds()

	inputFilename := os.Args[1]
	outputFilename := fmt.Sprintf("%s_mr.json", strings.TrimSuffix(inputFilename, filepath.Ext(inputFilename)))

	outputFile, err := os.Create(outputFilename)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(debugOutput)
	if err != nil {
		fmt.Printf("Error writing JSON: %v\n", err)
		return
	}

	fmt.Printf("\nDebug data written to %s\n", outputFilename)
	fmt.Printf("Total Execution Time: %f\n", totalTime.Seconds())
}