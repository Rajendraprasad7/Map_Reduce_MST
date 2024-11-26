package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type IterationDetail struct {
	IterationNumber   int     `json:"iteration_number"`
	MapTime           float64 `json:"map_time"`
	ReduceTime        float64 `json:"reduce_time"`
	PreComponentCount int     `json:"pre_component_count"`
	EdgesAdded        int     `json:"edges_added"`
	PartialMstWeight  int     `json:"partial_mst_weight"`
}

type OutputJSON struct {
	InputFile        string            `json:"input_file"`
	FileReadTime     float64           `json:"file_read_time"`
	TotalTime        float64           `json:"total_time"`
	TotalIterations  int               `json:"total_iterations"`
	TotalMapTime     float64           `json:"total_map_time"`
	TotalReduceTime  float64           `json:"total_reduce_time"`
	MstWeight        int               `json:"mst_weight"`
	FinalComponents  int               `json:"final_components"`
	IterationDetails []IterationDetail `json:"iteration_details"`
}

type Graph struct {
	m_v         int
	m_edges     [][3]int
	m_component map[int]int
}

type edge struct {
	v, w int
}

func (g *Graph) findComponent(u int) int {
	if g.m_component[u] == u {
		return u
	}
	g.m_component[u] = g.findComponent(g.m_component[u])
	return g.m_component[u]
}

func (g *Graph) setComponent(u int) {
	if g.m_component[u] == u {
		return
	}
	for k := range g.m_component {
		g.m_component[k] = g.findComponent(k)
	}
}

func (g *Graph) union(componentSize []int, u, v int) {
	rootU := g.findComponent(u)
	rootV := g.findComponent(v)

	if rootU == rootV {
		return
	}

	if componentSize[rootU] <= componentSize[rootV] {
		g.m_component[rootU] = rootV
		componentSize[rootV] += componentSize[rootU]
	} else {
		g.m_component[rootV] = rootU
		componentSize[rootU] += componentSize[rootV]
	}
}

func (g *Graph) boruvka(inputFile string) OutputJSON {
	totalStartTime := time.Now()

	outputJSON := OutputJSON{
		InputFile:        inputFile,
		IterationDetails: []IterationDetail{},
	}

	startFileRead := time.Now()
	componentSize := make([]int, g.m_v)
	var mstWeight int
	outputJSON.FileReadTime = time.Since(startFileRead).Seconds()

	minimumWeightEdge := make([][]int, g.m_v)
	for i := range minimumWeightEdge {
		minimumWeightEdge[i] = []int{-1, -1, -1}
	}

	for node := 0; node < g.m_v; node++ {
		g.m_component[node] = node
		componentSize[node] = 1
	}

	numOfComponents := g.m_v
	iteration := 1

	totalMapTime := 0.0
	totalReduceTime := 0.0

	for numOfComponents > 1 {
		startMapTime := time.Now()
		preComponentCount := numOfComponents
		edgesAdded := 0

		for _, edge := range g.m_edges {
			u, v, w := edge[0], edge[1], edge[2]
			uComponent := g.findComponent(u)
			vComponent := g.findComponent(v)

			if uComponent != vComponent {
				if minimumWeightEdge[uComponent][0] == -1 || minimumWeightEdge[uComponent][2] > w {
					minimumWeightEdge[uComponent] = []int{u, v, w}
				}
				if minimumWeightEdge[vComponent][0] == -1 || minimumWeightEdge[vComponent][2] > w {
					minimumWeightEdge[vComponent] = []int{u, v, w}
				}
			}
		}
		mapTime := time.Since(startMapTime).Seconds()
		totalMapTime += mapTime

		startReduceTime := time.Now()
		for node := 0; node < g.m_v; node++ {
			if minimumWeightEdge[node][0] != -1 {
				u, v, w := minimumWeightEdge[node][0], minimumWeightEdge[node][1], minimumWeightEdge[node][2]
				uComponent := g.findComponent(u)
				vComponent := g.findComponent(v)

				if uComponent != vComponent {
					mstWeight += w
					g.union(componentSize, uComponent, vComponent)
					numOfComponents--
					edgesAdded++
				}
			}
		}
		reduceTime := time.Since(startReduceTime).Seconds()
		totalReduceTime += reduceTime

		outputJSON.IterationDetails = append(outputJSON.IterationDetails, IterationDetail{
			IterationNumber:   iteration,
			MapTime:           mapTime,
			ReduceTime:        reduceTime,
			PreComponentCount: preComponentCount,
			EdgesAdded:        edgesAdded,
			PartialMstWeight:  mstWeight,
		})

		for i := range minimumWeightEdge {
			minimumWeightEdge[i] = []int{-1, -1, -1}
		}
		if(edgesAdded == 0) {
			break
		}
		iteration++
	}

	totalTime := time.Since(totalStartTime).Seconds()

	outputJSON.TotalTime = totalTime
	outputJSON.TotalIterations = iteration - 1
	outputJSON.TotalMapTime = totalMapTime
	outputJSON.TotalReduceTime = totalReduceTime
	outputJSON.MstWeight = mstWeight
	outputJSON.FinalComponents = numOfComponents

	fmt.Println("Serial MST weight:", mstWeight)

	return outputJSON
}

func readGraph(filePath string) map[int][]edge {
	adjacencyList := make(map[int][]edge)

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	
	// Skip header lines
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "%") {
			break
		}
	}

	// Read matrix data
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 2 {
			continue
		}

		u, err1 := strconv.Atoi(parts[0])
		v, err2 := strconv.Atoi(parts[1])
		
		if err1 != nil || err2 != nil {
			continue
		}

		w := 1
		if len(parts) > 2 {
			w, _ = strconv.Atoi(parts[2])
		}

		// Ensure non-negative weight
		if w < 0 {
			w = -w
		}

		adjacencyList[u] = append(adjacencyList[u], edge{v, w})
		adjacencyList[v] = append(adjacencyList[v], edge{u, w})
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return adjacencyList
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <mtx_file>")
		return
	}

	filePath := os.Args[1]
	adjacencyList := readGraph(filePath)

	graph := &Graph{
		m_v:         len(adjacencyList),
		m_component: make(map[int]int),
	}

	for u, neighbors := range adjacencyList {
		for _, neighbor := range neighbors {
			graph.m_edges = append(graph.m_edges, [3]int{u, neighbor.v, neighbor.w})
		}
	}

	output := graph.boruvka(filePath)

	outputFile := strings.Replace(filePath, ".mtx", "_serial.json", 1)
	jsonData, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	err = os.WriteFile(outputFile, jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing JSON file:", err)
		return
	}

	fmt.Println("Output JSON file created:", outputFile)
}