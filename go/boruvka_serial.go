package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Graph struct {
	m_v        int
	m_edges    [][3]int
	m_component map[int]int
}

func (g *Graph) findComponent(u int) int {
	if g.m_component[u] == u {
		return u
	}
	return g.findComponent(g.m_component[u])
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
	if componentSize[u] <= componentSize[v] {
		g.m_component[u] = v
		componentSize[v] += componentSize[u]
		g.setComponent(u)
	} else if componentSize[u] >= componentSize[v] {
		g.m_component[v] = g.findComponent(u)
		componentSize[u] += componentSize[v]
		g.setComponent(v)
	}
}

func (g *Graph) boruvka() {
	componentSize := make([]int, g.m_v)
	var mstWeight int

	minimumWeightEdge := make([][]int, g.m_v)
	for i := range minimumWeightEdge {
		minimumWeightEdge[i] = []int{-1, -1, -1}
	}

	for node := 0; node < g.m_v; node++ {
		g.m_component[node] = node
		componentSize[node] = 1
	}

	numOfComponents := g.m_v

	for numOfComponents > 2 {
		for _, edge := range g.m_edges {
			u, v, w := edge[0], edge[1], edge[2]
			uComponent, vComponent := g.m_component[u], g.m_component[v]

			if uComponent != vComponent {
				if minimumWeightEdge[uComponent][0] == -1 || minimumWeightEdge[uComponent][2] > w {
					minimumWeightEdge[uComponent] = []int{u, v, w}
				}
				if minimumWeightEdge[vComponent][0] == -1 || minimumWeightEdge[vComponent][2] > w {
					minimumWeightEdge[vComponent] = []int{u, v, w}
				}
			}
		}

		for node := 0; node < g.m_v; node++ {
			if minimumWeightEdge[node][0] != -1 {
				u, v, w := minimumWeightEdge[node][0], minimumWeightEdge[node][1], minimumWeightEdge[node][2]
				uComponent, vComponent := g.m_component[u], g.m_component[v]

				if uComponent != vComponent {
					mstWeight += w
					g.union(componentSize, uComponent, vComponent)
					numOfComponents--
				}
			}
		}

		for i := range minimumWeightEdge {
			minimumWeightEdge[i] = []int{-1, -1, -1}
		}
	}

	fmt.Println("Serial MST weight:", mstWeight)
}

func readGraph(filePath string) map[int][]edge {
	adjacencyList := make(map[int][]edge)

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "%") {
			continue
		}
		parts := strings.Split(line, " ")
		u, _ := strconv.Atoi(parts[0])
		v, _ := strconv.Atoi(parts[1])
		w, _ := strconv.Atoi(parts[2])
		adjacencyList[u] = append(adjacencyList[u], edge{v, w})
		adjacencyList[v] = append(adjacencyList[v], edge{u, w})
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return adjacencyList
}

type edge struct {
	v, w int
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <mtx_file>")
		return
	}

	filePath := os.Args[1]
	adjacencyList := readGraph(filePath)

	graph := &Graph{
		m_v:        len(adjacencyList) + 1,
		m_component: make(map[int]int),
	}

	for u, neighbors := range adjacencyList {
		for _, neighbor := range neighbors {
			graph.m_edges = append(graph.m_edges, [3]int{u, neighbor.v, neighbor.w})
		}
	}

	graph.boruvka()
}