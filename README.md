# Borůvka's Algorithm for Minimum Spanning Tree using MapReduce (`mrjob`)

This project implements **Borůvka’s algorithm** for computing the **Minimum Spanning Tree (MST)** of a graph using **MapReduce**, facilitated via the [`mrjob`](https://mrjob.readthedocs.io/) Python library. This approach is especially suitable for large-scale distributed environments.

---

## 📌 Overview

Borůvka’s algorithm is an efficient greedy algorithm for MST computation. At each iteration, the algorithm:
1. Finds the lightest outgoing edge from each connected component.
2. Merges the connected components using these lightest edges.
3. Repeats this process until all nodes are part of a single component.

This implementation simulates this iterative process using **MapReduce**, where each Map step identifies candidate edges and each Reduce step selects the lightest one.

---

## 📁 File Structure

- `boruvka_mr.py`: The main MapReduce job using `mrjob`.
- `dsu.py`: A helper module that implements the **Disjoint Set Union (Union-Find)** data structure, used to keep track of components.

---

## ⚙️ Requirements

- Python 3.7+
- [`mrjob`](https://mrjob.readthedocs.io/en/latest/) (`pip install mrjob`)

---

## 📦 Installation

```bash
pip install mrjob
```

---

## 🚀 How to Run

The script expects three arguments:
- `--adjacency_list`: A stringified dictionary of the graph adjacency list.
- `--dsu_rank`: A stringified list of the DSU ranks.
- `--dsu_parent`: A stringified list of the DSU parent pointers.

Each round of Borůvka corresponds to a single MapReduce job. Between iterations, the DSU state must be updated manually to reflect new merged components.

### 🧪 Example Run

```bash
python boruvka_mr.py \
    --adjacency_list="{0: [(1, 4), (2, 1)], 1: [(0, 4), (2, 3)], 2: [(0, 1), (1, 3)]}" \
    --dsu_rank="[0, 0, 0]" \
    --dsu_parent="[0, 1, 2]" \
    -r inline
```

### 📤 Output

Each output line corresponds to the **minimum edge chosen** from each component in the current round.

Example output:
```
0	(0, 2, 1)
1	(1, 2, 3)
```

This means node `0` selected edge `(0, 2, 1)` and node `1` selected `(1, 2, 3)` for MST construction.

---

## 🧠 How It Works

### Mapper:
For each vertex and its adjacency list:
- It finds the component representatives of both endpoints using DSU.
- If the endpoints are in different components, it yields a candidate edge.

### Reducer:
- For each component, selects the **minimum-weight outgoing edge** to merge with another component.

---

## 🔄 Iterative Use

Borůvka’s algorithm is inherently iterative. This script runs **a single phase** of the algorithm. To complete the MST, you must:
1. Run this job.
2. Use the resulting edges to merge DSU components.
3. Update the DSU state (`parent`, `rank`) and re-run until all nodes are in one component.

---

## 📚 References

- Borůvka, O. (1926). “O jistém problému minimálním” (On a Certain Minimal Problem), *Práce Mor. Prírodoved. Spol. v Brne* III (3): 37–58.
- [Wikipedia: Borůvka's Algorithm](https://en.wikipedia.org/wiki/Bor%C5%AFvka%27s_algorithm)
- [`mrjob` documentation](https://mrjob.readthedocs.io/)

---

## 🧑‍💻 Author

**Rajendraprasad Saravanan**  
GitHub: [@Rajendraprasad7](https://github.com/Rajendraprasad7)

---

## 📜 License

This project is licensed under the MIT License. See the `LICENSE` file for details.
