from pyspark import SparkContext


sc = SparkContext(appName="ParallelDijkstra")


lines = sc.textFile("weighted_graph.txt")

# Read header for number of nodes/edges
header = lines.first()
num_nodes, num_edges = map(int, header.strip().split())


edges = lines.filter(lambda line: line != header) \
             .map(lambda line: tuple(map(int, line.strip().split()))) \
             .map(lambda x: (x[0], (x[1], x[2])))


adj_list = edges.groupByKey().mapValues(list).cache()


source_node = 0
#(node, (distance, path))
distances = sc.parallelize([(i, (0 if i == source_node else float('inf'), [])) for i in range(num_nodes)])

#update distances
for i in range(num_nodes - 1):
    print(f"Iteration {i+1}/{num_nodes - 1}")

    #join distances with adjacency list
    joined = distances.join(adj_list)

    #flatMap to compute potential new distances
    new_distances = joined.flatMap(lambda x: [
        (neighbor, (x[1][0][0] + weight, list(x[1][0][1]) + [x[0]]))
        for (neighbor, weight) in x[1][1]
    ])

    #union old and new distances, keep shortest
    distances = distances.union(new_distances) \
                         .reduceByKey(lambda a, b: a if a[0] < b[0] else b) \
                         .cache()

    distances.count()  # Force evaluation to persist/cache

#save results
results = distances.collect()

#write to results.txt
with open("results.txt", "w") as f:
    f.write(f"\nShortest distances from node {source_node}:\n")
    for node, (dist, path) in sorted(results, key=lambda x: x[0]):
        if dist == float('inf'):
            f.write(f"Node {node}: INF\n")
        else:
            f.write(f"Node {node}: {dist}\n")

#finish run
sc.stop()
