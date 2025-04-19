from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

# Initialize Spark session
spark = SparkSession.builder.appName("Dijkstra").getOrCreate()

# Read the file as a DataFrame
df = spark.read.text("weighted_graph.txt")

# Split each line into an array of values
df_split = df.withColumn("columns", split(col("value"), " "))

# Extract the number of nodes and edges
first_row = df_split.limit(1).collect()[0]["columns"]
num_nodes = int(first_row[0])
num_edges = int(first_row[1])

# Process the edges
df_edges = df_split.withColumn("source", col("columns")[0].cast("int")) \
                   .withColumn("destination", col("columns")[1].cast("int")) \
                   .withColumn("weight", col("columns")[2].cast("int")) \
                   .drop("columns", "value") \
                   .filter(col("weight").isNotNull())  # Exclude first row

# Collect the edges into a list
edges = df_edges.collect()


# Graph class for Dijkstra's algorithm
class Graph:
    def __init__(self, vertices):
        self.V = vertices
        self.graph = [[0 for _ in range(vertices)] for _ in range(vertices)]

    def add_edge(self, src, dest, weight):
        self.graph[src][dest] = weight
        self.graph[dest][src] = weight  # If the graph is undirected

    def printSolution(self, dist):
        print("Shortest distances from node 0:")
        for node in range(self.V):
            if dist[node] == sys.maxsize:
                print(f"Node {node}: INF")
            else:
                print(f"Node {node}: {dist[node]}")


    def minDistance(self, dist, sptSet):
        min_val = sys.maxsize
        min_index = -1
        for u in range(self.V):
            if dist[u] < min_val and not sptSet[u]:
                min_val = dist[u]
                min_index = u
        return min_index

    def dijkstra(self, src):
        dist = [sys.maxsize] * self.V
        dist[src] = 0
        sptSet = [False] * self.V

        for _ in range(self.V):
            x = self.minDistance(dist, sptSet)
            sptSet[x] = True
            for y in range(self.V):
                if self.graph[x][y] > 0 and not sptSet[y] and dist[y] > dist[x] + self.graph[x][y]:
                    dist[y] = dist[x] + self.graph[x][y]

        self.printSolution(dist)


# Initialize graph with nodes
g = Graph(num_nodes)

# Add edges
for row in edges:
    src, dest, weight = row["source"], row["destination"], row["weight"]
    g.add_edge(src, dest, weight)

# Run Dijkstra from source node 0
g.dijkstra(0)
