
---

## Requirements

- Ubuntu-based system or VM (tested on Azure)
- Apache Spark (version ≥ 3.5.5)
- Python 3
- `pyspark` installed

To install Spark on Ubuntu:

```bash
sudo apt update
sudo apt install openjdk-11-jdk scala wget -y
wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar xvf spark-3.5.0-bin-hadoop3.tgz
mv spark-3.5.0-bin-hadoop3 ~/spark
echo 'export SPARK_HOME=~/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```
Input format: weighted_graph.txt
```php-template
<num_nodes> <num_edges>
<source> <destination> <weight>
<source> <destination> <weight> 
...
```

Example:
```
5 6
0 1 7
0 2 3
1 3 9
2 4 4
3 4 6
1 4 2
```

## Run
Ideally would like to have at least 32BG RAM and 4vCPUs
```bash
spark-submit --driver-memory 16G --executor-memory 16G --conf spark.executor.cores=4 rddVersion.py
```

## Output
Reuslts will be displayed in results.txt file
```yaml
Shortest distances from node 0:
Node 0: 0
Node 1: 7
Node 2: 3
Node 3: 16
Node 4: 7
```
Unreachable Nodes should display like:
```yaml
Node 7: INF
```
