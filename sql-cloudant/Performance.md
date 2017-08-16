## Performance

### Database load tests for _changes endpoint

Load tests were performed using:
1. JAR built against java-cloudant dependency branch
2. JAR built against master branch

Both jars had Spark Streaming batch interval set to 5 seconds.  The batch interval is in number of seconds and holds
data before creating RDD.  
See [Spark Guide: Setting the Right Batch Interval](https://spark.apache.org/docs/latest/streaming-programming-guide.html#setting-the-right-batch-interval) 
for tuning this option.

#### On DSX

Database Size | JAR used | Spark Streaming Storage Level | Time Elapsed (mins:secs)
--- | --- | --- | ---
1 G | java-cloudant | `MEM_ONLY` | 1:01 to 1:20
1 G | master        | `MEM_ONLY` | 1:11 to 1:23
5 G | java-cloudant | `MEMORY_SR` | 4:11 to 5:25
5 G | master        | `MEMORY_SR` | 3:53 to 5:35
10 G | java-cloudant | `MEMORY_SR` | 6:33 to 8:46
10 G | master        | `MEMORY_SR` | 6:53 to 8:49
15 G | java-cloudant | `MEMORY_SR` | 9:39 to 10:40
15 G | master        | `MEMORY_SR` | 9:08 to 10:09
25 G | java-cloudant | `MEMORY_SR` | 17:00 to 18:00
25 G | master        | `MEMORY_SR` | 18:30 to 19:30

`MEM_ONLY`: Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level.

`MEM_ONLY_SER`: Store RDD as serialized Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a fast serializer, but more CPU-intensive to read.

Note: `MEM_ONLY` is the default Spark Streaming storage level in Bahir `sql-cloudant`.
