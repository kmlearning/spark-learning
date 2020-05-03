
#RDD, dataset, dataframe
    # RDD - read only collection of records (java/scala objects)
    # dataframe - data organised into named columns
    # dataset - typesafe object oriented intefrace to dataframe with catalyst optimiser query optimiser

# Performance improvements
    # Use dataframes api instead of RDDs to enable catalyst optimiser to improve execution plan
    # Avoid regex's (slow!)
    # Larger dataset on left in left join (left is kept static in executors and transfers data on the right)
    # Cache dataframes
    # Analyse table with COMPUTE STATISTICS for columns to help optimiser
    # Drop unused columns so less data shuffled/operated on
    
# Encoder
    # serialise-deserialise framework for converting between JVM objects and internal binary format
    

# Create Spark session object (subsumes hive and sql context)
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .appName("My app")\
                    .getOrCreate()

# Read in data
data = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load("../datasets.my.csv")

# Get data schema
data.printSchema()

# Get number of rows
data.count()

# Get top n rows
data.limit(n).show()

# Drop rows with na values
data.dropna()

# Drop column
data = data.drop("COL TO DROP")

# Get unique values
data.select("COLUMN").distinct()

# Filter records
data.filter(data["COLUMN"] == "VALUE")
data.filter(data["COLUMN"].isin(["VAL1", "VAL2"]))
data.filter(data["COLUMN"] >= VALUE)

# Show a fraction of dataset (e.g. 10%)
data.sample(fraction=0.1).show()

# Group by the value of a column
data.groupBy("COLUMN")

# Do an aggregation on grouped values
data.groupBy("COLUMN_TO_GROUP")\
    # Agg by group
    .agg({"COLUMN_TO_SUM":"sum"})|
    # aggs
        # sum
        # min
        # max
    .withColumnRenamed("sum(COLUMN_TO_SUM)", "NEW_NAME")

# Access values of returned datasets
RETURNED = data.etcetc
RETURNED.collect()[0][0] # First cell value

# Add new column as function of others (e.g. one divided by another)
import pyspark.sql.functions as func
data.withColumn(
    "NEW_COL",
    func.round(data.COL1 / VALUE * 100, 2) # etc, example
)

# Show results in order (e.g. first ten desc order of col COLNO_TO_ORDER)
result.orderBy(result[COLNO_TO_ORDER_BY]).desc())\
    .show(10)

# Crosstab - two columns x and y
data.crosstab("COLX", "COLY")\
    .select(COLS_TO_SHOW)\
    .show()



# Closures
    # Variables as input to closure not shared between tasks - copied per task, not per node
    # Lots of work on shuffling
    # No copying from one task to another - all from central master
    # Improve performance using broadcast vars and accumulators
# Broadcast vars
    # Only one read only copy per node (not per task)
    # Peer to peer copies enabled
    # No shuffling
    # In memory 
    # Use whenever tasks across stages need the same data (training data, static lookup tables)
# Accumulators
    # As broadcast but can be added to (additions but must come out to same result)
    # Types: long, double, collections
    # Can extend with accumulators v2
    # Use for things like global counters or sums
    # Workers can only modify state
    # Only the driver can read state

# Create own "user defined function"
from pyspark.sql.functions import udf
my_udf = udf(lambda x: x.split('-')[0])
data = data.withColumn(
    'NEWCOL',
    my_udf(data.COL)
)

# Join dataframes
# inner join
combined = left.join(right, left.col1 == right.col1)
#OR
combined = left.join(right, ['join_col1', 'joincol2'])
# for large dataframes, broadcast smaller df to all nodes
combined = left.select(
        'coltojoin',
        'coltokeep1'
    ).join(
        broadcast(right),
        left.coltojoin == right.coltojoin
    )
)

# Share values across nodes with accumulators (e.g. count categorical variables across dataset)
# Apply to every row
pos = spark.sparkContext.accumulator(0)
zero = spark.sparkContext.accumulator(0)
neg = spark.sparkContext.accumulator(0)

def count_sign(row):
    data_point = float(row.col)

    if data_point > 0:
        pos.add(1)
    elif data_point == 0:
        zero.add(1)
    elif data_point < 0:
        neg.add(1)

result = data.foreach(lambda x: count_sign(x))
