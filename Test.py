from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row

conf = SparkConf().setAppName("Project3").setMaster("local")
try:
    sc.stop()
    sc = SparkContext(conf=conf)
except:
    pass
sc = SparkContext.getOrCreate()
data = sc.textFile("/user/ds503/input/Transactions.csv").cache()
csvRDD = data.map(lambda row: row.split(","))
parsedRDD = csvRDD.map(lambda r: Row(transID=int(r[0]),
    custID=int(r[1]),
    transTotal=float(r[2]),
    transNumItems=int(r[3]),
    transDesc=str(r[4])))
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(parsedRDD)
df.registerTempTable("trans")
# 1. drop transactions whose total < 200
resultOne = sqlContext.sql("""
        SELECT *
        FROM trans
        WHERE transTotal >= 200;
        """).registerTempTable("resultOne")
out = sqlContext.sql("""
        SELECT *
        FROM resultOne;
        """)
out.show()
# 2. group by transNumItems, select transTotal avg, max, min, sum
resultTwo = sqlContext.sql("""
        SELECT transNumItems, SUM(transTotal), AVG(transTotal), MIN(transTotal), MAX(transTotal)
        FROM resultOne
        GROUP BY transNumItems
        ORDER BY transNumItems;
        """)
# 3. report data to user
resultTwo.show()
# 4. over resultOne group by custID, report transNumItems total
resultThree = sqlContext.sql("""
        SELECT custID, SUM(transNumItems) as t3count
        FROM resultOne
        GROUP BY custID
        ORDER BY custID;
        """).registerTempTable("t3")
out = sqlContext.sql("""
        SELECT *
        FROM t3;
        """)
out.show()
# 5. filter out transactions from trans whose transTotal < 600
t4 = sqlContext.sql("""
        SELECT *
        FROM trans
        WHERE transTotal >= 600;
        """).registerTempTable("t4")
out = sqlContext.sql("""
        SELECT *
        FROM t4;
        """)
out.show()
# 6. over 5, group by custID, for each group, report custID and transTotal
t5 = sqlContext.sql("""
        SELECT custID, COUNT(*) as t5count
        FROM t4
        GROUP BY custID
        ORDER BY custID;
        """).registerTempTable("t5")
out = sqlContext.sql("""
        SELECT *
        FROM t5;
        """)
out.show()
# 7. over 6, select custID whose T5.count * 5 < T3.count
t6 = sqlContext.sql("""
        SELECT t5.custID 
        FROM t5
        INNER JOIN t3
        ON t3.custID = t5.custID
        WHERE t5count * 5 < t3count;
        """)
# 8. report to user
t6.show()
