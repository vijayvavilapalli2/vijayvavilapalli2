
from pyspark.sql import SparkSession
import pyspark
import findspark
findspark.init()
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)
def convertCase(name):
    resStr=''
    char_list=name.split(' ')
    print(char_list)
    for char in char_list:
        resStr=resStr+char[0].upper()+char[1:len(char)]+" "
    return resStr

# Converting function to UDF
convertUDF = udf(lambda z: convertCase(z),StringType())

df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)

def convertToUpper(name):
    return name.upper()

uppercaseUPF=udf(lambda x:convertToUpper(x),StringType())

df.select(col("Seqno"), \
    uppercaseUPF(col("Name")).alias("Name") ) \
   .show(truncate=False)
