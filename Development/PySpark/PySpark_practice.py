#Learn basic operations in pyspark

from pyspark.sql import SparkSession
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import row_number
findspark.init()
# Create SparkSession
spark = SparkSession.builder \
      .master("local[2]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

data=[('1461','Vijay',datetime.strptime('2022-10-11','%Y-%m-%d'),500),
        ('1462','Alice',datetime.strptime('2022-10-12','%Y-%m-%d'),200),
('1463','Bob',datetime.strptime('2022-10-11','%Y-%m-%d'),1000),
('1461','Vijay',datetime.strptime('2022-10-13','%Y-%m-%d'),1000),
('1462', 'Alice', datetime.strptime('2022-10-14','%Y-%m-%d'), 400),
('1463', 'Bob',  datetime.strptime('2022-10-12','%Y-%m-%d'), 200),
('1464', 'Robbin', datetime.strptime('2022-10-15','%Y-%m-%d'), 900),
('1461','Vijay',datetime.strptime('2022-10-17','%Y-%m-%d'),1000),
('1461','Vijay',datetime.strptime('2022-10-18','%Y-%m-%d'),1000)
]

schema=StructType([
        StructField('customer_id',StringType(),False),\
        StructField('customer_name',StringType(),True),\
        StructField('order_date',DateType(),True),\
        StructField('cost',IntegerType(),True)
])

data_frame=spark.createDataFrame(data=data,schema=schema)
data_frame.printSchema()

#----------Row_number------------
windowSpec_row_number=Window.partitionBy('customer_id').orderBy(desc('cost'))
df_row_number=data_frame.withColumn('row_number',row_number().over(windowSpec_row_number))
df_row_number.show()

#----------Rank------------
windowSpec_rank=Window.partitionBy('customer_id').orderBy(desc('cost'))
df_rank=data_frame.withColumn('rank',rank().over(windowSpec_rank))
df_rank.show()

#----------Dense_Rank()------
windowSpec_dense_rank=Window.partitionBy('customer_id').orderBy(desc('cost'))
df_dense_rank=data_frame.withColumn('Dense_Rank',dense_rank().over(windowSpec_dense_rank))
df_dense_rank.show()

#--------using when in pyspark-----------------
df_using_when=data_frame.withColumn('product level',when(data_frame.cost>500,lit('High')).otherwise(lit('Low'))).orderBy('customer_id')
df_using_when.show()
#---------using filter -----------------------
df_using_filter=df_using_when.filter(col('product level')=='High')
df_using_filter.show()

#--------Using aggregation function------------
#Create a column for average spending of each customer
window_spec_avg=Window.partitionBy('customer_id')
df_avg_spendings=data_frame.withColumn('average_spendings',avg(data_frame.cost).over(window_spec_avg))
df_avg_spendings.show()
window_on_avg_spendings=Window.partitionBy('customer_id').orderBy(desc(col('cost')))
df_highest_cost_of_each=df_avg_spendings.withColumn('dense_rank',dense_rank().over(window_on_avg_spendings))
df_highest_cost_of_each=df_highest_cost_of_each.filter(col('dense_rank')==lit(2))
df_highest_cost_of_each.show()

#----------LAG,Lead Function-------------------------
window_spec_lag_lead=Window.partitionBy('customer_id').orderBy('order_date')
df_date_lag=data_frame.withColumn('previous_order_date',lag(col('order_date')).over(window_spec_lag_lead))
df_date_lag.show()
df_date_lead=data_frame.withColumn('next_order_date',lead(col('order_date')).over(window_spec_lag_lead))
df_date_lead.show()
df_date_lead_data_diff=df_date_lead.withColumn('date_difference',datediff(df_date_lead.next_order_date,df_date_lead.order_date).cast(IntegerType()))
df_date_lead_data_diff.na.drop().show()
