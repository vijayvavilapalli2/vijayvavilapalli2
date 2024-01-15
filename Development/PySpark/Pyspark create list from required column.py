
import findspark
findspark.init()
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

data = [("James","Smith","USA","CA"),("Michael","Rose","USA","NY"), \
    ("Robert","Williams","USA","CA"),("Maria","Jones","USA","FL") \
  ]
columns=["firstname","lastname","country","state"]
df=spark.createDataFrame(data=data,schema=columns)
df.show()
#First method
state1=df.rdd.map(lambda x:x[3]).collect()
print(state1)

#second method

state2=df.rdd.map(lambda x:x.state).collect()
print('second method:')
print(state2)

#Third method
state3=df.select('state').rdd.flatMap(lambda x:x).collect()
print('Third method')
print(state3)

#Fourth method
state4=df.select(df.state).toPandas()
list_state4=list(state4['state'])
print('Fourth method')
print(list_state4)
#Fifth method
state5=df.select(df.state,df.firstname).toPandas()
list_state5=list(state5['state'])
print('Fifth method')
print(list_state5)

#Eliminate duplicates from the list
from collections import OrderedDict
print(OrderedDict.fromkeys(list_state5))
res = list(OrderedDict.fromkeys(list_state5))
print('Eliminate duplicates from the list')
print(res)
d=OrderedDict([('Vijay',None),('Vijay',None)])
#OrderDict will eliminate the duplicate keys and only have distinct keys
print(list(d))



