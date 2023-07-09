import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import*
findspark.init()

spark = SparkSession.builder.master("local[2]").appName('SparkByExamples.com').getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)
#Inner join
employee_inner_join_data=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"inner")

employee_inner_join_data=employee_inner_join_data.select(empDF.emp_id,empDF.name,empDF.superior_emp_id,empDF.year_joined,empDF.emp_dept_id,\
                                empDF.gender,empDF.salary,deptDF.dept_name)
employee_inner_join_data.show(truncate=False)
#leftjoin
employee_left_join_data=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"left")
employee_left_join_data=employee_left_join_data.select(empDF.emp_id,empDF.name,empDF.superior_emp_id,empDF.year_joined,empDF.emp_dept_id,\
                                empDF.gender,empDF.salary,deptDF.dept_name)
employee_left_join_data.show(truncate=False)

#Right join
employee_right_join_data=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"right")
employee_right_join_data=employee_right_join_data.select(empDF.emp_id,empDF.name,empDF.superior_emp_id,empDF.year_joined,empDF.emp_dept_id,\
                                empDF.gender,empDF.salary,deptDF.dept_name)
employee_right_join_data.show(truncate=False)

#Full join
employee_full_join_data=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"full")
employee_full_join_data=employee_full_join_data.select(empDF.emp_id,empDF.name,empDF.superior_emp_id,empDF.year_joined,empDF.emp_dept_id,\
                                empDF.gender,empDF.salary,deptDF.dept_name)
employee_full_join_data.show(truncate=False)

#left anti:-leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records.

employee_left_anti_join_data=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"leftanti")

employee_left_anti_join_data.show(truncate=False)
'''
+------+-----+---------------+-----------+-----------+------+------+
|emp_id|name |superior_emp_id|year_joined|emp_dept_id|gender|salary|
+------+-----+---------------+-----------+-----------+------+------+
|6     |Brown|2              |2010       |50         |      |-1    |
+------+-----+---------------+-----------+-----------+------+------+
'''
