import statistics

from pyspark.sql import SparkSession
from pyspark import SparkFiles
"""
Spark demo for comparing two dataframes column by column
"""
#  SparkSession
from pyspark.sql.functions import when, col, array_remove, lit, array, expr, first

spark = SparkSession.builder.appName("SplineDemo").getOrCreate()

##Loading
applicants = (
    spark.read.option("header", "true")
        .option("inferschema", "true")
        .csv("../data/input/applicants_table.csv")
)
applicants.createOrReplaceTempView("applicants")

applicants1 = (
    spark.read.option("header", "true")
        .option("inferschema", "true")
        .csv("../data/input/applicant-1.csv")
)


## Ignore col list
dropCols=["applied_limit","address","phone"]

## Group data
grpByCols=["segment1","segment2"]
## Drop ignored columns
## Src DF
src = applicants.drop(*dropCols).groupby(*grpByCols).agg(statistics.stdev("id").alias("sumC"))
src.show(20,False)

## target DF
target = applicants1.drop(*dropCols).groupby(*grpByCols).agg(first("id").alias("sumC"))

## Prepending columns with "target_"
target = target.selectExpr([colName+' as target_' + colName for colName in target.columns])
target.show()

#
# src.show()
# target.show()
#
#
# ## Descrive DF to check stats.. Note: Not applicable to String cloumns
# desc=dDF.describe()
# desc.show()
# desc.printSchema()
#
# ## Mismatched list..
# mismatches = [when(desc[c]!=desc1["target_"+c], lit(c)).otherwise("") for c in desc.columns if c != 'summary']
# ## Creata a new col with name mismatched_columns to store the list of mismatched column
# ## summary is the column name on which teh dfs will be joind to compare
# select_expr =[
#
#     *[desc[c] for c in desc.columns ],
#     *[desc1[c] for c in desc1.columns if c != 'summary'],
#     array_remove(array(*mismatches), "").alias("mismatched_columns")
# ]
# compareDF= desc.join(desc1,'summary','inner').select(*select_expr)
# compareDF.show()
# compareDict  = compareDF.toPandas().to_dict() ## Collecting to driver to store results as dict
# print(compareDict["mismatched_columns"])
# result="Pass"
# for k,v in compareDict["mismatched_columns"].items():
#     if len(v) > 0:
#         result="Fail"
#         break
# print(result)
# print(compareDict)
