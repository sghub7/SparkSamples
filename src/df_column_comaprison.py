from pyspark.sql import SparkSession
from pyspark import SparkFiles
"""
Spark demo for comparing two dataframes statistically
"""
#  SparkSession
from pyspark.sql.functions import when, col, array_remove, lit, array, expr, concat_ws

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

##threshold
threshold = 0.05


## You can either provide option to select or ignore col list from the source/target
selectCols=["id","name"]
dropCols=["applied_limit","address","phone"]

sDF = applicants.select(*selectCols)
# sDF.show(20,False)

## Drop ignored columns
## Src DF
src = applicants.drop(*dropCols)
# src.show(20,False)


## grp columns are segment columns. i.e columns which virtually segment the dataframe and we need to run stats on each segment
## cannot use group by here as intent is not really to run any aggregates rather split teh df into multiple segmented df and then run teh describe command
# grpByCols=[]## Empty list signifies no segmentation needed
grpByCols=["segment1","segment2"]## Empty list signifies no segmentation needed

## Create a new column "masterSegment" to segment the data on
src=src.withColumn("masterSegment",concat_ws('||',*grpByCols))
# src.show()

segments = src.select("masterSegment").distinct().toPandas().to_dict(orient='list')
print(segments)
print(segments["masterSegment"])
#create list of dataframes by segments
srcArray = [src.where(src.masterSegment == x) for x in segments["masterSegment"]]
srcArray = [src.drop("masterSegment") for src in srcArray]
# if segments["masterSegment"] ==['']:
#     srcArray= [src]
# else:
#     srcArray = [src.where(src.masterSegment == x) for x in segments["masterSegment"]]
srcStatsArray=[]
for i in srcArray:
    print("***Source Dataframes***")
    i.show()
    # Describe DF to check stats.. Note: Not applicable to String columns
    src_stats=i.describe()
    srcStatsArray.append(src_stats)
    src_stats.show()
    src_stats.printSchema()

## target DF
trg = applicants1.drop(*dropCols)
trg=trg.withColumn("masterSegment",concat_ws('||',*grpByCols))
trgsegments = trg.select("masterSegment").distinct().toPandas().to_dict(orient='list')
print(trgsegments)
print(trgsegments["masterSegment"])
#create list of dataframes by segments
trgArray = [trg.where(trg.masterSegment == x) for x in trgsegments["masterSegment"]]
"""
Program assumes the segments are common across src and target. If not, throw an exception
"""
if len(srcArray) != len(trgArray):
    raise Exception(f" Source and Target number of segments dont match src == {segments}  and target ={trgsegments}")
"""
Below loop iterates for each target.
Compares teh stats at segment level
Appends the stats in a list
Finally creates a dictionary and writes the final status for all segments
"""
masterStatus =[]
for index,i in enumerate(trgArray):
    print("***Target Dataframes***")
    i.show()
    # Describe DF to check stats.. Note: Not applicable to String columns
    trg_stats=i.drop("masterSegment").describe()

    #Prepending columns with "target_" for differentiating taget and source columns
    trg_stats = trg_stats.selectExpr([colName+' as xx_target_' + colName for colName in trg_stats.columns]) \
        .withColumnRenamed("xx_target_summary","summary")
    trg_stats.show()
    trg_stats.printSchema()

    """
    # ## Mismatched list. Comapare each df in source and target df arrays
    ## Assumption - Src and target have same set of segments.. Handled by line #73
    ## 
    """

    mismatches = [when(srcStatsArray[index][c]!=trg_stats["xx_target_"+c], lit(c)).otherwise("") for c in srcStatsArray[index].columns if c != 'summary']

    # ## Create a new col with name mismatched_columns to store the list of mismatched column
    ## summary is the column name on which teh dfs will be joind to compare
    select_expr =[

        *[srcStatsArray[index][c] for c in srcStatsArray[index].columns ],
        *[trg_stats[c] for c in trg_stats.columns if c != 'summary'],
        array_remove(array(*mismatches), "").alias("mismatched_columns")#Removes the extra "" record intriduced if not mismatch
    ]

    src = srcStatsArray[index]
    src.show()
    compareDF= src.join(trg_stats,'summary','inner').select(*select_expr)
    compareDF.show()
    compareDict  = compareDF.toPandas().to_dict() ## Collecting to driver to store results as dict
    print(compareDict["mismatched_columns"])
    result="Pass"
    for k,v in compareDict["mismatched_columns"].items():
        if len(v) > 0:
            result="Fail"
            break
    masterStatus.append({"segment_index":index,"segment_result":result,"details":compareDict})

print(masterStatus)
"""
Sample output - 
[{'segment_index': 0, 'segment_result': 'Fail', 'details': {'summary': {0: 'count', 1: 'mean', 2: 'stddev', 3: 'min', 4: 'max'}, 'id': {0: '2', 1: '10.5', 2: '13.435028842544403', 3: '1', 4: '20'}, 'name': {0: '2', 1: None, 2: None, 3: 'User1', 4: 'User2'}, 'segment1': {0: '2', 1: None, 2: None, 3: 'SU', 4: 'SU'}, 'segment2': {0: '2', 1: None, 2: None, 3: 'S1', 4: 'S1'}, 'xx_target_id': {0: '2', 1: '1.5', 2: '0.7071067811865476', 3: '1', 4: '2'}, 'xx_target_name': {0: '2', 1: None, 2: None, 3: 'User1', 4: 'User2'}, 'xx_target_segment1': {0: '2', 1: None, 2: None, 3: 'SU', 4: 'SU'}, 'xx_target_segment2': {0: '2', 1: None, 2: None, 3: 'S1', 4: 'S1'}, 'mismatched_columns': {0: [], 1: ['id'], 2: ['id'], 3: [], 4: ['id']}}}, {'segment_index': 1, 'segment_result': 'Pass', 'details': {'summary': {0: 'count', 1: 'mean', 2: 'stddev', 3: 'min', 4: 'max'}, 'id': {0: '1', 1: '50.0', 2: None, 3: '50', 4: '50'}, 'name': {0: '1', 1: None, 2: None, 3: 'User5', 4: 'User5'}, 'segment1': {0: '1', 1: None, 2: None, 3: 'SU', 4: 'SU'}, 'segment2': {0: '1', 1: None, 2: None, 3: 'S2', 4: 'S2'}, 'xx_target_id': {0: '1', 1: '50.0', 2: None, 3: '50', 4: '50'}, 'xx_target_name': {0: '1', 1: None, 2: None, 3: 'User5', 4: 'User5'}, 'xx_target_segment1': {0: '1', 1: None, 2: None, 3: 'SU', 4: 'SU'}, 'xx_target_segment2': {0: '1', 1: None, 2: None, 3: 'S2', 4: 'S2'}, 'mismatched_columns': {0: [], 1: [], 2: [], 3: [], 4: []}}}, {'segment_index': 2, 'segment_result': 'Pass', 'details': {'summary': {0: 'count', 1: 'mean', 2: 'stddev', 3: 'min', 4: 'max'}, 'id': {0: '1', 1: '30.0', 2: None, 3: '30', 4: '30'}, 'name': {0: '1', 1: None, 2: None, 3: 'User3', 4: 'User3'}, 'segment1': {0: '1', 1: None, 2: None, 3: 'DU', 4: 'DU'}, 'segment2': {0: '1', 1: None, 2: None, 3: 'D1', 4: 'D1'}, 'xx_target_id': {0: '1', 1: '30.0', 2: None, 3: '30', 4: '30'}, 'xx_target_name': {0: '1', 1: None, 2: None, 3: 'User3', 4: 'User3'}, 'xx_target_segment1': {0: '1', 1: None, 2: None, 3: 'DU', 4: 'DU'}, 'xx_target_segment2': {0: '1', 1: None, 2: None, 3: 'D1', 4: 'D1'}, 'mismatched_columns': {0: [], 1: [], 2: [], 3: [], 4: []}}}, {'segment_index': 3, 'segment_result': 'Fail', 'details': {'summary': {0: 'count', 1: 'mean', 2: 'stddev', 3: 'min', 4: 'max'}, 'id': {0: '1', 1: '40.0', 2: None, 3: '40', 4: '40'}, 'name': {0: '1', 1: None, 2: None, 3: 'User4', 4: 'User4'}, 'segment1': {0: '1', 1: None, 2: None, 3: 'DU', 4: 'DU'}, 'segment2': {0: '1', 1: None, 2: None, 3: 'D2', 4: 'D2'}, 'xx_target_id': {0: '1', 1: '4.0', 2: None, 3: '4', 4: '4'}, 'xx_target_name': {0: '1', 1: None, 2: None, 3: 'User4', 4: 'User4'}, 'xx_target_segment1': {0: '1', 1: None, 2: None, 3: 'DU', 4: 'DU'}, 'xx_target_segment2': {0: '1', 1: None, 2: None, 3: 'D2', 4: 'D2'}, 'mismatched_columns': {0: [], 1: ['id'], 2: [], 3: ['id'], 4: ['id']}}}]

"""


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
