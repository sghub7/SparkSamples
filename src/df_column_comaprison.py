from pyspark.sql import SparkSession
from pyspark import SparkFiles
"""
Spark demo for comparing two dataframes statistically
"""


#  SparkSession
from pyspark.sql.functions import when, col, array_remove, lit, array, expr, concat_ws, size, first

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
threshold = 0  ## If there is 5% deviation across the src and target stats then mark as failed
## You can either provide option to select or ignore col list from the source/target
selectCols=["id","applied_limit"]
compareKeys=[]
dropCols=["phone"]



print(dropCols)

src=applicants.drop(*dropCols)
src.show()
###If no input passed in compareKey take all non String columns as compare Keys.

if len(compareKeys)==0:
    for field in src.schema.fields:
        print(field.name +" , "+str(field.dataType))
        if str(field.dataType) != "StringType":
            compareKeys.append(field.name)
            """ Sample ***
                id , IntegerType
                name , StringType
                applied_limit , IntegerType
                address , StringType
                segment1 , StringType
                segment2 , StringType"""

print (compareKeys) ##['id', 'applied_limit']



## grp columns are segment columns. i.e columns which virtually segment the dataframe and we need to run comparison on each segment
## cannot use group by here as intent is not really to run any aggregates rather split teh df into multiple segmented df and then run teh describe command
# grpByCols=[]## Empty list signifies no segmentation needed
grpByCols=["segment1","segment2"]## Empty list signifies no segmentation needed

## Create a new column "masterSegment" to segment the data on
src=src.withColumn("masterSegment",concat_ws('___',*grpByCols))
print(" ** Added master Segment")
src.show()

segments = src.select("masterSegment").distinct().toPandas().to_dict(orient='list')
print(segments)
print(segments["masterSegment"])
sortedSegments= segments["masterSegment"]
sortedSegments.sort() ## sorting segments keys list so target and src have the same order in comparison
print("src segments sorted - ",sortedSegments)

#create list of dataframes by segments
srcArray = [src.where(src.masterSegment == x) for x in sortedSegments]
for i in srcArray:
    print("Source DF")
    i.show()
# srcArray = [src.drop("masterSegment") for src in srcArray]

## target DF
trg = applicants1.drop(*dropCols)
trg=trg.withColumn("masterSegment",concat_ws('___',*grpByCols))
trgsegments = trg.select("masterSegment").distinct().toPandas().to_dict(orient='list')
print(trgsegments)
print("Target Segments - ",trgsegments["masterSegment"])
#create list of dataframes by segments
## Only add the master segments which are common across target and src.. Segments which are not common across sc and target will not be compared
commonSegments = list(set(trgsegments["masterSegment"]).intersection(set(segments["masterSegment"])))
commonSegments.sort() ## sorting segments keys list so target and src have the same order in comparison
print("Common Segments Sorted - ",commonSegments)

trgArray = [trg.where(trg.masterSegment == x) for x in commonSegments]
for i in trgArray:
    print("Target DF")
    i.show()

## If segments are added/removed  in target..
if len(sortedSegments) != len(trgsegments["masterSegment"]):
    print(f" Source and Target number of segments dont match src == {segments}  and target ={trgsegments}")

masterStatus =[]
compareDFs =[]
"""
iterate for every segment in target.
if target has extra segments compared to source , then 
"""

for index,target in enumerate(trgArray):
    src = srcArray[index]
    print("***Target Dataframes Marked Columns***")

        #Prepending columns with "target_" for differentiating target and source columns
    target.show()
    target = target.selectExpr([colName+' as xx_target_' + colName for colName in target.columns])
    """
    Sample Data -
        +------------+--------------+-----------------------+-----------------+------------------+------------------+-----------------------+
    |xx_target_id|xx_target_name|xx_target_applied_limit|xx_target_address|xx_target_segment1|xx_target_segment2|xx_target_masterSegment|
    +------------+--------------+-----------------------+-----------------+------------------+------------------+-----------------------+
    |           4|         User4|                   9000|               A4|                DU|                D2|                DU___D2|
    +------------+--------------+-----------------------+-----------------+------------------+------------------+-----------------------+
    """
    # target.show()
    """
    Below loop is for renaming the compare columns names back to original names 
    Done to make it easier to join on similar column names across src and target
    """
    for i in compareKeys:
        target = target.withColumnRenamed(f"xx_target_{i}",i)
    """
    target.show()
    Sample Data 
        +------------+-----+-----------------------+-------+------------------+------------------+-----------------------+
    |xx_target_id| name|xx_target_applied_limit|address|xx_target_segment1|xx_target_segment2|xx_target_masterSegment|
    +------------+-----+-----------------------+-------+------------------+------------------+-----------------------+
    |           4|User4|                   9000|     A4|                DU|                D2|                DU___D2|
    +------------+-----+-----------------------+-------+------------------+------------------+-----------------------+
      
    """

    mismatches = [when( ( (target["xx_target_"+c]) > threshold * src[c] + src[c] ) | ( (target["xx_target_"+c]) < src[c]  - threshold * src[c]  )  , lit(c)).otherwise("") for c in src.columns if c not in  compareKeys]

    # mismatches = [when( ( (target["xx_target_"+c]) != src[c]    )  , lit(c)).otherwise("") for c in src.columns if c not in  compareKeys]

    # ## Create a new col with name mismatched_columns to store the list of mismatched column
    ## summary is the column name on which teh dfs will be joind to compare
    select_expr =[

        *[src[c] for c in src.columns ],
        *[target[c] for c in target.columns if c not in compareKeys],
        array_remove(array(*mismatches), "").alias("mismatched_columns")#Removes the extra "" record intriduced if not mismatch
    ]

    src.show()
    compareDF= src.join(target,compareKeys,'inner').select(*select_expr)
    print("compare *********")
    # compareDF.select(size("mismatched_columns")).show()
    compareDF=compareDF.withColumn("numColsMismatch",size("mismatched_columns"))##write this s3
    compareDF.show()
    compareDFs.append(compareDF)
    # compareDF.write.mode('overwrite').parquet('compare_details')

    totalMismatchRecords = compareDF.groupBy().sum("numColsMismatch").collect()
    segmentInfo = compareDF.select(first("xx_target_masterSegment")).collect()
    print("Total Mismatches - ", totalMismatchRecords[0][0])
    print("Segment Info - ", segmentInfo[0][0])
    masterStatus.append({"segmentColumns":segmentInfo[0][0],"mismatches":totalMismatchRecords[0][0]})

print(masterStatus)

cdf = compareDFs[0]
for i in range(1, len(compareDFs)):
    cdf = cdf.union(compareDFs[i])
cdf.write.mode('overwrite').parquet('compare_details')

## Reading detailed reports
df = spark.read.parquet("compare_details")
df.show()
"""
sample output - 
+---+-----+-------------+-------+--------+--------+-------------+------------+-----------------------+------------------+------------------+-----------------------+-------------------+---------------+
| id| name|applied_limit|address|segment1|segment2|masterSegment|xx_target_id|xx_target_applied_limit|xx_target_segment1|xx_target_segment2|xx_target_masterSegment| mismatched_columns|numColsMismatch|
+---+-----+-------------+-------+--------+--------+-------------+------------+-----------------------+------------------+------------------+-----------------------+-------------------+---------------+
| 11|User1|        10000|     A1|      SU|      S1|      SU___S1|           1|                  10000|                SU|                S1|                SU___S1|               [id]|              1|
|  2|User2|        30000|     A2|      SU|      S1|      SU___S1|           2|                  20000|                SU|                S1|                SU___S1|    [applied_limit]|              1|
| 50|User5|        15000|     A5|      SU|      S2|      SU___S2|          68|                  17000|                SU|                S2|                SU___S2|[id, applied_limit]|              2|
| 40|User4|         9000|     A4|      DU|      D2|      DU___D2|           4|                   9000|                DU|                D2|                DU___D2|               [id]|              1|
| 30|User3|        20000|     A3|      DU|      D1|      DU___D1|          30|                  20000|                DU|                D1|                DU___D1|                 []|              0|
+---+-----+-------------+-------+--------+--------+-------------+------------+-----------------------+------------------+------------------+-----------------------+-------------------+---------------+

"""
