#!pip install pyspark
import pyspark, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType
from os import listdir




def preformat_union_csvs(file_name='kidney.csv', remove_top_8_lines=False):
  """
        DOCSTRING:  Reads the raw csv of Azimuth data, optionally removes the top 8 lines and brings it in the required structure like the ASCT-B CCF reporter visualization tool.
                    We will remove the first 8 lines, drop the *COUNT* columns, add a new column containing File-name for backtracking purposes and register a new Pyspark.SQL view.
        INPUT:      csv file-name, resultant-df to append new dataframe to, flag to indicate removal of top 8 lines for merging.
        OUTPUT:     Dataframe csv at target-directory, or error.
  """
  try:
    df = spark.read.options(header=True, inferSchema=True).csv(file_name)

    # Ignore the first 8 lines containing metadata
    df = df.rdd.zipWithIndex().filter(lambda x: x[1] > 8).map(lambda x: x[0]).toDF()

    # Rename the dataframe columns as the first row in current dataframe after deleting 8 lines
    schema_from_first_row = StructType([ StructField(field_name, StringType(), True)  for field_name in df.first() ])
    df = spark.createDataFrame(df.rdd, schema=schema_from_first_row)

    # Drop the /COUNT/ columns for now
    regex = re.compile(".*COUNT.*")
    for col in list(filter(regex.match, df.schema.names)):
      df = df.drop(col)
    
    # Add a FILE column for backtracking purposes
    file_category = file_name.replace('.csv','')
    df = df.withColumn('FILE',lit(file_category))
    
    # Create temp views in spark sql for later processing via SQL
    df.createOrReplaceTempView(file_category)

    print('\nPre-formatted the input data for {} successfully!'.format(file_name))
    return df
  except Exception as e:
    print('\nSomething went wrong while pre-formatting the input data. Please check if the file is currently in use.')
    print(e)



spark = SparkSession.builder.master('local[1]').appName('Azimuth_Launcher.com').getOrCreate()
csvs = [fname for fname in listdir() if fname.endswith('csv')]
print(csvs)
print(spark)


anatomical_structure_dfs = {}
for csv in csvs:
  anatomical_structure_dfs[csv] = preformat_union_csvs(csv)




""" 
    Further Preprocessing hardcoded for now, should be dynamic based on varying number of column-headers 
"""

# Manipulate each of the pyspark-sql views to bring them in the Source->Target network structure
anatomical_structure_dfs['pbmc.csv'] = spark.sql("""
              SELECT `AS/1`, `AS/1/LABEL`, `AS/1/ID`, `AS/2`, `AS/2/LABEL`, `AS/2/ID`, '1->2' LAYER_CONNECTION, FILE from pbmc
              UNION
              SELECT `AS/2`, `AS/2/LABEL`, `AS/2/ID`, `AS/3`, `AS/3/LABEL`, `AS/3/ID`, '2->3' LAYER_CONNECTION, FILE from pbmc
          """)

anatomical_structure_dfs['lung.csv'] = spark.sql("""
              SELECT `AS/1`, `AS/1/LABEL`, `AS/1/ID`, `AS/2`, `AS/2/LABEL`, `AS/2/ID`, '1->2' LAYER_CONNECTION, FILE from lung
          """)

anatomical_structure_dfs['pancreas.csv'] = spark.sql("""
              SELECT `AS/1`, `AS/1/LABEL`, `AS/1/ID`,  `AS/1` AS `AS/2`,  `AS/1/LABEL` as `AS/2/LABEL`, `AS/1/ID` as `AS/1/ID`, '1->2' LAYER_CONNECTION, FILE from pancreas
          """)

anatomical_structure_dfs['kidney.csv'] = spark.sql("""
              SELECT `AS/1`, `AS/1/LABEL`, `AS/1/ID`, `AS/2`, `AS/2/LABEL`, `AS/2/ID`, '1->2' LAYER_CONNECTION, FILE from kidney
              UNION
              SELECT `AS/2`, `AS/2/LABEL`, `AS/2/ID`, `AS/3`, `AS/3/LABEL`, `AS/3/ID`, '2->3' LAYER_CONNECTION, FILE from kidney
          """)

anatomical_structure_dfs['motor_cortex.csv'] = spark.sql("""
              SELECT `AS/1`, `AS/1/LABEL`, `AS/1/ID`, `AS/2`, `AS/2/LABEL`, `AS/2/ID`, '1->2' LAYER_CONNECTION, FILE from motor_cortex
              UNION
              SELECT `AS/2`, `AS/2/LABEL`, `AS/2/ID`, `AS/3`, `AS/3/LABEL`, `AS/3/ID`, '2->3' LAYER_CONNECTION, FILE from motor_cortex
              UNION
              SELECT `AS/3`, `AS/3/LABEL`, `AS/3/ID`, `AS/4`, `AS/4/LABEL`, `AS/4/ID`, '3->4' LAYER_CONNECTION, FILE from motor_cortex
          """)



# Create a dummy target dataframe to store a Source->Target network structure
tgt_schema = StructType([
                         StructField('AS/1',StringType(),True), StructField('AS/1/LABEL',StringType(),True), 
                         StructField('AS/1/ID',StringType(),True), StructField('AS/2',StringType(),True), 
                         StructField('AS/2/LABEL',StringType(),True), StructField('AS/2/ID',StringType(),True), 
                         StructField('LAYER_CONNECTION',StringType(),True), StructField('FILE',StringType(),True)
                         ])
tgt_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema = tgt_schema)


# For each standardized dataset, union it with the dummy tgt dataframe we created
for csv in csvs:
  tgt_df = tgt_df.union(anatomical_structure_dfs[csv])

tgt_df.schema.names = ['LABEL', 'EXPANDED_LABEL', 'AS/1/ID', 'LABEL', 'EXPANDED_LABEL', 'AS/2/ID', 'LAYER_CONNECTION', 'FILE']
tgt_df.summary().show()

tgt_df.toPandas().to_csv('New_Consolidated_Network_Python.csv', index=False, sep='\t')