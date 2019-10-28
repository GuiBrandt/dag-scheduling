from pyspark.sql import DataFrame, SparkSession, functions as F
from functools import reduce
from dag import Task
from typing import Dict

class SimpleTask(Task):
    def perform(self, df: DataFrame) -> DataFrame:
        print("Performing", self.name, '...')
        return df

class SimpleJoinTask(Task):
    def perform(self, data: Dict[str, DataFrame]) -> DataFrame:
        print("Performing join task", self.name, '...')
        df = reduce(DataFrame.union, data.values())
        return df


spark = SparkSession.builder.getOrCreate()

t1 = SimpleTask('a')
t2 = SimpleTask('b')
t3 = SimpleTask('c')
t4 = SimpleTask('d')
t5 = SimpleTask('e')
t6 = SimpleJoinTask('f')
t7 = SimpleTask('g')
t8 = SimpleTask('h')
t9 = SimpleJoinTask('i')

d = ((t1 >> t2 >> t3) // (t4 >> t5)) >> t6 >> (t7 // t8) >> t9

spark = SparkSession.builder.getOrCreate()

df1 = spark.createDataFrame([('A', 1, '22/10/2019')], ('name', 'number', 'date'))
df2 = spark.createDataFrame([('B', 2, '22/10/2019')], ('name', 'number', 'date'))

result = d.perform({ 'a': df1, 'd': df2 })
result.show()
