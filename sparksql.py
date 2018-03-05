SchemaRDD:由Row 对象组成的RDD
#提供hive支持的hivecontext和不提供支持的sqlcontext

from pyspark.sql import hivecontext,row
hivectx=hivecontext(sc)

input = hivectx.jsonfile('.json') #input为SchemaRDD
input.registertemptable('table') #对临时表的操作返回datafrm\ame
schemaRDD = hivectx.sql('')
hiveCtx.cacheTable("tableName")

#基于RDD创建SchemaRDD
rows=sc.parallelize([Row(col1='',col2='')])
schemaRDD = hiveCtx.inferschema(rows)
schemaRDD.registertemptable('')

#使用hivecontexr注册自定义函数，这个函数可以用python编写，可以嵌入SQL语句
def func(x):
	return len(x)
hiveCtx.registerfunction('func_name',func,integertype())
schemaRDD = hiveCtx.sql('select func('text') from table')