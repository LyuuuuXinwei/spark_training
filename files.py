#文件格式与文件系统
input = sc.textFile(" ")
result.saveAsTextFile(outputFile)

import json
data = input.map(lambda x:json.loads(x))
data.filter(lambda x:x['s']).map(lambda x:x.json.dump(x)).saveAsTextFile(outputFile)

import csv
import stringIO
def loadrecord(line):
	input = stringIO.stringIO(line)
	reader = csv.DictReader(input,fieldnames=['col1','col2'])
	return reder.next()
input = sc.textFile(" ").map(loadrecord)

def loadrecordall(file):
	input=stringIO.stringIO(file):
	reader=csv.DictReader(input,fieldnames=[])
	return reader
input=sc.wholetextfiles('').flatmap(loadrecordall)

def writerecords(records):
	ouput== StringIO.StringIO()
	writer = csv.DictWriter(output,fieldnames=["name", "favoriteAnimal"])
	for record in records:
		writer.writerow(record)
	return [output.getvalue()]
rdd.mappartitions(writerecords).saveAsTextFile('')	

data = sc.sequencefile('path',)



#Spark SQL/hive中的结构化数据源
#查询结果是 到由Row 对象组成的RDD
from pyspark.sql import Hivecontext
hct=Hivecontext(sc)
rows=hct.sql('select name,age from users')
firstrow= row.first()

#json:创建一个HiveContext。然后使用HiveContext.jsonFile,注册临时表registerTempTable
#可以把任意SchemaRDD 注册为临时表，hct读完的都是SchemaRDD
tweets=hct.jsonfile('tweets.json')
tweets.registerTempTable('tweets')
result=hct.sql('select name from tweets')



#数据库与键值存储