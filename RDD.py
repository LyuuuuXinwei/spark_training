#读取、创建
lines = sc.textfile('README.md')
lines = sc.parallelize([1,2,3])

#转化
pythonlines = line.filter(lambda line :"python" in line)
errorrdd = inputrdd.filter(lambda x:'' in x)
warningrdd = inputrdd.filter(lambda x: '' in x)
badlinesrdd = errorrdd.union(warningrdd) #会包含这些重复数据
rdd1.intersection(rdd2) #交集，去重
rdd1.subtract(rdd2)# 差集
rdd.distinct()#去重
rdd.sample(withreplacement=False,0.1) #采样

nums=sc.parallelize([1,2,3,4])
squared=nums.map(lambda x:x*x).collect()

lines = sc.parallelize(["hello world", "hi"])
words = lines.flatmap(lambda x:x.split(' '))
#RDD怎样变python的数据结构？ words.collect()?

#行动
pythonlines.first()
badlinesrdd.count()
for line in badlinesrdd.take(10): #取到本地
rdd.collect() #取到本地--驱动器端
sum = rdd.reduce(lambda x,y:x+y)
sum = rdd.fold(0)(lambda x,y:x+y) #fold提供
rdd.top()
rdd.roreach(func)

#aggregate直接将map转换二元组--节点reduce--总体reduce封装了
sumcount = nums.aggregate((0,0),
							lambda acc ,value : (acc[0] + value,acc[1]+1)  
							lambda acc1,acc2:(acc1[0]+acc2[0],acc1[1]+acc2[1]))  
return sumcount[0]/sumcount[1]

#专用操作
numRDD.mean()
numRDD.variance()
max/sum/




pairRDD.join(pairRDD)

#rdd存入分布式文件系统
rdd.saveAsTextFile('')
rdd.saveAsSequenceFile('')
#缓存RDD
lines.persist()
lines.cache()
