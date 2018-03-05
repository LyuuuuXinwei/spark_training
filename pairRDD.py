#pairRDD
#创建pairRDD
pairs = lines.map(lambda x:(x.split(' ')[0],x))

pairRDD.mapvalues(lambda x:(x,1)).reducebykey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

#单词计数
rdd = sc.textFile("s3://...")
words = rdd.flatMap(lambda x: x.split(" "))
counts = words.flatmap(lambda x:(x,1)).reducebykey(lambda x,y:x+y,10)#并行度10

#combinebykey类似于aggregate，都是分区内要聚合一次，然后分区合并，可以让用户返回与输入数据的类型不同的返回值。
sumcount = nums.combinebykey((lambda x:(x,1)), #createcombiner()
							 (lambda x,y:(x[0]+y,x[1]+1)), #mergevalue()已有键的合并
							 (lambda x,y:(x[0]+y[0],x[1]+y[1]))) #mergecombiner()合并累加器
#以上所有x,y指的都是value，nums是一个pairRDD,最终返回的类型是(key,(acc,count))
sumcount.map(lambda key,v:(key,v[0]/v[1])).collectasmap()#返回key下的平均
#以键值对集合{(1, 2), (3, 4), (3, 6)}为例
#collectAsMap() 将结果以映射表的形式返回，以便查询rdd.collectAsMap() Map{(1, 2), (3,4), (3, 6)}

#分区数量
rdd.getnumpartitions()

pairrdd.groupby()	
rdd.groupby(lambda x:x[0])		
#groupBy() 可以用于未成对的数据上，也可以根据除键相同以外的条件进行分组。它可以
#接收一个函数，对源RDD 中的每个元素使用该函数，将返回结果作为键再进行分组。		

pairRDD.join(pairRDD2)
leftouterjoin
rightouterjoin
outerjoin()
		
rdd.sortbykey(ascending=true,numpartitions=,keyfunc=lambda x: str(x))		

#行动操作
countbykey()
sollectasmap()
rdd.lookup(key) #给定键返回值

#分区：基于键的操作多时用,而且是二元操作
join时会计算key的哈希值，将键相同的放在同一个分区，再连接，也就是得数据混洗
userdata.partitionby(spark.HashPartitioner(100)) #传递HashPartitioner对象，哈希分区100个
userdata.partitionby(100)python写法
userdata.join(events)时，events这个小表的混洗已经无所谓了
