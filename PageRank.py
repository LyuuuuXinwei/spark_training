from pyspark import SparkConf,SparkContext

def f(x):
    list1=[]
    s=len(x[1][0])
    for y in x[1][0]:
        list1.append(tuple((y,x[1][1]/s)))
    return list1

def p(x):
    print x

if __name__=="__main__":
    list=[('A',('D',)),('B',('A',)),('C',('A','B')),('D',('A','C'))]
    conf=SparkConf().setMaster("local").setAppName("pagerank")
    sc=SparkContext(conf=conf)
    pages=sc.parallelize(list).map(lambda x:(x[0],tuple(x[1]))).partitionBy(2).cache()
    links=sc.parallelize(['A','B','C','D']).map(lambda x:(x,1.0))
	
   for i in range(1,10):
        rank=pages.join(links).flatMap(f)
        links=rank.reduceByKey(lambda x, y:x+y)
	    links=links.mapValues(lambda x:0.15+0.85*x)
    
	links.foreach(p)
    links.saveAsSequenceFile("/pagerank")