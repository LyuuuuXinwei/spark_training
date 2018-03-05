#spark机器学习的两种场景：1.大数据量并行2.不同机器不同参数的并行，单节点可以使用单机机器学习库
#垃圾邮件
from pyspark.mllib.regression import labelpoint
from pyspark.mllib.feature import hashingtf
from pyspark.mllib.classification import logesticregressionwithsgd

spam = sc.textfile('span.txt')
normal = sc.textfile('normal.txt')

tf = hashingtf(numfeatures = 10000)

spam = spam.map(lambda x:tf.transform(x.split(' ')))
normal = 

spam = spam.map(lambda x:labelpoint(1,x))
normal = 

trainingset = spam.union(normal)
trainingset.cache()

model = logesticregressionwithsgd.train(trainingset)

model.predict(testtext)