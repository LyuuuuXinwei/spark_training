from pyspark.mllib.feature import StandardScaler

scaler = StandardScaler(withMean=True, withStd=True)
model = scaler.fit(dataset)
result = model.transform(dataset)

#归一化
Normalizer.transform(rdd)

from pyspark.mllib.feature import word2vec

w2v = word2vec()
.fit(dataset)
.transform(dataset)

from pyspark.mllib.stat import statistics


#支持的算法
LinearRegressionWithSGD
LassoWithSGD
RidgeRegressionWithSGD

LogisticRegressionWithLBFGS

SVMWithSGD

mllib.classification.NaiveBayes

mllib.tree.DecisionTree 类中的静态方法trainClassifier() 和trainRegressor()

mllib.clustering.KMeans

mllib.recommendation.ALS  mllib.recommendation.Rating 对象组成的RDD
调用predict() 来对一个由
(userID, productID) 对组成的RDD 进行预测评分。8 你也可以使用model.recommendProducts
(userId, numProducts) 来为一个给定用户找到最值得推荐的前numProduct 个产品

