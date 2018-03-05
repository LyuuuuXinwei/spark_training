#共享变量：累加器，#广播变量


#累加器,在一个过程中间附带累积的,但累加器只能用在行动中才准确，节点失败，foreach绝配
file=sc.textfile('')
blanklines = sc.accumulator(0) #累加器从sc得到

def func(line):
	global blanklines
	if (line == ' '):
		blanklines+=1
	return line.split(' ')

input = file.flatmap(func)

valid=sc.accumulator(0)
invalid=sc.accumulator(0)

def validsign(sign):
	global valid,invalid
	if re.match(r' ',sign): #加入验证有效性的正则
		valid+=1
		return Ture
	else:
		invalid+=1
		return false
		
validsign = signs.filter(validsign) #
count = validsign.map(lambda x:(x,1)).reducebykey(lambda x,y:x+y)

#广播变量
broadcast = loadCallSignTable()
def func(sign_count,broadcast): #此时的broadcast是个需要分发的变量,若很大，则代价大
	return
rdd.map(func).reducebykey()

#封装为广播变量
signPrefixes = sc.broadcast(loadCallSignTable())

def processSignCount(sign_count, signPrefixes):
	country = lookupCountry(sign_count[0], signPrefixes.value) #用value获取值
	count = sign_count[1]
	return (country, count)
countryContactCounts = (contactCounts.map(processSignCount).reduceByKey((lambda x, y: x+ y)))


#只对分区做一次的操作
def func(signs):
	http = urllib3.poolmanager()
	urls = map(lambda x: 'http://73s.com/qsos/%s.json'% x, signs)
	request = map(lambda x:(x,http.request('get',x)),urls)
	result = map(lambda x: (x[0],json.loads(x[1].data)),requests)
	return filter(lambda x:x[1] is not none,result)
		
rdd.mappartitions(lambda x:func(x)) #对每个分区操作一次map

foreachPartitions()#foreach不返回