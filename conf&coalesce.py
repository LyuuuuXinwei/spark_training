conf = sparkconf()
conf.set('spark.app.name','myapp')
conf.set('master','local[4]')

lines = lines.filter().coalesce(5).cache()#缓存前合并分区