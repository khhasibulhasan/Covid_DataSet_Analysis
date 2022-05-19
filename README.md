
dbutils.fs.ls ("/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/")



files=dbutils.fs.ls ("/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/")



sizeList = [ x.size for x in files]



pathlist= [x.path for x in files]



filesRDD = sc.parallelize (pathlist)



filesRDD.take(5)

-
filesRDD.count()



csvFilesRDD= filesRDD.filter(lambda a:'.csv' in a)



csvFilesRDD.take(3)



csvFilesRDD.count()



difference=filesRDD.count()-csvFilesRDD.count()



difference



import re

def convert_name (filepath):
    m = re.match("^.*([0-9]{2})-([0-9]{2})-([0 -9]{4}).csv$",filepath)
    return int ( m.group(3)+m.group(1)+m.group(2))



convert_name('dbfs:/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/01-01-2021.csv')


csvPairRDD =csvFilesRDD.map(lambda a: [convert_name(a),a])



csvPairRDD.take(3)



customRDD = csvPairRDD.sortByKey(ascending=False)



customRDD.take(5)



chosenfile = customRDD.first()



chosenfile[1]



chosenDF = spark.read.csv(chosenfile[1])



chosenDF.show(truncate=False)





