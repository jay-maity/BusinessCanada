from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from datetime import datetime
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler, StringIndexer, Normalizer, PolynomialExpansion
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row

DATE_BEGIN=1998
DATE_END=2016+1
#FILE="DataMerging/merged_final.csv"
FILE="DataMerging/small.csv"
REG_INT=1

"""
+--------------------+
|Status              |
+--------------------+
|Open                |
|Invalid Status Code |
|Cancelled           |
|Gone Out of Business|
|Issued              |
|Inactive            |
|Pending             |
|Closed              |
+--------------------+

"""

CLOSED_STATUS="%r"%(tuple(['Cancelled', 'Closed' 'Gone Out of Business']),)
ISSUED_STATUS="%r"%(tuple(['Issued', 'Open']),)


def clean_data(df_data):

    #df_data.distinct().select("IssuedDate").orderBy("IssuedDate", ascending=False).show()
    df_data=df_data.where("length(IssuedDate)>=8")

    def udf_convert_date(row):
        try:
            return datetime.strptime(row[:10], "%Y-%m-%d")
        except:
            try:
                return datetime.strptime(row, "%d-%b-%Y")
            except:
                return None

    convert_date = udf(udf_convert_date, DateType())

    df_data=df_data.withColumn("IssuedDate", convert_date(df_data.IssuedDate))
    df_data=df_data.withColumn("ExpiryDate", convert_date(df_data.ExpiryDate))

    def udf_extract_year(row):
        if row is not None:
            return row.year
        else:
            return None

    extract_year = udf(udf_extract_year, IntegerType())
    df_data = df_data.withColumn("IssuedYear", extract_year(df_data.IssuedDate))
    df_data = df_data.withColumn("ExpiryYear", extract_year(df_data.ExpiryDate))

    df_data = df_data.where("IssuedDate is not null")
    df_data = df_data.where("BusinessName is not null or length(BusinessName)>1 ")

    df_data=df_data.cache()


    #df_data.show()
    #print "count:", df_data.count()

    return df_data


def load_dataset(filename, sqlCt):


    df_data=sqlCt.read.format('com.databricks.spark.csv')\
                      .options(header=True) \
                      .load(filename)


    return clean_data(df_data)


def opens_per_year(df_data, sqlCt, out):

    df_data.registerTempTable("business_licenses")

    df_opens_year=sqlCt.sql("""SELECT BusinessType, IssuedYear, count(*) as NumOpens
                               FROM business_licenses
                               WHERE Status in %s
                               GROUP BY BusinessType, IssuedYear
                               ORDER BY IssuedYear"""%ISSUED_STATUS)

    df_opens_year.coalesce(1).write.format("csv").options(header=True).save("out_data/"+out+"/opens_per_year", mode="overwrite")


    #--Getting top 5----
    top5 = sqlCt.sql("""SELECT BusinessType
                                   FROM business_licenses
                                   WHERE Status in %s
                                   GROUP BY BusinessType
                                   ORDER BY count(*) DESC
                                   LIMIT 5""" % ISSUED_STATUS).collect()

    top5=[str(row.BusinessType) for row in top5 ]
    df_opens_year.where("BusinessType in %r"%(tuple(top5),)).coalesce(1).write.format("csv").options(header=True)\
        .save("out_data/"+out+"/top5_opens_per_year", mode="overwrite")

    #df_opens_year.where("BusinessType=='Office'").orderBy("NumOpens", ascending=False).show()

    return df_opens_year

def closes_per_year(df_data, sqlCt, out):

    df_data.registerTempTable("business_licenses")

    df_closes_year=sqlCt.sql("""SELECT BusinessType, ExpiryYear, count(*) as NumCloses
                                FROM business_licenses
                                WHERE ExpiryYear is not null AND Status in %s
                                GROUP BY BusinessType, ExpiryYear
                                ORDER BY ExpiryYear"""%CLOSED_STATUS)

    df_closes_year.coalesce(1).write.format("csv").options(header=True).save("out_data/"+out+"/closes_per_year", mode="overwrite")
    # --Getting top 5----
    top5 = sqlCt.sql("""SELECT BusinessType
                                     FROM business_licenses
                                     WHERE Status in %s
                                     GROUP BY BusinessType
                                     ORDER BY count(*) DESC
                                     LIMIT 5""" % CLOSED_STATUS).collect()

    top5 = [str(row.BusinessType) for row in top5]

    df_closes_year.where("BusinessType in %r" % (tuple(top5),)).coalesce(1).write.format("csv").options(header=True)\
        .save("out_data/"+out+"/top5_closes_per_year", mode="overwrite")

    #df_opens_year.where("BusinessType=='Office'").orderBy("NumOpens", ascending=False).show()

    return df_closes_year

def udf_longevity(current, IssuedDate, ExpiryDate):
    #print "current", current, "IssuedDate", IssuedDate, "ExpiryDate:", ExpiryDate

    if IssuedDate is None:
        return 0

    if ExpiryDate is not None:
        if ExpiryDate.year > current:
            return (datetime(current, 12, 31).date() - IssuedDate).days
        elif ExpiryDate.year == current:
            return (ExpiryDate - IssuedDate).days
        elif ExpiryDate.year < current:
            return (ExpiryDate - IssuedDate).days
    else:
        return (datetime(current, 12, 31).date()-IssuedDate).days

def longevity_of_closed_per_year(df_data, sqlCt, out):
    #df_data.select("IssuedDate").distinct().orderBy("IssuedDate").show()

    df_longevity_of_closed={}
    df_save = None
    for year in range(DATE_BEGIN, DATE_END):
        print "Year", year
        longevity = udf(lambda IssuedDate, ExpiryDate: udf_longevity(year, IssuedDate, ExpiryDate), IntegerType())
        df_data_longevity=df_data.withColumn("Longevity",longevity(df_data.IssuedDate, df_data.ExpiryDate))
        df_data_longevity.registerTempTable("business_licenses_longevity_closed_%s"%year)

        df_longevity_of_closed[year] = sqlCt.sql("""SELECT %s as year, BusinessType,
                                                      count(*) as NumBusiness,
                                                      sum(Longevity)/count(*) as AvgLongevity,
                                                      (sum(Longevity)/count(*))/365 as AvgLongevityYears
                                                      FROM business_licenses_longevity_closed_%s
                                                      WHERE Longevity>0 and
                                                            Longevity is not null and
                                                            Status in %s AND
                                                            IssuedYear<=%s
                                                      GROUP BY BusinessType Order By count(*) desc"""%(year, year, CLOSED_STATUS, year))

        if df_save is None:
            df_save = df_longevity_of_closed[year]
        else:
            df_save=df_save.unionAll(df_longevity_of_closed[year])

    df_save.coalesce(1).write.format("csv").options(header=True)\
            .save("out_data/"+out+"/longevity_of_closed_per_year", mode="overwrite")


    return df_save

def longevity_of_issued_per_year(df_data, sqlCt, out):
    #df_data.select("IssuedDate").distinct().orderBy("IssuedDate").show()

    df_longevity_of_issued={}

    df_save = None
    for year in range(DATE_BEGIN, DATE_END):
        print "Year", year
        longevity = udf(lambda IssuedDate, ExpiryDate: udf_longevity(year, IssuedDate, ExpiryDate), IntegerType())
        df_data_longevity=df_data.withColumn("Longevity",
                                              longevity(df_data.IssuedDate, df_data.ExpiryDate))

        df_data_longevity.registerTempTable("business_licenses_longevity_issued_%s"%year)

        df_longevity_of_issued[year] = sqlCt.sql("""SELECT %s as year,  BusinessType,
                                                      count(*) as NumBusiness,
                                                      sum(Longevity)/count(*) as AvgLongevity,
                                                      (sum(Longevity)/count(*))/365 as AvgLongevityYears
                                                      FROM business_licenses_longevity_issued_%s
                                                      WHERE Longevity>0 and
                                                            Longevity is not null and
                                                            Status in %s AND
                                                            IssuedYear<=%s
                                                      GROUP BY BusinessType Order By count(*) desc"""%(year,year, ISSUED_STATUS, year))

        if df_save is None:
            df_save = df_longevity_of_issued[year]
        else:
            df_save=df_save.unionAll(df_longevity_of_issued[year])

    #df_save.coalesce(1).write.format("csv").options(header=True)\
    #        .save("out_data/"+out+"/longevity_of_issued_per_year", mode="overwrite")


    return df_save

def longevity_of_issued_per_year_per_city(df_data, sqlCt):
    #df_data.select("IssuedDate").distinct().orderBy("IssuedDate").show()

    df_longevity_of_issued={}

    df_save = None
    for year in range(DATE_BEGIN, DATE_END):
        print "Year", year
        longevity = udf(lambda IssuedDate, ExpiryDate: udf_longevity(year, IssuedDate, ExpiryDate), IntegerType())
        df_data_longevity=df_data.withColumn("Longevity",
                                              longevity(df_data.IssuedDate, df_data.ExpiryDate))

        df_data_longevity.registerTempTable("business_licenses_longevity_issued_%s"%year)

        df_longevity_of_issued[year] = sqlCt.sql("""SELECT DataSetOrigin, %s as year,  BusinessType,
                                                      count(*) as NumBusiness,
                                                      sum(Longevity)/count(*) as AvgLongevity,
                                                      (sum(Longevity)/count(*))/365 as AvgLongevityYears
                                                      FROM business_licenses_longevity_issued_%s
                                                      WHERE Longevity>0 and
                                                            Longevity is not null and
                                                            Status in %s AND
                                                            IssuedYear<=%s
                                                      GROUP BY DataSetOrigin, BusinessType Order By count(*) desc"""%(year,year, ISSUED_STATUS, year))

        if df_save is None:
            df_save = df_longevity_of_issued[year]
        else:
            df_save=df_save.unionAll(df_longevity_of_issued[year])

    #df_save.coalesce(1).write.format("csv").options(header=True)\
    #        .save("out_data/"+out+"/longevity_of_issued_per_year", mode="overwrite")


    return df_save



def longevity_per_year(df_data, sqlCt, out):

    #df_data.select("IssuedDate").distinct().orderBy("IssuedDate").show()

    df_longevity_year = {}
    df_save = None
    for year in range(DATE_BEGIN, DATE_END):
        print "Year", year

        longevity = udf(lambda IssuedDate, ExpiryDate: udf_longevity(year, IssuedDate, ExpiryDate), IntegerType())



        df_data_longevity=df_data.withColumn("Longevity",
                                                      longevity(df_data.IssuedDate, df_data.ExpiryDate))


        df_data_longevity.registerTempTable("business_licenses_longevity_%s"%year)


        df_longevity_year[year] = sqlCt.sql("""SELECT %s as year, BusinessType,
                                                      count(*) as NumBusiness,
                                                      sum(Longevity)/count(*) as AvgLongevityDays,
                                                      (sum(Longevity)/count(*))/365 as AvgLongevityYears
                                               FROM business_licenses_longevity_%s
                                               WHERE Longevity>0 and Longevity is not null and IssuedYear<=%s
                                               GROUP BY BusinessType Order By count(*) desc"""%(year,year,year))
        if df_save is None:
            df_save = df_longevity_year[year]
        else:
            df_save=df_save.unionAll(df_longevity_year[year])

    df_save.coalesce(1).write.format("csv").options(header=True)\
            .save("out_data/"+out+"/longevity_per_year", mode="overwrite")

    return df_save

def view_unique_status(df_data):

    df_data.select("Status").distinct().show(truncate=False)


def predict_trends(df_target, pipe_model, model):

    Types=df_target.select("BusinessType").distinct().coalesce()[:]
    Cities=df_target.select("DataSetOrigin").distinct().coalesce()[:]

    rows=[]
    for year in range(2017,2027):
    	for Type in Types:
            for city in Cities:
            	rows.append(Row(Year=year, BusinessType=Type, DataSetOrigin=city, prediction=None))

    df_predictions=sc.parellelize(rows).toDF()
    df_transformed=pipe_model.transform(df_predictions)
    
    df_results = model.transform(df_transformed)

    
    df_results.coalesce(1).write.format("csv").options(header=True)\
            .save("out_data/"+predictions+"/trends", mode="overwrite")

    df_results.show(n=50, truncate=False)
        
 


def regression(df_target, sc):


    stringIndexer_city = StringIndexer(inputCol="DataSetOrigin", outputCol="DataSetOriginIndex")
    stringIndexer_btype = StringIndexer(inputCol="BusinessType", outputCol="BusinessTypeIndex")
    encoder = OneHotEncoder(inputCol="BusinessTypeIndex", outputCol="BusinessTypeHot")
    assembler = VectorAssembler(inputCols=["DataSetOriginIndex", "BusinessTypeHot", "year", "NumBusiness"],outputCol="VectorFeatures")
    normalizer = Normalizer(inputCol="VectorFeatures", outputCol="norm_features")

    polyExpansion = PolynomialExpansion(inputCol="norm_features", outputCol="features")
    #lr = LinearRegression(maxIter=REG_INT, featuresCol="features", labelCol="AvgLongevity")
    lr = LinearRegression(maxIter=REG_INT, featuresCol="norm_features", labelCol="AvgLongevity")
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.3]) \
        .addGrid(lr.elasticNetParam, [0.8]) \
        .addGrid(polyExpansion.degree, [2]) \
        .build()

    pipeline = Pipeline(stages=[stringIndexer_city, stringIndexer_btype, encoder, assembler, normalizer])


    crossval = CrossValidator(estimator=Pipeline(stages=[lr]),
                              estimatorParamMaps=paramGrid,
                              evaluator=RegressionEvaluator(metricName="rmse", labelCol="AvgLongevity"),
                              numFolds=2)

    print "Transforming features..."
    pipe_model = pipeline.fit(df_target)
    df_transformed=pipe_model.transform(df_target)
    print "Fiting regression"
    model = crossval.fit(df_transformed.select("norm_features", "AvgLongevity"))
    print "Predicting"
    predictions = model.transform(df_transformed)
    predictions.show(n=100, truncate=False)


    print "Evaluating Root mean square"
    evaluator = RegressionEvaluator(labelCol="AvgLongevity", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on train data = %g" % rmse)

    print "Evaluating Mean absolute error"
    evaluator = RegressionEvaluator(labelCol="AvgLongevity", predictionCol="prediction", metricName="mae")
    mae = evaluator.evaluate(predictions)
    print("Mean absolute error (mae) on train data = %g" % mae)
    model.bestModel.summary.show()
    
#    predict_trends(df_target, pipe_model, model,sc)


def main():
    print"Starting"
    conf = SparkConf().setAppName('Business Licenses')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df_data = load_dataset(FILE, sqlContext)
#    df_van_data=df_data.where("DataSetOrigin=='Van'").cache()
#    df_tor_data = df_data.where("DataSetOrigin=='Tor'").cache()
    #df_data.show()



    #df_opens_year=opens_per_year(df_data, "all")
    #df_closes_year=closes_per_year(df_data, "all")

#    df_opens_year=opens_per_year(df_van_data, "van")
#    df_closes_year=closes_per_year(df_van_data, "van")

#    df_opens_year=opens_per_year(df_tor_data, "tor")
#    df_closes_year=closes_per_year(df_tor_data, "tor")



    #df_longevity_year=longevity_per_year(df_data, 'all')
    #df_longevity_year[2016].show()
    #df_longevity_of_closed_per_year=longevity_of_closed_per_year(df_data, "all")
    #df_longevity_of_closed_per_year[2016].show()
    #df_longevity_of_issued_per_year=longevity_of_issued_per_year(df_data, sqlCt, "all")
   
    df_longevity_of_issued_per_year_per_city=longevity_of_issued_per_year_per_city(df_data, sqlContext)
    regression(df_longevity_of_issued_per_year_per_city,sc)
    #df_longevity_of_issued_per_year[2016].show()

    #df_longevity_year = longevity_per_year(df_van_data, 'van')
    #df_longevity_of_closed_per_year = longevity_of_closed_per_year(df_van_data, "van")
    #df_longevity_of_issued_per_year = longevity_of_issued_per_year(df_van_data, "van")

    #df_tor_data.show()
    #df_longevity_year = longevity_per_year(df_tor_data, 'tor')
    #df_longevity_of_closed_per_year = longevity_of_closed_per_year(df_tor_data, "tor")
    #df_longevity_of_issued_per_year = longevity_of_issued_per_year(df_tor_data, "tor")


    #Plot(filter(lambda a: a[CITY]=='Vancouver' ,oyl))


if __name__ == "__main__":
    main()












