import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat

 object WebLogPractice {
    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      System.setProperty("hadoop.home.dir", "C:\\bin")

      def ConvertToLong(timestamp:String)= {
        val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val dt = df.parse(timestamp)
        val epoch = dt.getTime
        epoch
      }

      val f = udf(ConvertToLong _)


      // create a spark object and will be running on local machine
      val spark=SparkSession.builder().appName("PAYPAY CHALLENGE").master("local[*]").getOrCreate()

      //read the log file as csv with " " as delimiter
      val df=spark.read.option("delimiter"," ").csv("data/2015_07_22_mktplace_shop_web_log_sample.log")

      //reading the default required columns from dataframe df and add column names(schema)
      val newDF=df.select(col("_c0"),col("_c2"),col("_c11"),col("_c12")).toDF("timestamp","userIp","url","Browser")

      // calculate epoch time in Long using udf and add it to newDF

      val webLogDF=newDF.withColumn("epoch",f(col("timestamp")))

      //window function to be used
      val window = Window.partitionBy("userIp").orderBy("epoch")

      //create a lag column as prevEpoch
      val webLogDfEpoch= webLogDF.withColumn("prevEpoch",lag(webLogDF("epoch"), 1).over(window))

      //to filter the null epoch values
      val webLogDfEpochCln= webLogDfEpoch.withColumn("prevEpoch_cln", coalesce(col("prevEpoch"),col("epoch")))

      // calculate the session time between previous and present in ms
      val webLogDfDuration= webLogDfEpochCln.withColumn("duration_ms",col("epoch")-col("prevEpoch_cln"))

      //checking for new sessions as keeping each session as default 15 minutes (i.e 900000 milli seconds)
      val webLogDfNewSession= webLogDfDuration.withColumn("isNewSession",when(col("duration_ms")>900000,1).otherwise(0))

      //creating an index column later to append into sessionid
      val webLogDfIndex=webLogDfNewSession.withColumn("SessionIndex",sum("isNewSession").over(window).cast("string"))

      //creating sessionId for new session by concatenating user IP and previously calculated Index column
      val webLogSessionId=webLogDfIndex.withColumn("sessionId",concat(col("userIp"),col("SessionIndex")))

      //select required column to display/output and cache it in memory for repetitive use
      val cacheWebLogDf=webLogSessionId.select(col("userIp"),col("sessionId"),col("duration_ms"),col("url")).cache()

      //1.Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.

      //    cacheWebLogDf.show()

      cacheWebLogDf.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").mode("overwrite").save("C:\\data\\aggregation.csv")

      //2. Determine the average session time
      //   cacheWebLogDf.select(mean("duration_ms")).show()
      cacheWebLogDf.select(mean("duration_ms")).coalesce(1).write.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").mode("overwrite").save("C:\\data\\average_session.csv")

      //3.Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
      //Spark SQL collect_list() and collect_set() functions are used to create an array (ArrayType) column on DataFrame by merging rows, typically after group by or window partitions
      //set removes duplicate urls
      val setOfUrlPerSession=cacheWebLogDf.select("sessionId","url").groupBy("sessionId").agg(collect_set("url").alias("urlSet"))
      //size of the set column gives unique counts per sessionId
      val UniqueUrlVisitsPerSession= setOfUrlPerSession.select(col("sessionId"),size(col("urlSet")).alias("UniqueUrlCount"))
      UniqueUrlVisitsPerSession.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").mode("overwrite").save("C:\\data\\unique_url_visits.csv")

      //4.Find the most engaged users, ie the IPs with the longest session times

      val mostEngagedUsers=cacheWebLogDf.groupBy("userIp").sum("duration_ms").sort(col("sum(duration_ms)").desc).filter(col("sum(duration_ms)")=!=0)

      mostEngagedUsers.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").mode("overwrite").save("C:\\data\\longest_session_IPs.csv")


    }
}
