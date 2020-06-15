import org.apache.spark.sql.SparkSession

object MergeFiles {
  def main(args: Array[String]): Unit = {
    //hadoop home
    System.setProperty("hadoop.home.dir","C:\\hadoop")
    // create spark session
    val spark = SparkSession
      .builder()
      .appName("MergeFiles1")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val input = Seq(("001","first"),("002","second"))
    val df=spark.createDataFrame(input).toDF("id","text").createOrReplaceGlobalTempView("df")
    spark.sql("select * from df").show()
    //end spark session
    spark.stop()
  }
}
