import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import scala.collection.mutable.ListBuffer

object MergeFiles {
  def main(args: Array[String]): Unit = {
    //hadoop home
    System.setProperty("hadoop.home.dir","C:\\hadoop")
    // create spark session
    val spark = SparkSession
      .builder()
      .appName("MergeFiles1")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()
    val inputPath="C:\\Users\\varun\\Documents\\Varun\\Personal\\Important\\rawData\\input"
    mergeFiles(spark,inputPath)
    def mergeFiles(sparkSession: SparkSession,inputPath:String): Unit = {
      var fileSize = 0L
      var parquetFlag = false
      var dataParquetCount = 0
      var commonPathSize = 0L
      val fsystem = FileSystem.get(new Configuration())
      val srcPath = new Path(inputPath)
      val fileNames = fsystem.listFiles(srcPath,true)
      while (fileNames.hasNext()){
          val f =fileNames.next()
        val fname=f.toString
          val filePath = f.getPath().toString
          val fPath = new Path(filePath)

        //println("filePath" +filePath)
        //println("fileNames" +fname)
        if(f.isFile)
          {
            val len = fsystem.getFileStatus(fPath).getLen
            fileSize = fileSize + len
            parquetFlag = true
            dataParquetCount = dataParquetCount + 1
          }
      }
      println("total file size" +fileSize)
      fileSize = (fileSize -(commonPathSize*dataParquetCount))
      val fixedSize = (64*1024*1024)
      val numOfFiles = (fileSize < fixedSize) match {
        case true => 1
        case false => Math.round(fileSize/fixedSize)
      }
      println("numOfFiles" +numOfFiles.toString)
      val tmpLoc= inputPath + "_stg1"
      val tmpPath = new Path(tmpLoc)
      parquetFlag match {
        case true => sparkSession.read.csv(inputPath).coalesce(numOfFiles).write.orc(tmpLoc)
        case _ => println("this is not parquet")
      }
    }

   // val inputPath = Seq(("001","first"),("002","second"))
   // val df=spark.createDataFrame(inputPath).toDF("id","text")
    val df=spark.read.csv(inputPath)
    println("df :" +df.count())
    df.show()
    val df1=spark.read.orc(inputPath+"_stg1")
    println("df1 :" +df1.count())
    df1.show()
    //end spark session
    spark.stop()
  }
}
