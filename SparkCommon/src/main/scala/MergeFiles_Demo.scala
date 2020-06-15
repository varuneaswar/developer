import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer

object MergeFiles_Demo {
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
    //input folder path and data format
    val inputPath="C:\\Users\\varun\\Documents\\Varun\\Personal\\Important\\rawData\\input"
    val dataFormat="csv"
    // use merge file function
    mergeFiles(spark,inputPath,dataFormat)

    //define the mergeFiles function
    def mergeFiles(sparkSession: SparkSession,inputPath:String,dataFormat:String): Unit = {
      var fileSize = 0L
      var Flag = false
      var datFileCount = 0
      var commonPathSize = 0L
      val fsystem = FileSystem.get(new Configuration())
      val srcPath = new Path(inputPath)
      val fileNames = fsystem.listFiles(srcPath,true)
      val folderPathList = new ListBuffer[String]
      while (fileNames.hasNext()) {
        val f = fileNames.next()
        val fname = f.toString
        val filePath = f.getPath().toString
        val fPath = new Path(filePath)
        folderPathList += f.getPath().getParent.toString
      }
      folderPathList.toList.distinct.foreach(
        fp => {val newPath = new Path(fp)
          val fileNames = fsystem.listFiles(newPath,true)
          while (fileNames.hasNext()) {
            val f = fileNames.next()
            val fname = f.toString
            val filePath = f.getPath().toString
            val fPath = new Path(filePath)
            if (f.isFile) {
              val len = fsystem.getFileStatus(fPath).getLen
              fileSize = fileSize + len
              Flag = true
              datFileCount = datFileCount + 1
            }
          }
            println("total file size" +fileSize)
            fileSize = (fileSize -(commonPathSize*datFileCount))
            val fixedSize = (64*1024*1024)
            val numOfFiles = (fileSize < fixedSize) match {
              case true => 1
              case false => Math.round(fileSize/fixedSize)
            }
            println("numOfFiles" +numOfFiles.toString)
          println("file path name " +fp)
            val tmpLoc= fp + "_stg1"
            val tmpPath = new Path(tmpLoc)
            Flag match {
              case true => if (dataFormat=="orc"){sparkSession.read.orc(fp).coalesce(numOfFiles).write.orc(tmpLoc)}
              else if (dataFormat=="parquet"){sparkSession.read.parquet(fp).coalesce(numOfFiles).write.parquet(tmpLoc)}
              else {sparkSession.read.csv(fp).coalesce(numOfFiles).write.csv(tmpLoc)}
              case _ => println("this is not in available data format")
            }
        fsystem.delete(newPath,true)
        fsystem.rename(tmpPath,newPath)
        }
      )

    }

    // val inputPath = Seq(("001","first"),("002","second"))
    // val df=spark.createDataFrame(inputPath).toDF("id","text")
   val df = if (dataFormat=="orc"){spark.read.orc(inputPath)}
    else if (dataFormat=="parquet"){spark.read.parquet(inputPath)}
    else {spark.read.csv(inputPath)}
   // val df=spark.read.orc(inputPath)
    println("df :" +df.count())
    df.show()
    val df1=spark.read.csv(inputPath+" - Copy")
    println("df1 :" +df1.count())
    df1.show()
    //end spark session
    spark.stop()
  }
}
