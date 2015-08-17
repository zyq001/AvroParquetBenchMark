import java.io.IOException
import java.util

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.Log
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.{Page, PageReader, PageReadStore}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.example.ExampleInputFormat
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.spark.{SparkContext, SparkConf}
//import parquet.column.page.Page

/**
 * @author zangyq
 */
object AvroParquetBenchmark {
  var i = 0
  def count(t: Any) ={
//    println(t)
//    if(t > 100)
 i += 1
  }

  def consumeGroup(g:Group) = {
//    var Ip = g.getBinary("ip", 0)
    var city = g.getInteger("city", 0)
//    var url = g.getString("url", 0)
//    var time = g.getLong("timestamp", 0)
    city += 1
//    i += 1
//    print(url)
  }

  def consumeRecode(r:GenericRecord) = {
    var Ip = r.get("ip")
    var city = r.get("city")
    var url = r.get("url")
    var time = r.get("timestamp")
    var city2 = city.asInstanceOf[Int]
    i += 1
//    print(url)
  }

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
  var log = new Log(AvroParquetBenchmark.getClass)

@throws(classOf[IOException])
def readColumn(testFile: Path, configuration: Configuration, colsToLoad: Int) {
  val t0: Long = System.currentTimeMillis
  val schemaString: StringBuilder = new StringBuilder("message sample { required binary ip; required int32 city; required int64 timestamp; required binary url;}")
  val schema: MessageType = MessageTypeParser.parseMessageType(schemaString.toString)
  val columns: util.List[ColumnDescriptor] = schema.getColumns()
  val readFooter: ParquetMetadata = ParquetFileReader.readFooter(configuration, testFile)
  val parquetFileReader: ParquetFileReader = new ParquetFileReader(configuration, testFile, readFooter.getBlocks, schema.getColumns)
  var pages: PageReadStore = null
  do {
    pages = parquetFileReader.readNextRowGroup
    if (pages == null) {
//      break//todo: break is not supported
      return
    }
    import scala.collection.JavaConversions._
    for (col <- columns) {
      val reader: PageReader = pages.getPageReader(col)
      var page: Page = null
      do {
        page = reader.readPage
        if (page != null) {
//          val data: String = new String(page.getBytes.toByteArray, BytesUtils.UTF8)
//          var datas = data.split("\\s*\14*\15*\0+")
//          datas.foreach(count)
            page.toString
        }
      } while (page != null)
    }
  } while (pages != null)



}
  def useSparkSQL(path : String): Long ={

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "true")
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "1000000")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

    var startRead = System.currentTimeMillis()

    val parquetFile = sqlContext.parquetFile(path)

    parquetFile.registerTempTable("parquetFile")
    //    sqlContext.cacheTable("parquetFile")

    var afterRegister = System.currentTimeMillis()
    val teenagers = sqlContext.sql("SELECT city FROM parquetFile WHERE city > 100")
    //    val teenagers = sqlContext.sql("SELECT city FROM parquetFile")
    //    teenagers.cache()

    var afterSELETE = System.currentTimeMillis()

    //    teenagers.foreach(w => count(w.getInt(0) ))
    //    val countTime = teenagers.count()
    //  teenagers.foreach(count)
    //    teenagers.map(t => t(0)).collect().foreach(print)
        teenagers.map(t => (t(0), 1)).reduceByKey(_+_).collect().foreach(count)
    var afterCount = System.currentTimeMillis()
    //    log.error("#####SparkSQLRead:  " + (afterCount - startRead))

    return (afterCount - startRead)
  }

  def sparkParquet(path: String, schema: String): Long ={
    val job = Job.getInstance()
//    val schemaString: StringBuilder = new StringBuilder("message sample { required int32 city;}")
    job.getConfiguration.set(ReadSupport.PARQUET_READ_SCHEMA, schema);

    var beforParquetFile = System.currentTimeMillis()

    var pv1 = sc.newAPIHadoopFile(path, classOf[ExampleInputFormat],
      classOf[Void], classOf[Group], job.getConfiguration)
//    pv1.cache()
    ////    var pv1 = sc.hadoopFile(args(0), classOf[DeprecatedParquetInputFormat[Group]],
    ////        classOf[Void], classOf[Container[Group]], 10)
    pv1 = pv1.filter(r => r._2.getInteger("city", 0) > 100)
    //    pv1.map(x=> (x._2.getBinary("ip", 0) +x._2.getInteger("city", 0).toString +  x._2.getLong("timestamp", 0).toString,1)).reduceByKey(_+_).collect.foreach(count)
    pv1.map(x=> (x._2.getInteger("city", 0) ,1)).reduceByKey(_+_).collect.foreach(count)
    //      pv1.foreach(t => consumeGroup(t._2))
    var AvroReadStart = System.currentTimeMillis()

//    log.error("#####SparkParquetGroupRead:  " + (AvroReadStart - beforParquetFile))
    return (AvroReadStart - beforParquetFile)
  }


  def sparkAvro(path: String): Long ={
    val job = Job.getInstance()
    var readStart = System.currentTimeMillis();
    var pv = sc.hadoopFile(path, classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
      classOf[org.apache.hadoop.io.NullWritable], 10)
//    pv.cache()
    pv = pv.filter(r => r._1.datum().get("city").asInstanceOf[Int] > 100)
    pv.map(x=> (x._1.datum.get("city"),1)).reduceByKey(_+_).collect.foreach(count)
    //      pv.foreach(x => consumeRecode(x._1.datum()))
    var readEnd = System.currentTimeMillis();

    return (readEnd - readStart)
  }

  def sparkAvroNewAPI(path: String): Long ={
    val job = Job.getInstance()
    job.getConfiguration.set("mapred.max.split.size", "71964386");
    var readStart = System.currentTimeMillis();
    var pv2 = sc.newAPIHadoopFile(path, classOf[AvroKeyInputFormat[GenericRecord]],
          classOf[AvroKey[GenericRecord]], classOf[NullWritable], job.getConfiguration)
    pv2 = pv2.filter(r => r._1.datum().get("city").asInstanceOf[Int] > 100)
//    pv2.map(x=> (x._1.datum.get("city") + x._1.datum.get("ip").toString  + x._1.datum.get("timestamp"),1)).reduceByKey(_+_).collect.foreach(count)
          pv2.foreach(x => consumeRecode(x._1.datum()))
    var readEnd = System.currentTimeMillis()
//    log.error("#####AvroReadNewAPI:  " + (readEnd - readStart))
    return (readEnd - readStart)
  }

  def main(args: Array[String]) {

    val parquetFile1: Path = new Path(args(0))
    var readerStart = System.currentTimeMillis();
//    readColumn(parquetFile1, new hadoop.conf.Configuration(), 1)
    var readerEnd = System.currentTimeMillis();


    val sparkSqlTotle = useSparkSQL(args(0))
    log.error("@@@@@@@iiiiiiiiiiiiiiiiiiiiiiiii::  " + i)

    val schemaString: StringBuilder = new StringBuilder("message sample { required int32 city;}")
    val sparkParquetTotle = sparkParquet(args(0), schemaString.toString())

    val schemaString4Lines: StringBuilder = new StringBuilder("message sample { required binary ip; required int32 city; required int64 timestamp; required binary url;}")
    val sparkParquet4LinesTotle = sparkParquet(args(0), schemaString4Lines.toString())

    val sparkAvroTotle = sparkAvro(args(1))

    val sparkAvroNewAPITotle = sparkAvroNewAPI(args(1))


    log.error("#####ParquetPageReader:  " + (readerEnd - readerStart))

    log.error("@@@@@@@iiiiiiiiiiiiiiiiiiiiiiiii::  " + i)

    log.error("#####SparkSQLParquetRead:  " + sparkSqlTotle)
    log.error("#####ParquetRead:  " + sparkParquetTotle)
    log.error("#####ParquetRead4Lines:  " + sparkParquet4LinesTotle)
    log.error("#####AvroRead:  " + sparkAvroTotle)
    log.error("#####AvroNewAPIRead:  " + sparkAvroNewAPITotle)



  }

}
