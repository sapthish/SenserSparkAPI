import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, desc, input_file_name, max, min}

object sensorDetails extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name","DataFrame")
  sparkConfig.set("spark.master","local[*]")

  val spark =  SparkSession
                .builder()
                .config(sparkConfig)
                .getOrCreate()
  /**
   * Kindly provide the path of the directory where .csv file are placed
   */
  val sensor = spark
                .read
                .option("header",true)
                .option("inferSchema",true)
                .csv("/Users/sapthish/Downloads/sensor/*.csv")

  val noOfInputFiles = sensor.select(input_file_name()).distinct.count
  val sensorWithNaN = sensor.where("humidity != 'NaN'").cache()
  val sensorWithoutNaN = sensor.where("humidity == 'NaN'").cache()

  /**
   * Join will give sensor info which has only NaN Humidity
   */

  val uniqueSensorNaN = sensorWithoutNaN
                        .join(sensorWithNaN,sensorWithoutNaN("sensor-id") === sensorWithNaN("sensor-id"),"leftanti")
                        .distinct()

  /**
   * Calculation give min, avg and Max Humidity
   */


  val sensorWithoutNaNAgg = sensorWithNaN.repartition(4)
                                          .groupBy("sensor-id")
                                          .agg(
                                            min("humidity").as("min"),
                                            avg("humidity").as("avg"),
                                            max("humidity").as("max"))
                                          .sort(desc("avg"))


  val sensorWithNaNAgg = uniqueSensorNaN.groupBy("sensor-id")
                                          .agg(
                                            min("humidity").as("min"),
                                            avg("humidity").as("avg"),
                                            max("humidity").as("max"))

  val sensorDetails = sensorWithoutNaNAgg.union(sensorWithNaNAgg)


  println("Num of processed files: " + noOfInputFiles)
  println("Num of processed measurements: " + sensor.distinct().count)
  println("Num of failed measurements: " + sensorWithoutNaN.distinct().count)
  println("Sensors with highest avg humidity: " )

  sensorDetails.show











}
