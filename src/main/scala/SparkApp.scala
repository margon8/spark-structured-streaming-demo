import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

trait SparkApp extends App with LazyLogging {

  def sparkAppName: String

  def execute(): Unit

  val spark = SparkSession.builder.appName(sparkAppName).getOrCreate()

  try {
    logger.info(s"Launching $sparkAppName with args $args")
    execute()
  } catch {
    case e: Exception =>
      logger.info(s"$sparkAppName crashed with error ${e.getLocalizedMessage}")
      throw e
  } finally {
    logger.info(s"$sparkAppName fininshed succesfully")
    spark.stop()
  }


}

