import org.apache.spark._
import com.datastax.spark.connector._

object Application {
    def main(args: Array[String]) {

      val conf = new SparkConf(true).set("spark.cassandra.connection.host","127.0.0.1").setMaster("local").setAppName("IneedToKnow")
      val sc = new SparkContext(conf)

      val hello = sc.cassandraTable[(String, String)]("test", "hello")

      val first = hello.first

      println(first)
    }
}
