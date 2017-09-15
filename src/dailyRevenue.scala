
/* Problem Statement
  Calculate total revenue per day for all completed and closed orders
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.typesafe.config._
import org.apache.hadoop.fs.{FileSystem,Path}

object dailyRevenue {

  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setMaster("local").setAppName("Daily Revenue")
    val sc = new SparkContext(conf)

    val ipPath = args(0)
    val opPath = args(1)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val op = new Path(opPath)

    if(!fs.exists(new Path(ipPath)))
      println("Invalid Input path")

    if(fs.exists(op))
      fs.delete(op, true)

    val orders= sc.textFile(ipPath+"/orders")
    val order_items=sc.textFile(ipPath+"/order_items")
   /*
        val orders = sc.textFile("file:///C:/Users/Mahesh/Desktop/data/retail_db/orders/part-00000")
        val order_items= sc.textFile("file:///C:/Users/Mahesh/Desktop/data/retail_db/order_items/part-00000")
 */
    /* Setting up hadoop Binaries*/
    System.setProperty("hadoop.home.dir", "C:/winutils/")


    val orders_filter= orders.filter(rec=> rec.split(",")(3)=="COMPLETE" || rec.split(",")(3)=="CLOSED")
    val orders_map = orders_filter.map(rec=>(rec.split(",")(0).toInt,rec.split(",")(1)))
    val orditm_filter= order_items.map(rec=>(rec.split(",")(1).toInt,rec.split(",")(4).toDouble))

    val orders_join= orders_map.join(orditm_filter)

    val orders_join_map= orders_join.map(rec=>rec._2)

    //val orders_join_map_rbk= orders_join_map.reduceByKey(_+_).sortByKey().map(rec=> rec.productIterator.mkString("\t")).saveAsTextFile("file:///C:/Users/Mahesh/Desktop/output_dailyRevenue")
    val orders_join_map_rbk= orders_join_map.reduceByKey(_+_).sortByKey().map(rec=> rec.productIterator.mkString("\t")).saveAsTextFile(opPath)


    //orders_join_map_rbk.take(10).foreach(println)
    //orders_join_map_rbk.saveAsTextFile("file:///C:/Users/Mahesh/Desktop/output_dailyRevenue/")

  }
}
