// BATCH B TEAM 7
// -----------------ANALYSIS ON CROP PRODUCTION DATASET -------------------

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import breeze.plot._
import breeze.linalg._

object CROP_ANALYSIS
{
  val sc = new SparkContext("local", "term")
  def main(args:Array[String]):Unit={
    val tf = sc.textFile("C:\\Users\\ASUS\\Desktop\\crop_production.csv").map(x => x.split(",")).zipWithIndex()
      .filter(a => a._2 > 0)
      .map(_._1)

    //analysis functions

    crop_v_season(tf)
    soil_in_state(tf)
    average_state_temperature(tf)

    best_crop_in_state(tf)
    soil_for_crop(tf)
    correlation(tf,Set(3,7,12),"cotton")
    correlation(tf,Set(3,9,12),"cotton")

    best_crop_producer(tf)
    area_of_land_for_soil(tf)

    chemical_usage(tf)
    state_crop_area_allocation(tf)
    average_state_rainfall(tf)


  }
  //columns dropping function
  def give_columns(rdd: RDD[Array[String]], x: Set[Int]): RDD[Array[String]] = {
    var newData = rdd.map(row => row.zipWithIndex.filter(a => x.contains(a._2)).map(_._1))
    return newData
  }

  def crop_v_season(rdd: RDD[Array[String]]): Unit = {
    val rdd1 = give_columns(rdd, Set(2, 3, 12)).map(x=>(x(0),(x(1),(x(2).toDouble,1.toDouble))))
      .groupByKey().collect()
      .map(x=>(x._1,sc.parallelize(x._2.toList)
      .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .map(x=>(x._1,x._2._1/x._2._2)).sortBy(_._2,ascending = false).collect()))
    rdd1.foreach(x=>println(x._1+" "+x._2.mkString(" ")))
  }

  def soil_in_state(rdd: RDD[Array[String]]): Unit = {
    val rdd1 = give_columns(rdd, Set(1, 7, 10)).map(x => (x(0), (x(1).toDouble, BigInt(x(2)))))
      .map { case (a, b) => if (b._1 >= 4.8 && b._1 < 5.2) {
        ("soil1", (a, b._2))
      }
      else if (b._1 >= 5.2 && b._1 < 5.6) {
        ("soil2", (a, b._2))
      }
      else if (b._1 >= 5.6 && b._1 < 6) {
        ("soil3", (a, b._2))
      }
      else {
        ("soil4", (a, b._2))
      }
      }.groupByKey().collect()
    val rdd2 = rdd1.map(x => (x._1, x._2.toList))
      .map(x => (x._1, sc.parallelize(x._2)
        .reduceByKey(_ + _).sortBy(_._2, ascending = false).collect()))
    rdd2.foreach(x => println(x._1 + " : " + x._2.mkString(" ")))
  }
  def average_state_temperature(rdd: RDD[Array[String]]): Unit = {
    val rdd1 = give_columns(rdd, Set(1, 9)).map(x => (x(0), (x(1).toDouble, 1.toDouble)))
      .groupByKey().collect()
      .map(x => (x._1, sc.parallelize(x._2.toList).reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .map((x => (x._1, x._2._1 / x._2._2))).sortBy(_._2)
    rdd1.foreach(x => println(x._1 + " : " + x._2))
    val input_data = rdd1.map(x => (x._2.toInt, 1))
    val in = sc.parallelize(input_data).reduceByKey(_ + _).collect()
    val x = in.map(x => x._1)
    val y = in.map(x => x._2)
    val f = Figure()
    val plt = f.subplot(0)
    val xVector = DenseVector(x: _*)
    val yVector = DenseVector(y: _*)
    plt += scatter(xVector, yVector, _ => 0.3)
    f.refresh()
  }

  def soil_for_crop(rdd: RDD[Array[String]]): Unit = {
    val rdd1 = give_columns(rdd, Set(3, 7, 12)).map(x => (x(0), (x(1).toDouble, x(2).toDouble)))
      .map { case (a, b) =>
        if (b._1 >= 4.8 && b._1 < 5.2) {
          (a, (("soil1"), (b._2, 1.00)))
        }
        else if (b._1 >= 5.2 && b._1 < 5.6) {
          (a, (("soil2"), (b._2, 1.00)))
        }
        else if (b._1 >= 5.6 && b._1 < 6) {
          (a, (("soil3"), (b._2, 1.00)))
        }
        else {
          (a, (("soil4"), (b._2, 1.00)))
        }
      }.groupByKey().collect()
    val rdd2 = rdd1.map(x => (x._1, x._2.toList))
      .map(x => (x._1, sc.parallelize(x._2)
        .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }.map(a => (a._1, (a._2._1.toFloat / a._2._2.toFloat))).sortBy(_._2, ascending = false).collect()))
    rdd2.foreach(x => println(x._1 + "  :  " + x._2.mkString(" ")))
  }

  def best_crop_in_state(rdd:RDD[Array[String]]):Unit={
    val rdd1=give_columns(rdd,Set(1,3,11)).map(x=>(x(0),x(1),BigInt(x(2))))
    val rdd2 = rdd1.groupBy(w => w._1)
      .map { case (a, b) => (a, b.toArray) }
      .map { case (a, b) => (a, b.map(x => (x._2, x._3))) }.collect()
    val rdd3 = rdd2.map(x =>
      (x._1, sc.parallelize(x._2).reduceByKey(_ + _).sortBy(_._2, ascending = false).collect())
    )
    rdd3.foreach(x => println(x._1 + " : " + x._2.mkString("  ")))
  }


  def correlation(rdd: RDD[Array[String]], index: Set[Int],cn:String): Unit = {
    val rdd1 = give_columns(rdd, index).filter(a=>a(0)==cn).map(x=>(x(1).toDouble,x(2).toDouble))
    val x = rdd1.map(x => x._1).collect()
    val y = rdd1.map(x => x._2).collect()
    val xstdv = sc.parallelize(x).stdev()
    val ystdv = sc.parallelize(y).stdev()
    val n = x.length
    val xSum = x.sum
    val ySum = y.sum
    val xySum = x.zip(y).map { case (xi, yi) => xi * yi }.sum
    val numerator = (xySum - (xSum * ySum) / n) / n - 1
    val denominator = xstdv * ystdv
    val correlation_coefficient = numerator / denominator
    println(s"Correlation Coefficient: $correlation_coefficient")
  }

  def chemical_usage(rdd: RDD[Array[String]]): Unit = {
    val rdd1 = give_columns(rdd, Set(1, 4, 5, 6, 10))
      .map(x => (x(0), (BigInt(x(1)) * BigInt(x(4)), BigInt(x(2)) * BigInt(x(4)), BigInt(x(3)) * BigInt(x(4)))))
      .groupByKey().map { case (a, b) => (a, b.reduce((i, j) => (i._1 + j._1, i._2 + j._2, i._3 + j._3))) }.collect()
    println("state  nitrogen  phosphorus  potassium")
    rdd1.foreach(x => println(x._1 + "  " + (x._2)))
  }

  def best_crop_producer(rdd: RDD[Array[String]]):Unit={
    val rdd1 = give_columns(rdd, Set(1, 3, 11)).map(x => (x(1),x(0), BigInt(x(2))))
    val rdd2 = rdd1.groupBy(w => w._1)
      .map { case (a, b) => (a, b.toArray) }
      .map { case (a, b) => (a, b.map(x => (x._2, x._3))) }.collect()
    val rdd3 = rdd2.map(x =>
      (x._1, sc.parallelize(x._2).reduceByKey(_ + _).sortBy(_._2, ascending = false).collect())
    )
    rdd3.foreach(x => println(x._1 + " : " + x._2.head))
  }

  def area_of_land_for_soil(rdd: RDD[Array[String]]): Unit = {
    val rdd1 = give_columns(rdd, Set(1, 7, 10)).map(x => (x(0), (x(1).toDouble, BigInt(x(2)))))
      .map { case (a, b) => if (b._1 >= 4.8 && b._1 < 5.2) {
        (a, ("soil1", b._2))
      }
      else if (b._1 >= 5.2 && b._1 < 5.6) {
        (a, ("soil2", b._2))
      }
      else if (b._1 >= 5.6 && b._1 < 6) {
        (a, ("soil3", b._2))
      }
      else {
        (a, ("soil4", b._2))
      }
      }.groupByKey().collect()
    val rdd2 = rdd1.map(x => (x._1, x._2.toList))
      .map(x => (x._1, sc.parallelize(x._2)
        .reduceByKey(_ + _).sortBy(_._2, ascending = false).collect()))
    rdd2.foreach(x => println(x._1 + " : " + x._2.mkString(" ")))
  }


  def state_crop_area_allocation(rdd: RDD[Array[String]]): Unit = {
    val rdd1 = give_columns(rdd, Set(1, 3, 10)).map(x => (x(0), x(1), BigInt(x(2))))
    val rdd2 = rdd1.groupBy(w => w._1)
      .map { case (a, b) => (a, b.toArray) }
      .map { case (a, b) => (a, b.map(x => (x._2, x._3))) }.collect()
    val rdd3 = rdd2.map(x =>
      (x._1, sc.parallelize(x._2).reduceByKey(_ + _).sortBy(_._2, ascending = false).collect())
    )
    rdd3.foreach(x => println(x._1 + " : " + x._2.mkString("  ")))
  }

  def average_state_rainfall(rdd:RDD[Array[String]]):Unit={
    val rdd1 = give_columns(rdd, Set(1, 2, 8))
      .map(x=>(x(1),(x(0),x(2).toDouble)))
      .groupByKey().collect()
      .map{case(a,b)=>(a,sc.parallelize(b.map(x=>(x._1,(x._2,1.toDouble))).toList)
        .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
        .map(x=>(x._1,x._2._1/x._2._2)).sortBy(_._2,ascending = false).collect())}
    rdd1.foreach(x=>println(x._1+" "+x._2.mkString(" ")))
  }


}
