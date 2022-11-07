package lab4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class ScalaWordCount {

}

object ScalaWordCount{
  def main(args:Array[String]):Unit={
    var list = List(
      "Hello, I'm Diana",
      "雪 豹 闭 嘴",
      "Hello, I'm Carol",
      "Hello, I'm AVA"
    )

    var sparkConf = new SparkConf().setAppName("word-count").setMaster("yarn");
    var sc = new SparkContext(sparkConf);
    var lines:RDD[String] = sc.parallelize(list)
    var words:RDD[String] = lines.flatMap((line:String)=>{line.split(" ")})
    var wordAndOne:RDD[(String, Int)] = words.map((word:String)=>{(word, 1)})
    var wordAndNum:RDD[(String, Int)] = wordAndOne.reduceByKey((count1:Int, count2:Int)=>{count1 + count2})
    var  ret = wordAndNum.sortBy(kv=>kv._2, ascending = false)
    print(ret.collect.mkString(","))
    ret.saveAsTextFile("hdfs://wx-2020213599-0001:8020/spark-test")
    sc.stop()
  }
}