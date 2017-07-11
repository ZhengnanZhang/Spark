package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem1 {

  var PostId = 1
  var VoteTypeId = 2
  var UserId = 3

  def Question1(textFile: RDD[Array[String]]) {
	
  //Firstly map(VoteTypeId,PostId) and use distinct() to remove the duplicate PostId
    val first = textFile.map(line=>(line(2),line(1))).distinct()
  //map to (VoteTypeId,1) and then use reduceByKey to count the number of each VoteTypeId.
  //after that, we swap and the format becomes (number of VoteTypeId,VoteTypeId), then using sortByKey to sort in descending order
  //take the top 5 and then we swap again to format (VoteTypeId,numbers)
    val topId = first.map (a =>(a._1,1)).reduceByKey(_+_).map(_.swap).sortByKey(false).take(5).map(_.swap)
  //finally,using foreach(println) to print every line, and insert a "\t" between VoteTypeId and the numbers.
    val result = topId.foreach(line=>println(line._1 +"\t"+line._2))

  }
  
  def Question2(textFile: RDD[Array[String]]) {
	//fill your code here, print out the information (foreach(println))
  //first step, we select the only lines that the VoteTypeId is 5
    val fPostGroup = textFile.filter(line =>line(2) == "5")
  //after filter origin input, we map to (PostId,[UserId]), because we want to generate a total list
  //for each PostId in the following reduce step, and after the map we remove the duplicate UserId 
  //for the same PostId
    val fPostGroup2 = fPostGroup.map(line=>(line(1).toInt,List(line(3).toInt))).distinct()
  //reduceByKey and generate the format of (PostId,[UserId,UserId,...])
    val result = fPostGroup2.reduceByKey((a,b)=>a:::b)
  //find the PostId that contains morn than 10 users and sort the PostId in ascending order
    val result2 = result.filter(a=>a._2.size>10).sortByKey(true)
  //after that, we sort the list for each PostId in ascending order
    val result3 = result2.map(b=>(b._1,b._2.sorted))
  //finally we print the format(PostId#UserId,UserId,...)
    val result4 = result3.foreach(line=>println(line._1 + "#" +line._2.mkString(",")))
    
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile).map(_.split(","))
    println("Question 1 Answer:")
    Question1(textFile)
    println("Question 2 Answer:")
    Question2(textFile)
  }
}
