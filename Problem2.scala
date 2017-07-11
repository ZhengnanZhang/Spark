package comp9313.ass3
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object Problem2{
 
 
 
 
  def main(args:Array[String]){
    val inputFile = args(0)
    val outputFile = args(1)
    val topPair = args(2)
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)
    //first step split the line and convert every String to lowercase and then filter the blank line and the String not a word
    val textFile = sc.textFile(inputFile).map(x => x.split("[\\s*$&#/\"'\\,.:;)?!\\[\\](){}<>~\\-_]+").map(x => x.toLowerCase()).filter{ x =>x.length()>0 &&  x.charAt(0).isLetter  })
    //using flatMap makes all documents pair become ((word1,word2),1)
    val first = textFile.cache()
    val second = first.flatMap{x=>
    for{
         i<-0 until x.length
         j<- (i+1) until x.length
    }yield{
      if(x(i)<x(j)){
      ((x(i),x(j)),1)}
      else
        {((x(j),x(i)),1)}
    }
    }
    //reduced by Key 
    val third = second.reduceByKey(_+_)
    //get the first n-top pairs
    val forth = third.map(_.swap).sortByKey(false).map(_.swap).take(topPair.toInt)
    val result1 = forth.map(x=>(x._1._1+","+x._1._2+"\t"+x._2))
    //convert to RDD
    val result2 = sc.makeRDD(result1)  
    //save as a text File
    result2.saveAsTextFile(outputFile)


  

  }
}