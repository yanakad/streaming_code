package com.akamai.test
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.SparkConf

import org.yaml.snakeyaml.Yaml
import org.joda.time.{DateTime, Period}

import java.io._


case class State(val count:Integer,val date:DateTime)


object UpdateStreamTest{
  
  val batchSize = 10
  
  
  def createContext(master: String, dropDirectory: String,checkpointDirectory:String) = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    
    // Create the context with a 60 second batch size
    
    val conf = new SparkConf()
             .setMaster(master)
             .setAppName("UpdateStreamTest")
             .set("spark.executor.memory", "1g")             
             .setJars(StreamingContext.jarOfClass(this.getClass));
             
    val ssc = new StreamingContext(conf, Seconds(batchSize))
    
    ssc.checkpoint(checkpointDirectory)
    println("Setting checkpoint dir "+checkpointDirectory)
    
    val updateFunc = (values: Seq[Int], state: Option[State]) => {
      val currentCount = values.foldLeft(0)(_ + _)

      val previousState = state.getOrElse(State(0,DateTime.now)).asInstanceOf[State]
      val expiry_minutes = 10 //if state is not updated for 10 min throw it away
      val threshold = new DateTime().minusMinutes(expiry_minutes)
      val result = 
      if (currentCount == 0  && previousState.date.isBefore(threshold)){
        println(s"Have not seen key for $expiry_minutes minites...dropping")
        None
      } else {
        if (currentCount == 0 ) {
           Some(previousState) //no change to state but keep around
        }
        else
          Some(State(currentCount + previousState.count,new DateTime()))
      }
      result
    }
    
    
    val lines = ssc.textFileStream(dropDirectory)
  
    println(s"Drop dir is $dropDirectory")

    val beacons = lines.map(line =>{
	     val record = line.split("\t")
	     (record(0), record(1),record(2))    
    })
    val stateDstream =
      beacons.map(x => (x, 1))
        .updateStateByKey[State](updateFunc)
    
    stateDstream.print()
    
    stateDstream.foreachRDD(
        rdd=>{
        val keys = rdd.map(_._1) .collect() 
        val sz = keys.size 
        val dst = keys.distinct.size
        val dups = dst!=sz
        
        println(s"size $sz, $dst")
        println(s"Duplicate keys $dups")
        
        if (dups){
          val dups = keys.groupBy(l => l).map(t => (t._1, t._2.length)).filter(_._2>1)
          println(" DUPS"+dups.mkString(" "))
        }
    })
  
    //stateDstream.saveAsTextFiles(checkpointDirectory, "partitions_test") 
   // println("***********SAVED TEXT FILES ***********")
    
    println("Done printing ")
    ssc    
  }
  
  
  def main(args: Array[String]) {
    var checkpointDirectory = ""
    var master = ""
    var dropDirectory = ""

    val resourceStream = new FileInputStream(new File("./properties.yaml"))
         
    val yaml = 
      try {
        new Yaml().load(resourceStream).asInstanceOf[java.util.Map[String, Any]]    
      } finally {
      resourceStream.close()
    }
   
    master = yaml.get("MASTER").asInstanceOf[String]
    dropDirectory = yaml.get("DROP_DIR").asInstanceOf[String]
    checkpointDirectory = yaml.get("CHECK_DIR").asInstanceOf[String]
    val expiry_min = yaml.get("expire_min").asInstanceOf[Int]
    
    println(s"Master $master; Drop dir $dropDirectory; CHKPT $checkpointDirectory")
        
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
       createContext(master, dropDirectory,checkpointDirectory)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}