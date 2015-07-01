import org.apache.spark._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import SparkContext._
import org.apache.spark.rdd.PairRDDFunctions

/**
 * Created by fzj on 2015/3/21.
 */
object MySeismic {

  def main(array: Array[String]): Unit ={
    if(array.length == 0){
      println("Please specify a command!")
      println("Help for -help command")
      sys.exit()
    }
    val commandLength = array.length//Command Length
    val conf = new SparkConf().setAppName("Seismic")
    val sc = new SparkContext(conf)
    if(array(0).equals("merge")){
      if(commandLength < 5) {
        println("Lack of Parameters!")
        sys.exit()
      }
      if(array(1).equalsIgnoreCase("Horizon")||array(1).equalsIgnoreCase("Interval")){
        var file = sc.textFile(array(2))//open the 1st file
        val head = new PairRDDFunctions(file.zipWithIndex().map(s=>(s._2,s._1)))//build PairRDD
        file = sc.textFile(array(3))//open the 2nd file
        var value = file.map(s=>s.split(" +")(4)).zipWithIndex().map(s=>(s._2,s._1))
        var result = new PairRDDFunctions(head.join(value).sortByKey()
                                              .map(s=>(s._1,s._2.toString().replace("[","").replace("]","")
                                              .replace("(","").replace(")","").replace(","," "))))
        if(commandLength != 5) {
          for (i <- 1 to commandLength - 5) {
            file = sc.textFile(array(i + 3)) //open other files
            value = file.map(s => s.split(" +")(4)).zipWithIndex().map(s => (s._2, s._1))
            result = new PairRDDFunctions(result.join(value).sortByKey()
                                                .map(s=>(s._1,s._2.toString().replace("[","").replace("]","")
                                                .replace("(","").replace(")","").replace(","," "))))
          }
        }
        result.values.saveAsTextFile(array(commandLength-1))
      }
    }
    //-merge command
    //type file1 file2 ... desFile
/*    if(array(0).equals("merge")) {
      if(commandLength < 5) {
        println("Lack of Parameters!")
        sys.exit()
      }
      //Merge Horizon or Interval type
      if(array(1).equalsIgnoreCase("Horizon")||array(1).equalsIgnoreCase("Interval")){
        //Start Get Head
        var file = sc.textFile(array(2))//Open the 1st File
        val col1 = file.map(s => s.split(" +")(1))//Get the 1st Column
        val col2 = file.map(s => s.split(" +")(2))//Get the 2nd Column
        var head_tmp = col1.zip(col2)//Zip 2 Columns
        var head = head_tmp.map(s => s.toString().replace("[","").replace("]","").replace("(","")
            .replace(")","").replace(","," "))
        //Zip the 3rd,4th Column
        for(i<- 3 to 4) {
          val col = file.map(s => s.split(" +")(i))//Get the 3rd,4th Column
          head_tmp = head.zip(col)//Zip
          head = head_tmp.map(s => s.toString().replace("[", "").replace("]", "").replace("(", "")
            .replace(")", "").replace(",", " "))
        }
        //End Zip the 3rd,4rd Column
        //End Get Head
        //Start Get Data
        //Merge the Beginning 2 Files
        val value1 = file.map(s => s.split(" +")(5))//Get the 5th Column of the 1st File
        value1.saveAsTextFile(array(commandLength-1)+"value1")
        println(value1.count())
        println(value1.partitions.size)
        file = sc.textFile(array(3))//Get the 2nd File
        val value2 = file.map(s => s.split(" +")(5))//Get the 5th Column of the 2nd File
        println(value2.count())
        println(value2.partitions.size)
        value2.saveAsTextFile(array(commandLength-1)+"value2")
        var values_tmp = value1.zip(value2)//Zip 2 Columns
        println(values_tmp.count())
        var values = values_tmp.map(s => s.toString.replace("[","").replace("]","").replace("(","")
            .replace(")","").replace(","," "))
        //println(values.count())
        //Merge more than 2 Files
        if(commandLength != 5) {
          for (i <- 1 to commandLength - 5) {
            file = sc.textFile(array(3 + i))
            val value_col = file.map(s => s.split(" +")(5))
            values_tmp = values.zip(value_col)
            values = values_tmp.map(s => s.toString.replace("[", "").replace("]", "").replace("(", "")
              .replace(")", "").replace(",", " "))
          }
        }
        //End Merge more than 2 Files
        val result = head.zip(values)//Zip head and values
        val final_result = result.map(s => s.toString().replace("[","").replace("]","").replace("(","")
            .replace(")","").replace(","," "))
        final_result.saveAsTextFile(array(commandLength-1)+"MergeResult")
      }
    }*/
    //-st command
    else if(array(0).equals("st")){
      if(array(1).equalsIgnoreCase("Horizon")||array(1).equalsIgnoreCase("Interval")){
        val file = sc.textFile(array(3))
        //MinMax standardize
        if(array(2).equalsIgnoreCase("mm")) {
          val value = file.map(s => s.split(" +")(5)).filter(s => (!s.equalsIgnoreCase(array(4)))).map(_.toDouble) //delete the nan
          val max_value = value.max() //max value
          val min_value = value.min() //min value
          println(max_value)
          println(min_value)
          val stValue = file.map(s=>{val tmp = s.split(" +")
             val value = tmp(5)
             if(!value.equalsIgnoreCase(array(4))){
               tmp(1)+" "+tmp(2)+" "+tmp(3)+" "+tmp(4)+" "+(value.toDouble-min_value)/(max_value-min_value)
             }
             else{
               tmp(1)+" "+tmp(2)+" "+tmp(3)+" "+tmp(4)+" "+value
             }
          })
          stValue.saveAsTextFile(array(5))
        }
        //Z-Score standardize
        else if(array(2).equalsIgnoreCase("zs")){
          val value = file.map(s => s.split(" +")(5)).filter(s=>(!s.equalsIgnoreCase(array(4)))).map(_.toDouble).cache()
          val count = value.count()
          println(count)
          val sum = value.reduce((x,y)=>x+y)
          println(sum)
          val average = sum/count
          println("average:"+average)
          val se = scala.math.sqrt(value.map(v => (v-average)*(v-average)).reduce((x,y)=>x+y)/count)
          println("se:"+se)
          val stValue = file.map(s=>{val tmp = s.split(" +")
            val value = tmp(5)
            if(!value.equalsIgnoreCase(array(4))){
              tmp(1)+" "+tmp(2)+" "+tmp(3)+" "+tmp(4)+" "+(value.toDouble-average)/se
            } else {
              tmp(1)+" "+tmp(2)+" "+tmp(3)+" "+tmp(4)+" "+value
            }
          })
          stValue.saveAsTextFile(array(5))
        }
      }
    }
    //-fs command
    //fs_name
    else if(array(0).equals("fs")){
      val origin_data = sc.textFile(array(2))
      val origin_values = origin_data.map(s=>(for{i<-4 to s.split(" +").length-1}
        yield s.split(" +")(i).toDouble)).map(_.toArray)
      //      val origin_values = origin_data.map(s=>(for{i<-4 to s.split(" +").length-1}
      //                                              yield s.split(" +")(i).toDouble)).map(_.toArray).map(Vectors.dense(_))
      //      val origin_head = origin_data.map(s=>(for{i<-0 to 3}
      //                                            yield s.split(" +")(i).toDouble)).map(_.toArray).map(Vectors.dense(_))
    }
    //-fe command
    else if(array(0).equals("fe")){

    }
    //-cs command
    else if(array(0).equals("cs")){
      val file = sc.textFile(array(2))//read data
      //KMeans
      if(array(1).equalsIgnoreCase("Kmeans")){
        val data = file.map(s=>{val line = s.split(" +")
            val att_num = s.split(" +").length-4
            var value = line(4)
            for(i<-2 to att_num)value=value+" "+line(3+i)
            (line(0)+" "+line(1)+" "+line(2)+" "+line(3),value)})
        val indexedLine = data.zipWithIndex().map(s=>(s._2,s._1))
        val notNanLine = indexedLine.filter(s=>(!s._2._2.matches("^.*-99999.*$"))).cache()//Line without NaN
        //notNanLine.saveAsTextFile(array(5)+"notNanLine")//for Debug
        val nanLine = indexedLine.filter(s=>s._2._2.matches("^.*-99999.*$"))
                                 .map(s=>(s._1,(s._2._1,s._2._2+" NaN"))).cache()//(Long,(String,String)) Line with NaN
        //nanLine.saveAsTextFile(array(5)+"nanLine")//for Debug
        val value = notNanLine.map(s=>{val value = s._2._2  //value to run KMeans
                                       for(i<-0 to value.split(" +").length-1) yield value.split(" +")(i).toDouble})
                              .map(_.toArray).map(Vectors.dense(_)).cache()
        val predicts = KMeans.train(value, array(3).toInt, array(4).toInt).predict(value)
        val result = notNanLine.zip(predicts).map(s=>(s._1._1,(s._1._2._1, s._1._2._2+" "+s._2))) //((Long,(String,String)),Int)
        val final_result = result.union(nanLine).sortByKey()
        final_result.values.map(s=>s._1+" "+s._2).saveAsTextFile(array(5))
      }
/*      if(array(1).equalsIgnoreCase("KMeans")){
        val origin_values = origin_data.map(s=>for{i<-4 to s.split(" +").length-1}
          yield s.split(" +")(i).toDouble).map(_.toArray).map(Vectors.dense(_))
        val origin_head = origin_data.map(s=>for{i<-0 to 3}
          yield s.split(" +")(i).toDouble).map(_.toArray).map(Vectors.dense(_))
        val numClusters = array(3).toInt
        val numIterations = array(4).toInt
        val predicts = KMeans.train(origin_values, numClusters, numIterations).predict(origin_values)
        val result = origin_head.zip(predicts).map(line => line.toString().replace("[","").replace("]","")
                                                               .replace("(","").replace(")","").replace(","," "))
        result.saveAsTextFile(array(5)+"KMeansResult")
      }*/
    }
    //-vs command
    else if(array(0).equals("vs")){
      val value = sc.textFile(array(2)).map(s=>s.split(" +")(4))
    }
    else if(array(0).equals("exit")){
      sys.exit
    }
    else{
      println("Unknown Command!");
      sys.exit
    }
  }
}