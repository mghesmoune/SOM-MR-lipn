package org.lipn.som.som

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Vector
import org.lipn.som.global.AbstractModel
import org.lipn.som.global.AbstractModel
import org.lipn.som.global.AbstractPrototype
import org.lipn.som.global.AbstractTrainer
import org.lipn.som.utils.NamedVector
import org.lipn.som.utils.DataGenerator


object RunSom{
  
    def main(args:Array[String]) {
      run(
          sparkMaster = args(0),
          intputFile = args(1),
          outputDir = args(2),
          execName = args(3),
          nbRow = args(4).toInt,
          nbCol = args(5).toInt,
          tmin = args(6).toDouble,
          tmax = args(7).toDouble,
          convergeDist = args(8).toDouble,
          maxIter = args(9).toInt,
          sep = args(10),
          initMap = args(11).toInt, //0: initialisation aleatoire
          initMapFile = args(12)
      )  
    }
  
  def run(
    sparkMaster: String,
    intputFile: String,
    outputDir: String,
    execName: String = "RunSom",
    nbRow: Int = 10, 
    nbCol: Int = 10, 
    tmin: Double = 0.9, 
    tmax: Double = 8,
    convergeDist: Double = -0.001,
		maxIter: Int = 50,
		sep : String = ";",
		initMap: Int = 0,
		initMapFile : String = ""
    ) = {
    val sparkConf = new SparkConf().setAppName(execName)
		sparkConf.setMaster(sparkMaster)
		val sc = new SparkContext(sparkConf)

    val somOptions = Map(
    		"clustering.som.nbrow" -> nbRow.toString, 
    		"clustering.som.nbcol" -> nbCol.toString,
    		"clustering.som.tmin" -> tmin.toString,
    		"clustering.som.tmin" -> tmax.toString,
    		"clustering.som.initMap" -> initMap.toString,
    		"clustering.som.initMapFile" -> initMapFile.toString,   
    		"clustering.som.separator" -> sep.toString
    		)
	    	
	  val trainingDatasetId = sc.textFile(intputFile).map(x => new Vector(x.split(sep).map(_.toDouble))).cache() 
	  
	  val trainingDataset = trainingDatasetId.map{ e =>
	     new Vector(e.elements.take(e.length - 1)) 
	  }.cache()
	    
 
	  println(s"nbRow: ${trainingDataset.count()}")
	    		
		val model = trainingAndPrint(new SomTrainerA, trainingDataset, somOptions, maxIter, convergeDist)
		print("le model est : "+model)
		sc.parallelize(model.prototypes).saveAsTextFile(outputDir+"/model")
	    	
		
	  // transformer un point de données en un objet contenant la données et son identifiant 
	  val trainingDatasetObj = trainingDatasetId.map{ e =>
	    val dataPart = e.elements.take(e.length - 1) // the last column represents the identifier
	    val id = e.elements(e.length - 1).toInt
	    new pointObj(new Vector(dataPart), id)
	  }.cache()
	  
	  trainingDataset.unpersist(true) 
	  
		model.assign(trainingDatasetObj).saveAsTextFile(outputDir+"/assignDatas")
		
		sc.stop()
  }


	def purity(model: AbstractModel, dataset: RDD[NamedVector]): Double = {
			//val nbRealClass = dataset.map(_.cls).reduce(case(cls1,cls2))

			val sumAffectedDatas = dataset.map(d => ((model.findClosestPrototype(d).id, d.cls), 1))
					.reduceByKey{case (sum1, sum2) => sum1+sum2}

			val maxByCluster = sumAffectedDatas.map(sa => (sa._1._1, sa._2))
					.reduceByKey{case (sum1, sum2) => sum1.max(sum2) }
			.map(_._2)
			.collect()

			maxByCluster.sum / dataset.count().toDouble
	}

	def trainingAndPrint(trainer: AbstractTrainer,
			dataset: RDD[Vector],
			modelOptions: Map[String, String],
			maxIteration: Int = 100,
			endConvergeDistance: Double): AbstractModel = {
			val model = trainer.training(dataset, modelOptions, maxIteration, endConvergeDistance)
		  // Initi  alisation du model
			//val trainer = new SomTrainer(nbRow, nbCol, trainingDataset, convergeDist, maxIter)
			//val model = trainer.model

  		println("-- Convergence Distance : " + trainer.getLastConvergence)
  		println("-- NbIteration : " + trainer.getLastIt)
  		println("-- Training duration : " + trainer.getLastTrainingDuration)
  		println("-- The model : " + model)
  		
  		
  		model
	}
}
