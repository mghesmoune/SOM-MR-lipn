package org.lipn.som.som

import scala.math.{abs, exp}
import java.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.util.Vector
import org.lipn.som.global.{AbstractPrototype, AbstractModel, AbstractTrainer}
import org.lipn.som.utils.NamedVector

/**
 * User: tug
 * Date: 14/06/13
 * Time: 12:35
 */
class SomTrainerA extends AbstractTrainer {
  val DEFAULT_SOM_ROW = 10
  val DEFAULT_SOM_COL = 10
  val DEFAULT_TMAX = 8
  val DEFAULT_TMIN = 1
  val DEFAULT_INITMAP = 0
  val DEFAULT_INITMAPFile = ""
  val DEFAULT_SEPARATOR = ""
  
  var tmax: Double = DEFAULT_TMAX
  var tmin: Double = DEFAULT_TMIN
  var initMap: Int = DEFAULT_INITMAP
  var initMapFile: String = DEFAULT_INITMAPFile
  var sep = DEFAULT_SEPARATOR

  protected var _somModel: SomModel = null
  protected def getModel: AbstractModel = _somModel

  protected def initModel(dataset: RDD[Vector], modelOptions: Map[String, String]) {
    var nbRow = DEFAULT_SOM_ROW
    var nbCol = DEFAULT_SOM_COL
    if (modelOptions != null) {
      nbRow = modelOptions("clustering.som.nbrow").toInt
      nbCol = modelOptions("clustering.som.nbcol").toInt
      tmax = modelOptions.get("clustering.som.tmax").map(_.toDouble).getOrElse(DEFAULT_TMAX)
      tmin = modelOptions.get("clustering.som.tmin").map(_.toDouble).getOrElse(DEFAULT_TMIN)
      initMap = modelOptions.get("clustering.som.initMap").map(_.toInt).getOrElse(DEFAULT_INITMAP)
      initMapFile = modelOptions.get("clustering.som.initMapFile").map(_.toString).getOrElse(DEFAULT_INITMAPFile)
      sep = modelOptions.get("clustering.som.separator").map(_.toString).getOrElse(DEFAULT_SEPARATOR)
    }

    val mapSize = nbRow * nbCol
    // todo : replace random = 42
    var selectedDatas: Array[org.apache.spark.util.Vector] = Array()
    if (initMap == 0) {    
       selectedDatas = {
      dataset.takeSample(withReplacement = false, mapSize, new Random().nextInt())
    }
    } else {
       selectedDatas = {
        scala.io.Source.fromFile(initMapFile).getLines().toArray.map(x => new Vector(x.split(sep).map(_.toDouble)))
      }
    }

    // todo : Check /nbCol et %nbCOl
    val neuronMatrix = Array.tabulate(mapSize)(id => new SomNeuron(id, id/nbCol, id%nbCol, selectedDatas(id)))
    _somModel = new SomModel(nbRow, nbCol, neuronMatrix)
  }

  protected def trainingIteration(dataset: RDD[Vector], currentIteration: Int, maxIteration: Int): Double = {
    val T = processT(maxIteration, currentIteration)

    // create som observations
    val mapping = dataset.map{d =>
      val bestNeuron = _somModel.findClosestPrototype(d).asInstanceOf[SomNeuron]

      _somModel.prototypes.map{proto =>
        val neuron = proto.asInstanceOf[SomNeuron]
        val factor = neuron.factorDist(bestNeuron, T)
        new SomObsA(d * factor, factor, neuron.id)
      }
    }

    // Concat observations
    val concatObs = mapping.reduce{(obs1, obs2) =>
      for (i <- 0 until obs1.length) {
        obs1(i) += obs2(i)
      }
      obs1
    }

    // Update model and process convergence distance
    concatObs.map(_somModel.update).sum
  }

  //protected def processT(maxIt:Int, currentIt:Int) = maxIt.toFloat - currentIt
   protected def processT(maxIt:Int, currentIt:Int) =
this.tmax*math.pow(this.tmin/this.tmax,currentIt/(maxIt.toFloat-1))

  protected class SomModel(val nbRow: Int, val nbCol: Int, neurons: Array[SomNeuron])
    extends AbstractModel(neurons.asInstanceOf[Array[AbstractPrototype]]) {

    // Update the data point of the neuron
    // and return the distance between the new and the old point
    def update(obs: SomObsA) = neurons(obs.neuronId).update(obs.compute)


    override def toString: String = {
      var str = ""
      for(neuron <- neurons) {
        str += neuron+"\n"
      }
      str
    }
  }

  protected class SomNeuron(id: Int, val row: Int, val col: Int, point: Vector) extends AbstractPrototype(id, point) {
    def factorDist(neuron: SomNeuron, T: Double): Double = {
      exp(-(abs(neuron.row - row) + abs(neuron.col - col)) / T)
    }

    override def toString: String = {
      "("+row+", "+col+") -> "+point
    }
  }

  protected class SomObsA(var numerator:Vector, var denominator: Double, val neuronId: Int) extends Serializable {
    def +(obs: SomObsA): SomObsA = {
      numerator += obs.numerator
      denominator += obs.denominator
      this
    }

    def compute = numerator / denominator

    override def toString = numerator.toString()+" : "+denominator.toString
  }



  def purity(dataset: RDD[NamedVector]): Double = {
    //val nbRealClass = dataset.map(_.cls).reduce(case(cls1,cls2))

    val sumAffectedDatas = dataset.map(d => ((_somModel.findClosestPrototype(d).id, d.cls), 1))
      .reduceByKey{case (sum1, sum2) => sum1+sum2}

    val maxByCluster = sumAffectedDatas.map(sa => (sa._1._1, sa._2))
      .reduceByKey{case (sum1, sum2) => sum1.max(sum2) }
      .map(_._2)
      .collect()

    maxByCluster.sum / dataset.count().toDouble
  }

  def affectations(dataset: RDD[NamedVector]): RDD[(Int, Int)] = {
    dataset.map(d => (d.cls, _somModel.findClosestPrototype(d).id))
  }
}

 class pointObj(
    val data: Vector,//the numeric part of the data-point
    //val label: Int,            //the real (provided) label
    val id: Int               //the identifier(=numeroLigne) of the data-point
    ) extends Serializable {
  override def toString: String = {" "
    //data.toArray.deep.mkString(", ") + pointPartBin.toArray.deep.mkString(", ")
    /*"partieNumerique -> "+pointPartNum.toArray.deep.mkString("[", ", ", "]") +
    "; partieBinaire -> "+pointPartBin.toArray.deep.mkString("[", ", ", "]")*/ 
  } 
 }
 
