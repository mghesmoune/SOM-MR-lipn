package org.lipn.som.global

import org.apache.spark.util.Vector
import org.apache.spark.rdd.RDD
import org.lipn.som.som.pointObj

/**
 * Created with IntelliJ IDEA.
 * User: tug
 * Date: 14/06/13
 * Time: 12:34
 * To change this template use File | Settings | File Templates.
 */
abstract class AbstractModel(val prototypes: Array[AbstractPrototype]) extends Serializable {
  def size = prototypes.size

  def findClosestPrototype(data: Vector): AbstractPrototype = {
    prototypes.minBy(proto => proto.dist(data))
  }
  
  def findClosestPrototypeId(data: Vector): AbstractPrototype = {
    prototypes.minBy(proto => proto.dist(data))
  }  

  def apply(i: Int) = prototypes(i)

  def assign(dataset: RDD[pointObj]): RDD[(Int, Int)] =  {
    dataset.map(d => (this.findClosestPrototype(d.data).id, d.id))
  }
}
