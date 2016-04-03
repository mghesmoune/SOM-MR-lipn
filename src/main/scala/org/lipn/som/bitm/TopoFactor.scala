package org.lipn.som.bitm

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 06/01/14
 * Time: 12:08
 */
trait TopoFactor extends Serializable {
  def gen(maxIter:Int, currentIter:Int, nbNeurons: Int): Array[Double]
}
