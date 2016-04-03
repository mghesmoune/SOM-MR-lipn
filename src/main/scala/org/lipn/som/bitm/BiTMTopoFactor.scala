package org.lipn.som.bitm

import scala.math.exp

object BiTMTopoFactor extends TopoFactor {
  def gen(maxIter:Int, currentIter:Int, nbNeurons: Int) = {
    val T:Double = (maxIter - currentIter + 1) / maxIter.toDouble
    //val T = 0.9
    Array.tabulate(nbNeurons*2)(dist => exp(dist / T))
  }
}