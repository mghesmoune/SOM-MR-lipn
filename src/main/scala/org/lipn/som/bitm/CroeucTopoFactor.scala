package org.lipn.som.bitm

import scala.math.exp


object CroeucTopoFactor extends TopoFactor {
  def gen(maxIter: Int, currentIter: Int, nbNeurons: Int): Array[Double] = {
    val T:Double = (maxIter - currentIter + 1) / maxIter.toDouble
    //val T = 0.9
    Array.tabulate(nbNeurons*2)(i => if (i == 0) exp(i / T) else Double.MaxValue)
  }
}