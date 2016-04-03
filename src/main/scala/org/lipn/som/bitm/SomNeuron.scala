package org.lipn.som.bitm

import org.apache.spark.util.Vector
import scala.math.{abs, exp}
import org.lipn.som.global.AbstractPrototype

class SomNeuron(id: Int, val row: Int, val col: Int, point: Vector) extends AbstractPrototype(id, point) {
  def factorDist(neuron: SomNeuron, T: Double): Double = {
    exp(-(abs(neuron.row - row) + abs(neuron.col - col)) / T)
  }

  override def toString: String = {
    "("+row+", "+col+") -> "+point
  }
}
