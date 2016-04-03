package org.lipn.som.bitm

import org.apache.spark.rdd.RDD
import org.lipn.som.utils.NamedVector
import org.lipn.som.bitm.CroeucTopoFactor

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 06/01/14
 * Time: 12:28
 */
class Croeuc(val nbCluster: Int, datas: RDD[NamedVector]) extends BiTM(nbCluster, 1, datas, CroeucTopoFactor) {
}
