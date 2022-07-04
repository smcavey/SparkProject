import scala.io.Source._

// 1001 | 1002 | 1003 | ... | 1500
//  501 |  502 |  503 | ... | 1000
//  1   |   2  |  3   | ... | 500

class NeighborDensity {
  
  def assignCell(point: Array[String]) Int: ={
    val x = point.toInt(0)
    val y = point.toInt(1)
    // (1,10) = 1/20.ceil -> 1 + 10/20.floor -> 0 = 1 + 0 * 500 = 1
    // (30,50) = 30/20.ceil -> 2 + 50/20.floor -> 2 = 2 + (2*500) = 1002
    return x/20.ceil + (y/20).floor * 500)
  }
  
  def main(args: Array[String]): Unit ={
    // array[String]
    val dataRDD = sc.textFile("/user/ds503/input/pointsSample.csv")
    // [Ljava.lang.String;@........
    val xyRDD = dataRDD.map(line => line.split(","))
    // cell num with point counts inside them
    val cellPoints = xyRDD.map({point => (assignCell(point)), 1)}).reduceByKey((x,y) => x+y)
  }
  
}