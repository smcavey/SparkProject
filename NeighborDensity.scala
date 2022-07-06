class NeighborDensity extends Serializable {
  def start: Unit ={
    val dataRDD = sc.textFile("/user/ds503/input/points.csv")
    val xyRDD = dataRDD.map(line => line.split(",").map(_.toInt))
    xyRDD.take(5).foreach(println)
    // convert points into cells and assign 1 to count
    val cellRDD = xyRDD.map(point => (assignCell(point), 1))
    cellRDD.take(5).foreach(println)
    // reduce cell keys and sum counts
    val cellPopRDD = cellRDD.reduceByKey((x,y) => x+y)
    cellPopRDD.take(5).foreach(println)
    // get neighbors for cells and create a (cell, count, [neighbors]) triple
    val cellNeighborsPopRDD = cellPopRDD.map(cell => (cell._1, cell._2, getNeighbors(cell._1)))
    cellNeighborsPopRDD.take(5).foreach(println)
    }
  // take a x,y point and return a cell number
    def assignCell(point: Array[Int]): Int ={
      val x = point(0)
      println(x)
      val y = point(1)
      println(y)
      // (1,10) = 1/20.ceil -> 1 + 10/20.floor -> 0 = 1 + 0 * 500 = 1
      // (30,50) = 30/20.ceil -> 2 + 50/20.floor -> 2 = 2 + (2*500) = 1002
      val xOffset = (x/20).ceil
      val yOffset = (y/20).floor * 500
      val cell = x + y
      return cell
    }
  // take cell number and return neighbors
  def getNeighbors(cell: Int): Array[Int] ={
    val cellNum = cell
    if(cellNum == 1){ // bottom left
      return Array[Int](2, 501, 502)
    }
    else if(cellNum == 500){ // bottom right
      return Array[Int](499, 1000, 999)
    }
    else if(cellNum == 24501){ // top left
      return Array[Int](24502, 24001, 24002)
    }
    else if(cellNum == 25000){ // top right
      return Array[Int](24999, 24500, 24499)
    }
    else if(cellNum > 1 && cellNum < 500){ // bottom row
      return Array[Int](cellNum-1, cellNum+1, cellNum+500, cellNum+499, cellNum+501)
    }
    else if(cellNum > 24501 && cellNum < 25000){ // top row
      return Array[Int](cellNum-1, cellNum+1, cellNum-500, cellNum-501, cellNum-499)
    }
    else if(cellNum % 500 == 0 && cellNum > 500 && cellNum < 25000){ // right side
      return Array[Int](cellNum+500, cellNum-500, cellNum-1, cellNum+499, cellNum-501)
    }
    else if(cellNum % 501 == 1 && cellNum > 1 && cellNum < 24501){ // left side
      return Array[Int](cellNum+500, cellNum-500, cellNum+1, cellNum+501, cellNum-499)
    }
    else{ // middle
      return Array[Int](cellNum+1, cellNum-1, cellNum+500, cellNum-500, cellNum+501, cellNum+499, cellNum-501, cellNum-499)
    }
  }
}
object Run {
  def main: Unit ={
    new NeighborDensity().start
  }
}