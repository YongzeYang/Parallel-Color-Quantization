package cityu.bigdata.project

import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import java.nio.file.{Files, Paths}

import scala.util.Random

object PureScala {
  /**
   * Calculate the Euclidean distance between two vectors.
   *
   * @param p The first vector.
   * @param q The second vector.
   * @return The Euclidean distance between p and q.
   */
  def distance(p: Vector[Int], q: Vector[Int]): Double = {
    var dist = math.sqrt(p.zip(q).map(pair => math.pow((pair._1 - pair._2), 2)).sum);
    return dist
  }

  /**
   * Calculate the average Euclidean distance between corresponding vectors in two arrays.
   *
   * @param p The first array of vectors.
   * @param q The second array of vectors.
   * @return The average Euclidean distance between corresponding vectors in p and q.
   */
  def distanceArray(p: Array[Vector[Int]], q: Array[Vector[Int]]): Double = {
    var dist = p.zip(q).map(pair => distance(pair._1, pair._2)).sum;
    dist = dist / p.length
    return dist
  }

  /**
   * Find the vector in an array of candidates that is closest to a given vector.
   *
   * @param q          The given vector.
   * @param candidates The array of candidate vectors.
   * @return The vector in candidates that is closest to q.
   */
  def closestpoint(q: Vector[Int], candidates: Array[Vector[Int]]): Vector[Int] = {

    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- candidates.indices) {
      val tempDist = distance(q, candidates(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    return candidates(bestIndex)
  }

  /**
   * Add two vectors.
   *
   * @param v1 The first vector.
   * @param v2 The second vector.
   * @return The result of adding v1 and v2.
   */
  def add_vec(v1: Vector[Int], v2: Vector[Int]): Vector[Int] = {
    var newVector = v1.zip(v2).map(pair => (pair._1) + pair._2)
    return newVector
  }

  /**
   * Calculate the average vector of a cluster of vectors.
   *
   * @param cluster The cluster of vectors.
   * @return The average vector of the cluster.
   */
  def average(cluster: Iterable[Vector[Int]]): Vector[Int] = {
    val numVectors = cluster.size
    var out = Vector(0, 0)
    var it = cluster.toIterator

    while (it.hasNext) {
      out = add_vec(out, it.next())
    }
    var ret = out.map(x => (x / numVectors))
    return ret
  }

  /**
   * Determine if the two arrays of vector are converged.
   * If the distance of any pair is higher than 5, then the arrays are not converged.
   *
   * @param p one array of integer vectors
   * @param q another array of integer vectors
   * @return true if converged, false if not
   */
  def isConverged(p: Array[Vector[Int]], q: Array[Vector[Int]]): Boolean = {
    val dist = p.zip(q).map(pair => distance(pair._1, pair._2))
    val flag = !dist.exists(_ > 10)
    return flag
  }

  /**
   * Returns the subdirectories in this path
   *
   * @param path The input path
   * @return The subdirectories in this path
   */
  def listSubdirectories(path: String): Array[File] = {
    val file = new File(path)
    if (file.exists && file.isDirectory) {
      file.listFiles.filter(_.isDirectory)
    } else {
      Array.empty[File]
    }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("K-Means").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dir = "input_csv"
    val output_csv = "pure_spark.csv"

    val ks = Array(2,4,6,8,12, 16, 32, 48, 64, 96, 128, 144, 192, 208, 256)
    val csv_path = dir + "/" + "artificial_rgb16bit.csv"
    val filename = csv_path.split("\\\\").last.split("\\.").head
    for (k <- ks) {
      println(s"Processing value: $k")
//      val file = iterator.next()
//      val csv_path = file.toString

//      println(s"Processing file: ${file.toString}")

      val clusterLines = sc.textFile(csv_path);
      val data = clusterLines.map(l => Vector.empty ++ l.split(',').map(_.toInt))



      val rand = new Random()

      // 生成k个Vector，每个Vector有三个整数值，范围是0-255
      var centers = Array.fill(k)(Vector(rand.nextInt(256), rand.nextInt(256), rand.nextInt(256)))

      // var centers = data.takeSample(withReplacement = false, k, 99)

      val minDist = 1e-6
      var d = 1 + minDist // iteration num
      var njlc = 1
      val maxIter = 100 // 指定的最大迭代次数

      val startTime = System.currentTimeMillis()

      var flag = true

      do {
        println(s"COUNT: $njlc")
        println("=" * 100)
        njlc += 1

        var closest = data.map(p => (closestpoint(p, centers), p))
        var pointsgroup = closest.groupByKey()
        var newCenters = pointsgroup.mapValues(ps => average(ps))
        var editedCenters = newCenters.values


        flag = isConverged(centers, editedCenters.collect)

        centers = editedCenters.collect

      } while (!flag && njlc <= maxIter)

      println("centers are calculated, iterations: " + (njlc-1))

      val processedImage = data.map(pixel => closestpoint(pixel, centers))
      val pure_spark_output = "pure_spark_output"

      val endTime = System.currentTimeMillis()

      processedImage.coalesce(1).map(pixel => pixel.mkString(",")).saveAsTextFile(pure_spark_output + "/multiple_iterations/" + filename + "_" + k)

      val timeCostSec = (endTime - startTime) / 1000.0
      val current_datetime = new SimpleDateFormat("HH:mm:ss-dd/MM/yyyy").format(new Date())

      val writer = new PrintWriter(new FileWriter(output_csv, true))
      try {
        writer.println(current_datetime + ","
          + filename + "," +
          + timeCostSec + ","
          + k + ","
          + (njlc - 1))
      } finally {
        writer.close()
      }

    }

    //    val csvs = Files.list(Paths.get(dir))
    //    val iterator = csvs.iterator()
    //
    //    while (iterator.hasNext) {


    sc.stop()

    //  }

  }
}