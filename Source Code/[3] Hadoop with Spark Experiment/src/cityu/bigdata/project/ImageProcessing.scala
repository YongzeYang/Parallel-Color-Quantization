package cityu.bigdata.project

import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.io._
import java.text.SimpleDateFormat
import java.util.Date

/**
 * The ImageProcessing object contains methods for image processing,
 * i.e. assign calculated k clusters to each pixel.
 */
object ImageProcessing {

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
   * @param q The given vector.
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
   * Returns the subdirectories in this path
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

  /**
   * The main method for the ImageProcessing object.
   *
   * @param args The command-line arguments.
   */
  def main(args: Array[String]): Unit = {

    // Define the Hadoop input and output paths
    val hadoop_input = "hadoop_input"
    val hadoop_output = "hadoop_output"
    val spark_output = "spark_output"
    val output_csv = "spark.csv"

    // Get the existing Hadoop result directories
    val exist_hadoop_result_directories = listSubdirectories(hadoop_output)

    // Create Spark configuration and Spark context
    val conf = new SparkConf().setAppName("Image Processing").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // For each existing Hadoop result directory
    exist_hadoop_result_directories.foreach { subdir =>

      // Get the directory name
      val dirName = subdir.getName

      println("Processing: " + dirName)

      // Split the directory name by the last two underscores
      val parts = dirName.split("_").reverse

      // Get the name, type, and k from the directory name
      val k = parts(0)
      val typeName = parts(1)
      val name = parts.drop(2).reverse.mkString("_")

      // Get the subdirectories of the current directory
      val subdirs = subdir.listFiles.filter(_.isDirectory)

      // Get the names of the subdirectories that start with "cluster-"
      val clusterDirs = subdirs.map(_.getName).filter(_.startsWith("cluster-"))

      // Get the maximum i from the cluster directories
      var maxI = if (clusterDirs.isEmpty) 0 else clusterDirs.map(name => name.stripPrefix("cluster-").toInt).max

      maxI = 0

      val clusterPath = hadoop_output + "/" + dirName + "/cluster-" + maxI + "/part-r-00000"
      val imagePath = hadoop_input + "/" + dirName + "/" + name + "_" + typeName + ".csv"

      // Load the cluster centers and image data from files
      val clusterLines = sc.textFile(clusterPath);
      val clusters = clusterLines.map(l => Vector.empty ++ l.split(',').map(_.toInt))

      val imageLines = sc.textFile(imagePath);
      val image = imageLines.map(l => Vector.empty ++ l.split(',').map(_.toInt))

      val startTime = System.currentTimeMillis()
      // Assign each pixel to the closest cluster center
      val clustersArray = clusters.collect()
      val processedImage = image.map(pixel => closestpoint(pixel, clustersArray))

      // Save the processed image
      processedImage.coalesce(1).map(pixel => pixel.mkString(",")).saveAsTextFile(spark_output + "/" + dirName)

      // Calculate time cost
      val endTime = System.currentTimeMillis()
      val timeCostSec = (endTime - startTime) / 1000.0
      val current_datetime = new SimpleDateFormat("HH:mm:ss-dd/MM/yyyy").format(new Date())
      // Write the result to the csv file
      val writer = new PrintWriter(new FileWriter(output_csv, true))
      try {
        writer.println(current_datetime + ","
          + name + "_" + typeName + ","
          + hadoop_output + "/" + dirName + "/cluster-" + maxI + "/part-r-00000" + ","
          + spark_output + "/" + dirName  + "/part-00000" + ","
          + timeCostSec + ","
          + k + ","
          + (maxI + 1))
      } finally {
        writer.close()
      }



    }

    // Stop the Spark context
    sc.stop()

  }
}