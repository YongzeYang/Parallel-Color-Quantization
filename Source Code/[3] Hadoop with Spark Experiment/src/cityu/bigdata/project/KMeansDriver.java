package cityu.bigdata.project;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.imageio.ImageIO;

/**
 * This class implements a k-Means Image Compression Method using Hadoop.
 * To run this class, you have to make sure that the hadoop service is already running.
 */
public class KMeansDriver {

    private static boolean first_flag = true;
    private static boolean first_reducer_flag = true;

    /**
     * This method generates a list of K clusters with random positions.
     * Each position is represented as a list of three integers, each ranging from 0 to 255.
     *
     * @param K The number of clusters to generate.
     * @return A list of clusters, each represented as a list of three integers.
     */
    public static ArrayList<ArrayList<Integer>> randCluster(int K) {

        ArrayList<ArrayList<Integer>> clusters = new ArrayList<ArrayList<Integer>>();
        Random rand = new Random(); // Random number generator

        for (int i = 0; i < K; i++) {
            ArrayList<Integer> clusterPos = new ArrayList<Integer>();
            clusterPos.add(rand.nextInt(256)); // Random integer between 0 and 255
            clusterPos.add(rand.nextInt(256));
            clusterPos.add(rand.nextInt(256));
            clusters.add(clusterPos);
        }
        return clusters;
    }

    /**
     * This method generates one cluster with random positions.
     * Each position is represented as a list of three integers, each ranging from 0 to 255.
     *
     * @return A list of clusters, each represented as a list of three integers.
     */
    public static ArrayList<Integer> randCluster() {

        Random rand = new Random(); // Random number generator

        ArrayList<Integer> clusterPos = new ArrayList<Integer>();
        clusterPos.add(rand.nextInt(256)); // Random integer between 0 and 255
        clusterPos.add(rand.nextInt(256));
        clusterPos.add(rand.nextInt(256));

        return clusterPos;
    }

    /**
     * This method calculates the Euclidean distance between two points in a multi-dimensional space.
     * Each point is represented as a list of integers, and the dimension of the space is determined by the size of the list.
     *
     * @param p1 The first point.
     * @param p2 The second point.
     * @return The Euclidean distance between the two points.
     */
    public static double calcDistance(ArrayList<Integer> p1, ArrayList<Integer> p2) {
        int dimension = p1.size();
        double distance = 0;
        for (int i = 0; i < dimension; i++) {
            distance += Math.pow(p1.get(i) - p2.get(i), 2);
        }
        return Math.sqrt(distance);
    }

    /**
     * This is a Mapper class for a k-Means clustering algorithm implemented with MapReduce.
     * It reads in pixels from the input, assigns them to the nearest cluster,
     * and outputs the cluster index and pixel with index.
     */
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private ArrayList<ArrayList<Integer>> clusters;

        protected ArrayList<ArrayList<Integer>> readClustersFromCache(int K, Context context) throws IOException {
            ArrayList<ArrayList<Integer>> clusters = new ArrayList<ArrayList<Integer>>();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0]))));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    ArrayList<Integer> cluster = new ArrayList<Integer>();
                    for (String part : parts) {
                        cluster.add(Integer.parseInt(part));
                    }
                    clusters.add(cluster);
                }
                reader.close();
            }
            return clusters;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Initialize clusters
            int K = context.getConfiguration().getInt("K", 256); // Get K from configuration
            if(first_flag) {
                clusters = randCluster(K);
                first_flag = false;
            } else {
                clusters = readClustersFromCache(K, context);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input into an RGB pixel
            String[] parts = value.toString().split(",");
            ArrayList<Integer> pixel = new ArrayList<Integer>();
            for (String part : parts) {
                pixel.add(Integer.parseInt(part));
            }

            // Calculate distances to all clusters and find the nearest one
            double minDistance = Double.MAX_VALUE;
            int nearestCluster = 0;
            for (int i = 0; i < clusters.size(); i++) {
                double distance = calcDistance(pixel, clusters.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCluster = i;
                }
            }
            context.write(new IntWritable(nearestCluster), value);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Make sure to generate one data for each cluster center
            for (int i = 0; i < clusters.size(); i++) {
                context.write(new IntWritable(i), new Text(clusters.get(i).get(0) + "," + clusters.get(i).get(1) + "," + clusters.get(i).get(2)));
            }
        }


    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        private ArrayList<ArrayList<Integer>> oldClusters;

        // Read cluster centers from cache
        protected ArrayList<ArrayList<Integer>> readClustersFromCache(int K, Context context) throws IOException {
            ArrayList<ArrayList<Integer>> clusters = new ArrayList<ArrayList<Integer>>();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0]))));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    ArrayList<Integer> cluster = new ArrayList<Integer>();
                    for (String part : parts) {
                        cluster.add(Integer.parseInt(part));
                    }
                    clusters.add(cluster);
                }
                reader.close();
            }
            return clusters;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // read the last clusters from cache
            int K = context.getConfiguration().getInt("K", 256); // Get K from configuration
            oldClusters = readClustersFromCache(K, context);
        }

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize a counter and a list to store the sum of RGB values
            int count = 0;
            ArrayList<Integer> sum = new ArrayList<Integer>(Arrays.asList(0, 0, 0));

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                for (int i = 0; i < parts.length; i++) {
                    sum.set(i, sum.get(i) + Integer.parseInt(parts[i]));
                }
                count++;
            }

            // Calculate the new cluster center, which is the average of each RGB value
            ArrayList<Integer> newCluster = new ArrayList<Integer>();

            if (count>0) {
                for (int i = 0; i < 3; i++) {
                    newCluster.add(sum.get(i) / count);
                }
            } else {
                System.out.println("empty");
                // if empty, use the last clusters
                if (first_reducer_flag) {
                    newCluster = randCluster();
                } else {
                    newCluster = oldClusters.get(key.get());
                }

            }

            if (first_reducer_flag) {
                first_reducer_flag = false;
            }

            // Output the new cluster center with NullWritable as the key
            context.write(NullWritable.get(), new Text(newCluster.get(0) + "," + newCluster.get(1) + "," + newCluster.get(2)));
        }

    }


    public static void main(String[] args) throws Exception{
        // Define the input, Hadoop input, and output paths
        String input_path = "input_csv"; // original image(csv format) path
        String hadoop_input_parent_path = "hadoop_input"; // parent path of hadoop input
        String output_path = "hadoop_output"; // parent path of hadoop output
        String output_csv_path = "hadoop.csv"; // csv file

        // int k = 5; // number of clusters
        int max_iteration = 1; // maximum iterations

        int[] ks = {2, 4, 6, 8, 12, 16, 32, 48, 64, 96, 128, 144, 192, 208, 256};


        File dir = new File(input_path);
        File[] csv_files = Objects.requireNonNull(dir.listFiles());

        for(int k : ks){
            File csv_file = csv_files[0];
            String input_filename_with_extension = csv_file.getName();
            System.out.println(input_filename_with_extension);
            String input_filename = input_filename_with_extension.substring(0, input_filename_with_extension.length() - 4);
            String input_filename_with_k = input_filename + "_" + k;
            java.nio.file.Path csv_path = csv_file.toPath();

            String hadoop_input_path = hadoop_input_parent_path + "/" + input_filename_with_k;

            java.nio.file.Path original_path = java.nio.file.Paths.get(hadoop_input_path);

            // If the Hadoop input path exists, skip the current iteration
            if (Files.exists(original_path)) {
                System.out.println("The " + k + " clusters of image: " + input_filename + " has already been calaulated.");
                continue;
            }

            System.out.println("Processing: " + input_filename);

            java.nio.file.Path hadoop_input = java.nio.file.Paths.get(hadoop_input_parent_path + "/"+input_filename_with_k);

            if (!Files.exists(hadoop_input)) {
                // If the Hadoop input path does not exist, create it
                Files.createDirectories(hadoop_input);
            }

            // Copy the file to the Hadoop input path
            Files.copy(csv_path, hadoop_input.resolve(csv_path.getFileName()), StandardCopyOption.REPLACE_EXISTING);


            boolean isDone = false; //indicate whether the algorithm has converged
            int iteration = 0; // current iteration

            long startTime = System.currentTimeMillis();

            // While the algorithm has not converged and the maximum number of iterations has not been reached
            while (!isDone && iteration < max_iteration) {

                // Create a Hadoop configuration
                Configuration conf = new Configuration();
                conf.setInt("K", k);

                // Create a Hadoop job
                Job job = Job.getInstance(conf, "k means");
                job.setJarByClass(KMeansDriver.class);
                job.setMapperClass(KMeansMapper.class);
                job.setReducerClass(KMeansReducer.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);

                // Add the input and output paths to the job
                FileInputFormat.addInputPath(job, new Path(hadoop_input_path));
                FileOutputFormat.setOutputPath(job, new Path(output_path + "/" + input_filename_with_k + "/cluster-" + iteration));

                if (iteration > 0) {
                    // If this is not the first iteration, add the result of the previous iteration to the job's cache files
                    job.addCacheFile(new Path(output_path + "/" + input_filename_with_k + "/cluster-" + (iteration - 1) + "/part-r-00000").toUri());
                }

                if (!job.waitForCompletion(true)) {
                    // Wait for the job to complete, if not complete, exit with error -1
                    System.exit(-1);
                }

                if(iteration > 0) {
                    // If this is not the first iteration, check whether the algorithm has converged
                    Path oldClusterPath = new Path(output_path + "/" + input_filename_with_k + "/cluster-" + (iteration - 1) + "/part-r-00000");
                    Path newClusterPath = new Path(output_path + "/" + input_filename_with_k + "/cluster-" + iteration + "/part-r-00000");
                    isDone = isConverged(oldClusterPath, newClusterPath);
                }

                iteration++;
            }

            long endTime = System.currentTimeMillis();
            double timeCostSec = (endTime - startTime)/1000.0;

            // Define the cluster path
            String cluster_path = output_path + "/" + input_filename_with_k + "/cluster-" + (iteration - 1) + "/part-r-00000";

            System.out.println("Total iterations: " + iteration);
            System.out.println("Total time cost: " + timeCostSec);
            System.out.println("The " + k + " cluster has been saved to: " + cluster_path);

            String current_datetime = new SimpleDateFormat("HH:mm:ss-dd/MM/yyyy").format(new Date());
            // append result to the csv_file
            try (PrintWriter writer = new PrintWriter(new FileWriter(output_csv_path, true))) {
                writer.println(current_datetime + ","
                        + input_filename + ","
                        + cluster_path + ","
                        + timeCostSec + ","
                        + k + ","
                        + iteration+ ","
                        + max_iteration);
            }

            System.exit(0);

        }





    }

    private static boolean isConverged(Path oldClusterPath, Path newClusterPath) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(oldClusterPath)));
        BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(newClusterPath)));

        String oldLine;
        String newLine;
        while ((oldLine = oldReader.readLine()) != null && (newLine = newReader.readLine()) != null) {
            ArrayList<Integer> oldCluster = new ArrayList<Integer>();
            ArrayList<Integer> newCluster = new ArrayList<Integer>();

            for (String s : oldLine.split(",")) {
                oldCluster.add(Integer.parseInt(s));
            }

            for (String s : newLine.split(",")) {
                newCluster.add(Integer.parseInt(s));
            }

            if (calcDistance(oldCluster, newCluster) > 5) {
                oldReader.close();
                newReader.close();
                return false;
            }
        }

        oldReader.close();
        newReader.close();
        return true;
    }

    // backup function, not used in this class
    private static void preprocessImage(String inputImagePath, String outputTextPath, String type) throws IOException {
        BufferedImage image = ImageIO.read(new File(inputImagePath));

        FileSystem fs = FileSystem.get(new Configuration());
        try (FSDataOutputStream out = fs.create(new Path(outputTextPath))) {
            for (int y = 0; y < image.getHeight(); y++) {
                for (int x = 0; x < image.getWidth(); x++) {
                    int rgb = image.getRGB(x, y);
                    if(Objects.equals(type, "grey")){
                        out.write(rgb);
                    } else if (Objects.equals(type, "color")) {
                        int red = (rgb >> 16) & 0xff;
                        int green = (rgb >> 8) & 0xff;
                        int blue = rgb & 0xff;
                        out.write(red);
                        out.write(green);
                        out.write(blue);
                    }

                }
            }
        }
    }

}
