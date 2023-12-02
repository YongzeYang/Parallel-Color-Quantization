# Parallel Color Quantization -A Big Data Approach to Image Segmentation

This project is a final project for CityU's Course CS5488 (Big Data Algorithms and Technologies).

## Introduction

Color quantization is a method of image segmentation that represents an image using a limited number of colors. Given that an image may contain millions or even billions of pixels, traditional sequential processing algorithms can result in significant time overhead. In this study, we leverage big data platforms such as Hadoop and Spark to implement image color quantization based on k-means clustering through parallel computing. By comparing the results of traditional methods and parallel computing, our experimental results demonstrate that our new method significantly reduces time overhead. Additionally, we compared the effects of varying parameters in the color quantization algorithm, such as the number of colors, on image quality and runtime. We also provide a discussion related to the practical application of color quantization.

![a](_img\b.png)



## Overview of Our Work

For an RGB image with n bits, it may have n^3 different colors. Getting such a large number down to a small range is a challenge. Some studies have proposed the use of clustering as a means of color quantification[1,2], and is considered to have good effects. The k-Means algorithm, a well-known implementation of a clustering algorithm, can be adapted for parallel processing. Existing research has substantiated the efficacy and speed of parallel versions of the k-Means algorithm[4]. This highlights the potential for leveraging parallel computing to enhance the performance of such algorithms.

 

Our work is structured around a sequence of experiments, each building upon the last, to refine our approach and ensure efficient processing of image data, which may range from millions to billions of pixels. The figure above shows the workflow of our experiments.

 

![b](_img\a.png)

**Fig 1.2** - Workflow for our color quantization experiment using big data technologies

 

We used the image dataset from the Image Compression Benchmark website[5], which comprises high-resolution, high-precision photographic images in Portable Pixmap Format. We selected a collection of color RGB images among them, which have 16-bit or 8-bit original file color bits. The raw dataset is approximately 2GB in ppm format. We decompose these images into RGB vectors and store them as .csv files for analysis, with each row representing pixel data by its RGB components. The image data in .csv files exceed 4GB in size.

 

We have structured our experimental approach into four distinct phases: Baseline Python k-Means, Joblib Parallel Experiment, Hadoop and Spark Experiment and Pure Spark Experiment. We establish a baseline by applying a Python implementation of the k-Means algorithm. By utilizing the Joblib library, we embark on our first parallel computing experiment. To expand our scope, we integrate Hadoop's robust data handling capabilities with Spark's in-memory processing to manage larger datasets more effectively. We have implemented MapReduce framework using Hadoop, and the Resilient Distributed Dataset implementation is also utilized using Spark. Finally, we use pure Spark to focus on optimizing in-memory data processing.

For programming language, we coded program Baseline Experiment and Parallel Python Experiment using Python 3.8. For Hadoop and Spark parts, we used Java 11 and Scala 3.3 respectively.

Each experiment generates new CSV files, including files recording color quantization data and execution details, and processed flatten image data. We used a unified script to convert image data from csv format and image format to each other. 

For further analysis, we use R to visualize and scrutinize the data. In this way, we aim to demonstrate the effectiveness of parallel computing in the field of image color quantization.



## Hardware and Software Environment in this Project

Initially, we planned to use a virtual machine or cloud server for our experiment. However, due to potential performance issues, we opted for a personal laptop running Windows 11 Professional. The laptop has an Intel i7-12700H processor with 14 cores and 20 threads, a 2TB SSD, and 32GB DDR5 memory. Although the laptop has an RTX-3070ti GPU and CUDA 11.2 environment, we conducted the experiment solely on the CPU for broader compatibility.

 

The experiment requires Python 3.8 or higher and Java Runtime Environment 11. For baseline and parallel Python experiments, we suggest using a virtual environment. For Hadoop and Spark experiments, Hadoop 2.7 and Spark 3.2.4 are needed. Ensure JAVA_HOME, HADOOP_HOME, and SPARK_HOME paths are set in the environment variables and start all Hadoop services with the start-all command before running the project. If you need to compile your program, install Java Development Kit 11 and Scala 3.3.1, and ensure Maven is available for dependency management in your project.

