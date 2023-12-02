import os
import time
from datetime import datetime
import csv
import numpy as np
import matplotlib.pyplot as plt
from random import randint
import math
from tqdm import tqdm
from joblib import Parallel, delayed

def rand_cluster(K):
    print("Start randomly generating clusters")
    clusters = []
    for i in range(K):
        cluster_pos = [randint(0, 255), randint(0, 255), randint(0, 255)]  # random cluster position
        clusters.append(cluster_pos)
    return clusters

def calc_distance(p1, p2):
    dimension = len(p1)
    distance = 0
    for i in range(dimension):
        distance += (p1[i] - p2[i]) ** 2
    return math.sqrt(distance)

def assign_label(p, clusters):
    distances = [calc_distance(p, c) for c in clusters]
    min_distance = min(distances)  # find min distance
    idx_min = distances.index(min_distance)  # -> 0/1/...
    return idx_min  # assign label

def update_cluster(i, image, labels):
    sum_x = 0
    sum_y = 0
    sum_z = 0
    point_count = 0
    for j in range(len(image)):
        if i == labels[j]:
            sum_x += image[j][0]
            sum_y += image[j][1]
            sum_z += image[j][2]
            point_count += 1
    if point_count != 0:
        # new pos = average pos
        new_x = sum_x / point_count
        new_y = sum_y / point_count
        new_z = sum_z / point_count
        return [new_x, new_y, new_z]

def process(input_img='', output_img='', k=16):
    print("Start processing images")
    image = plt.imread(input_img)  # (656, 561, 3)
    height = image.shape[0]  # rows
    width = image.shape[1]  # columns

    image = image.reshape(height * width, 3)  # -> pixel pos
    start = time.time()
    # change K here
    K = k
    clusters = rand_cluster(K)  # -> postion of each cluster
    labels = []

    # calculate distance
    print('Assign a label to each pixel in the image')
    print(image.shape)
    labels = Parallel(n_jobs=os.cpu_count())(delayed(assign_label)(p, clusters) for p in tqdm(image))

    print('Update the location of the cluster')
    clusters = Parallel(n_jobs=os.cpu_count())(delayed(update_cluster)(i, image, labels) for i in tqdm(range(K)))

    # update position
    print('Generate new image')
    new_image = np.zeros((height, width, 3), dtype=np.uint8)

    pixel_index = 0
    for i in tqdm(range(height)):
        for j in range(width):
            label_of_pixel = labels[pixel_index]
            new_image[i][j] = clusters[label_of_pixel]
            pixel_index += 1
    end = time.time()
    print('Display Image')
    plt.imshow(new_image)
    plt.savefig(output_img)
    plt.show()
    return start, end

def run():
    input_directory = 'file/input'
    output_directory = 'file/parallel'
    csv_file = 'file/output_parallel.csv'
    ks = [2,4,6,8,10]

    for filename in os.listdir(input_directory):
        for k in ks:
         if filename.endswith('ppm'):
            file_path = os.path.join(input_directory, filename)
            output_path = os.path.join(output_directory, os.path.splitext(filename)[0] +'_' + str(k) + '_compressed.png')
            
            start_time, end_time = process(file_path, output_path,k)
            
            elapsed_time = end_time - start_time
            current_time = datetime.now().strftime('%H:%M:%S-%d/%m/%Y')

            with open(csv_file, 'a', newline='') as csvfile:
                fieldnames = ['datetime', 'input_file', 'output_file', 'start_time', 'end_time', 'time_cost','k']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow({'datetime': current_time, 'input_file': os.path.splitext(filename)[0],
                                 'output_file': os.path.splitext(filename)[0] + '_' + str(k)+ '_compressed',
                                 'start_time': start_time, 'end_time': end_time, 'time_cost': elapsed_time,'k':k})

            print('Time cost: ', elapsed_time)


if __name__ == "__name__":
    run()
