import numpy as np 
import matplotlib.pyplot as plt 
from random import randint
import math
from tqdm import tqdm
import os
import time 
from datetime import datetime
import csv

def rand_cluster(K):
    print("s rand")
    clusters = []
    for i in range(K):
        cluster_pos = [randint(0,255), randint(0,255), randint(0,255)] #random cluster position 
        clusters.append(cluster_pos)
    return clusters

def calc_distance(p1, p2):
    # print("s calc")
    dimension = len(p1) 
    distance = 0
    for i in range(dimension):
        distance += (p1[i] - p2[i])**2
    return math.sqrt(distance)

def process(input_img = '', output_img = '', k = 16):
    image = plt.imread(input_img) # (656, 561, 3)
    print("s main")
    height = image.shape[0] #rows 
    width = image.shape[1]    #columns

    image = image.reshape(height*width, 3) # -> pixel pos 

    #change K here
    K = k
    clusters = rand_cluster(K) # -> postion of each cluster
    labels = []

    start_time = time.time()
    
    # calculate distance 
    print('for pixel in image')
    print(image.shape)
    for p in tqdm(image):
        distances = []
        for c in clusters: 
            distance = calc_distance(p, c)
            distances.append(distance)

        min_distance = min(distances) #find min distance
        #find index to assign label
        idx_min = distances.index(min_distance)# -> 0/1/...
        labels.append(idx_min) #assign label

    print('for i in k')
    for i in tqdm(range(K)):
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
            #new pos = average pos
            new_x = sum_x/point_count 
            new_y = sum_y/point_count
            new_z = sum_z/point_count
            clusters[i] = [new_x, new_y, new_z] 
            
        #update position
    print('print new image')
    new_image = np.zeros((height,width,3), dtype=np.uint8)

    pixel_index = 0
    for i in range(height):
        for j in range(width):
            label_of_pixel = labels[pixel_index]
            new_image[i][j] = clusters[label_of_pixel]
            pixel_index += 1
    print('show')

    end_time = time.time()
    
    plt.imshow(new_image)
    plt.savefig(output_img)
    plt.show()

    return start_time, end_time


def run():
    input_directory = 'file/input'
    output_directory = 'file/parallel'
    csv_file = 'file/output_non_parallel.csv'

    for filename in os.listdir(input_directory):
        for k in [32]:
            if filename.endswith('ppm'):
                file_path = os.path.join(input_directory, filename)
                output_path = os.path.join(output_directory, os.path.splitext(filename)[0] + '_' + str(k) + '_compressed.png')
                start_time = time.time()
                start_time, end_time = process(file_path, output_path, k)
                end_time = time.time()
                elapsed_time = end_time - start_time
                current_time = datetime.now().strftime('%H:%M:%S-%d/%m/%Y')

                with open(csv_file, 'a', newline='') as csvfile:
                    fieldnames = ['datetime', 'input_file', 'output_file', 'start_time', 'end_time', 'time_cost', 'k']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow({'datetime': current_time, 'input_file': os.path.splitext(filename)[0], 
                                    'output_file': os.path.splitext(filename)[0] + '_' + str(k) + '_compressed', 
                                    'start_time': start_time, 'end_time': end_time, 'time_cost': elapsed_time, 'k': k})

                print('Time cost: ', elapsed_time)
            
if __name__ == "__main__":
    run()

