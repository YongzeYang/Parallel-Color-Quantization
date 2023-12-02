import csv
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def get_height_width(name):
    df = pd.read_csv('fileinfo.csv')
    row = df[df['name'] == name]
    height = row['height'].values[0]
    width = row['width'].values[0]
    return height, width

def convert(path):

    output_csv_filename = 'part-00000'
    output_fig_path = 'img/'

    parts = path.split('_')
    k = int(parts[-1])
    type = parts[-2]
    name = '_'.join(parts[:-2])

    height, width = get_height_width(name + '_' + type)


    # output_fig_filename = path
    print("Processing: " + output_path + "/" + path + '/' + output_csv_filename )
    img = np.genfromtxt(output_path + "/" + path + '/' + output_csv_filename, delimiter=',', dtype=np.uint8)

    new_img = np.zeros((height, width, 3), dtype=np.uint8)

    idx = 0
    for i in range(height):
        for j in range(width):
            new_img[i][j] = img[idx]
            idx = idx + 1

    plt.imshow(new_img)
    print(output_fig_path + path)
    plt.savefig(output_fig_path + path)

if __name__ == '__main__':
    # output_path = 'spark_output'
    # directories = [name for name in os.listdir(output_path) if os.path.isdir(os.path.join(output_path, name))]
    #
    # for path in directories:
    #     convert(path)

    output_path = 'input_csv'
    directories = [name for name in os.listdir(output_path)]

    for path in directories:
        unique_triples = set()

        # Open the CSV file
        with open(output_path+"/"+"flower_foveon_rgb16bit_linear.csv", 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                # Convert each row to a tuple of integers
                triple = tuple(map(int, row))
                # Add the tuple to the set
                unique_triples.add(triple)

        # Return the number of unique triples
        print(path+" "+str(len(unique_triples)))






