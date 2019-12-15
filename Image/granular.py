import cv2, os, glob
import numpy as np
import matplotlib.pyplot as plt
from GPSPhoto import gpsphoto
from os import (
    listdir,
    path,
)
import pickle
import sqlite3
import numpy


corn_path = '/YOUR_DIRECTORY/imgs_de/corn/'
corn_path_test = '/YOUR_DIRECTORY/imgs_de/barley/test'

# img_list = [file for file in glob.glob("/Users/jaygkay/Desktop/jupyter/imgs_de/corn/*.jpg")]
gps_list = [gpsphoto.getGPSData(file) for file in glob.glob("/YOUR_DIRECTORY/imgs_de/barley/*.jpg")]
images = [cv2.resize(cv2.imread(file),(224,224),3) for file in glob.glob(barley_path+"*.jpg")]
file_lst = [file.split('/')[-1] for file in glob.glob(barley_path+"*.jpg")]
test_file_lst = [file.split('/')[-1] for file in glob.glob(barley_path+"test/*.jpg")]

# Create a Table
conn = sqlite3.connect('crops.db')
c = conn.cursor()
print('creating table...')
c.execute("drop table if exists corn")

c.execute(
    '''
    create table corn(
        id INTEGER PRIMARY KEY AUTOINCREMENT not null,
        file,
        gray,
        final,
        loc
    );
    '''
)
print('created table')
print(c.execute('''select * from corn;''').fetchall())


# Insert values to table and train drawContours data into 'text folder'

for img, file, gps in zip(images, file_lst,gps_list):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # Convert image from RGB to GRAY
    ret, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)
    # apply thresholding to convert the image to binary
    fg = cv2.erode(thresh, None, iterations=1)
    # erode the image
    bgt = cv2.dilate(thresh, None, iterations=1)
    # Dilate the image
    ret, bg = cv2.threshold(bgt, 1, 128, 1)
    # Apply thresholding
    marker = cv2.add(fg, bg)
    # Add foreground and background
    canny = cv2.Canny(marker, 110, 150)
    # Apply canny edge detector
    new, contours, hierarchy = cv2.findContours(canny, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    # Finding the contors in the image using chain approximation
    marker32 = np.int32(marker)
    # converting the marker to float 32 bit
    cv2.watershed(img,marker32)
    # Apply watershed algorithm
    m = cv2.convertScaleAbs(marker32)
    ret, thresh = cv2.threshold(m, 0, 255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)
    # Apply thresholding on the image to convert to binary image
    thresh_inv = cv2.bitwise_not(thresh)
    # Invert the thresh
    res = cv2.bitwise_and(img, img, mask=thresh)
    # Bitwise and with the image mask thresh
    res3 = cv2.bitwise_and(img, img, mask=thresh_inv)
    # Bitwise and the image with mask as threshold invert
    res4 = cv2.addWeighted(res, 1, res3, 1, 0)
    # Take the weighted average
    final = cv2.drawContours(res4, contours, -1, (0, 255, 0), 1)
    try:
        loc = gps
#         print(lat, lon)
    except:
        KeyError
        loc = 0,0
#         print(lat,lon)
    val = picID,rgb,hsv,lat,lon
    c.execute('insert into corn (file, gray,final,loc) values(?, ?, ?, ?)',
              (pickle.dumps(file),pickle.dumps(gray), pickle.dumps(final),pickle.dumps(loc)))

    plt.subplot(1,2,1)
    plt.imshow(img)
    plt.subplot(1,2,2)
    plt.imshow(final)
    cv2.imwrite(barley_path +'test/test_'+file+".jpg", final)
