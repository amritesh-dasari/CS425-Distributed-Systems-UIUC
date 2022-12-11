from server import *
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
import numpy as np
import matplotlib.pyplot as plt
import pickle
import tensorflow as tf
from tensorflow import keras
from sklearn.model_selection import train_test_split
from keras.datasets import mnist
from keras.optimizers import SGD
from keras.utils import np_utils
from keras import backend as K
import pickle
import numpy as np
import argparse
import cv2
import sys
import h5py
from resnets_utils import *

resnetmodel = keras.models.load_model("resnet_model.h5")
test_dataset = h5py.File('resnet_testdata.h5', "r")
test_set_x_orig = np.array(test_dataset["test_set_x"][:]) # your test set features
test_set_y_orig = np.array(test_dataset["test_set_y"][:]) # your test set labels

X_test = test_set_x_orig/255.
Y_test = convert_to_one_hot(test_set_y_orig, 6).T
print ("X_test shape: " + str(X_test.shape))
print ("Y_test shape: " + str(Y_test.shape))
print(sys.getsizeof(test_dataset))
#print(resnetmodel.summary())
for i in range(0,10):
	probs = resnetmodel.predict(X_test[np.newaxis, i],verbose = 0)
	print(f"probs:{probs}")
	prediction = probs.argmax(axis=1)

	print("[INFO] Predicted: {}, Actual: {}".format(prediction[0],
		np.argmax(Y_test[i])))