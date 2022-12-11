import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # or any {'0', '1', '2'}
import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import pickle
from lenet_model import LeNetModel
from sklearn.model_selection import train_test_split
from keras.datasets import mnist
from keras.optimizers import SGD
from keras.utils import np_utils
from keras import backend as K
import pickle
import numpy as np
import argparse
import cv2

#tf.get_logger().setLevel('INFO')


opt = SGD(lr=0.01)
model = LeNetModel.build(numChannels=1, imgRows=28, imgCols=28,
	numClasses=10,
	weightsPath="lenet_weights.hdf5")
model.compile(loss="categorical_crossentropy", optimizer=opt,
	metrics=["accuracy"])

with open("mnist_testdata.pkl", "br") as fh:
    data = pickle.load(fh)

testData = data[0] 
testLabels = data[1]
print(testData.shape)
print(testData[0].shape)
print(testLabels.shape)

for i in np.random.choice(np.arange(0, len(testLabels)), size=(10,)):
	# classify the digit
	#print(i)
	#var = testData[np.newaxis, i]
	#print(f"Input to model.predict: {var.shape}")

	probs = model.predict(testData[np.newaxis, i])
	print(f"probs:{probs}")
	prediction = probs.argmax(axis=1)

	print("[INFO] Predicted: {}, Actual: {}".format(prediction[0],
		np.argmax(testLabels[i])))
