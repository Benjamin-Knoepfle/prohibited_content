
import prohibited_code as pc

from sklearn import svm
from sklearn.linear_model import SGDClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import Perceptron
from sklearn.metrics import average_precision_score

import execnet, remote_preprocess
import cPickle as pickle
import itertools
import math

import datetime


file_path = '../../data/raw_data/'
train_file_name = 'avito_train.tsv'
test_file_name = 'avito_test.tsv'
train_max_samples = 3995802
test_max_samples = 1351242

sample_size = 5000


#train
print "distributed train"
preprocessor = pickle.dumps( pc.preprocess_data )

group = execnet.Group( ['popen']*4 )
mch = group.remote_exec( remote_preprocess )
for i in range( len(mch) ):
        mch[i].send( preprocessor )

'''
gw1 = execnet.makegateway()
channel1 = gw1.remote_exec( remote_preprocess )
gw2 = execnet.makegateway()
channel2 = gw2.remote_exec( remote_preprocess )
gw3 = execnet.makegateway()
channel3 = gw3.remote_exec( remote_preprocess )

print "preprocess train"
channel1.send( preprocessor )
channel2.send( preprocessor )
channel3.send( preprocessor )
mch = execnet.MultiChannel( [ channel1, channel2, channel3 ] )
'''


queue = mch.make_receive_queue( endmarker=-1 )
channels = itertools.cycle( mch )

num_splits = 3

messages = []
for i in range( num_splits ):
        start_pos = i*sample_size
        if start_pos > train_max_samples: continue
        messages.append( (file_path+train_file_name, sample_size, start_pos, True) )


print "send data"
for message in messages:
        
        channel = channels.next()
        channel.send( message )

train_data_sets=[]
train_label_sets=[]


print "receive data"
for i in range( len( messages ) ):
        channel, serialized_data = queue.get( )
        print "Split number " +str(i)+ " received"
        if serialized_data == -1: print "Error with data;" 
        train_data, labels = pickle.loads( serialized_data )
        train_data_sets.append( train_data )
        train_label_sets.append( labels )
  
#train_data, labels = pc.preprocess_data2( file_path+train_file_name, range(sample_size), targets=True )

print "tidy data"
feature_index = {}
item_ids, train_feat, labels = pc.tidy_data( train_data_sets, train_label_sets, feature_index = feature_index )

train_set, test_set, evaluation_set = pc.create_train_sets( train_feat, labels, train_frc=1, test_frc=0, evaluation_frq=0, method='simple' )

print "train classifier"
clf = svm.SVC()

clf.fit( train_set['data'], train_set['labels'] )

if(len( test_set )>0):
        print "evaluate classifier"
        test_prediction = clf.predict( test_set['data'] )
        test_score = average_precision_score( test_set['labels'], test_prediction )
        print "score: " + str( test_score )


####################################################################################
##      This is the classification tast                                           ##
####################################################################################

#test
print "test"

print "preprocess test"

print "calculate number of splits for test_data"
num_splits = test_max_samples/sample_size +1
print str(num_splits) + " splits needed for sample_size " + str(sample_size)

messages = []
for i in range( num_splits ):
        start_pos = i*sample_size
        if start_pos > train_max_samples: continue
        messages.append( (file_path+test_file_name, sample_size, start_pos, False) )


print "send data"
for message in messages:
        
        channel = channels.next()
        channel.send( message )

test_data_sets=[]


print "receive data"
for i in range( len( messages ) ):
        channel, serialized_data = queue.get( )
        print "Split number " +str(i)+ " received"
        if serialized_data == -1: print "Error with data;" 
        test_data = pickle.loads( serialized_data )
        test_data_sets.append( test_data )


group.terminate()
#test_data = pc.preprocess_data2( file_path+test_file_name, range(sample_size), targets=False )

print "tidy data"
item_ids, test_feat = pc.tidy_data( test_data_sets, feature_index = feature_index )

print "classify"
predicted_scores = clf.predict( test_feat )

solution =  pc.sort_solution( item_ids, predicted_scores )

solution_file_name = '../../data/data/solution' + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + '.csv'
pc.write_solution( solution_file_name, solution )

