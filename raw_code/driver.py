
import prohibited_code as pc

from sklearn import svm
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import average_precision_score

import execnet, remote_preprocess
import cPickle as pickle
import itertools

import datetime


file_path = '../../data/raw_data/'
train_file_name = 'avito_train.tsv'
test_file_name = 'avito_test.tsv'
sample_size = 20000


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

num_splits = 4

print "send data"
for i in range( num_splits ):
        start_pos = 4000000#i*sample_size
        channel = channels.next()
        channel.send( (file_path+train_file_name, sample_size, start_pos, True) )

train_data_sets=[]
train_label_sets=[]


print "receive data"
for i in range( num_splits ):
        channel, serialized_data = queue.get( )
        if serialized_data == -1: print "Error with data;" 
        train_data, labels = pickle.loads( serialized_data )
        train_data_sets.append( train_data )
        train_label_sets.append( labels )
  
#train_data, labels = pc.preprocess_data2( file_path+train_file_name, range(sample_size), targets=True )

print "tidy data"
feature_index = {}
item_ids, train_feat, labels = pc.tidy_data( train_data_sets, train_label_sets, feature_index = feature_index )

train_set, test_set, evaluation_set = pc.create_train_sets( train_feat, labels, train_frc=0.75, test_frc=0.2, evaluation_frq=0, method='simple' )

print "train classifier"
clf = svm.SVC()
clf.fit( train_set['data'], train_set['labels'] )
print "evaluate classifier"
test_prediction = clf.predict( test_set['data'] )
test_score = average_precision_score( test_set['labels'], test_prediction )
print "score: " + str( test_score )

group.terminate()

'''
#test
print "test"

print "preprocess test"
print "send data"
for i in range(3):
        start_pos = 0
        channel = channels.next()
        channel.send( (file_path+test_file_name, sample_size, start_pos, False) )

test_data_sets=[]

print "receive data"
for i in range(3):
        channel, serialized_data = queue.get()
        test_data = pickle.loads( serialized_data )
        test_data_sets.append( test_data )

group.terminate()
#test_data = pc.preprocess_data2( file_path+test_file_name, range(sample_size), targets=False )

print "tidy data"
item_ids, test_feat = pc.tidy_data( test_data_sets, feature_index = feature_index )

print "classify"
#predicted_scores = clf.predict( test_feat )

#solution =  pc.sort_solution( item_ids, predicted_scores )

#solution_file_name = '../../data/data/solution' + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + '.csv'
#pc.write_solution( solution_file_name, solution )
'''