
import prohibited_code as pc

from sklearn import svm
from sklearn.linear_model import Ridge
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import Perceptron
from sklearn.metrics import average_precision_score

import execnet, remote_preprocess, remote_classify
import cPickle as pickle
import itertools
import math

import datetime


file_path = '../../data/raw_data/'
train_file_name = 'avito_train.tsv'
test_file_name = 'avito_test.tsv'
train_max_samples = 3995802
test_max_samples = 1351242

sample_size = 50000
train_parallel = False
classifier_merge = None


#train
print "distributed train"

if train_parallel:
	print "Needs implementation"
	distribute_pipeline = pc.base_pipeline
else:
	distribute_pipeline = pc.base_pipeline
preprocessor = pickle.dumps( distribute_pipeline )

group = execnet.Group( ['popen']*4 )
mch = group.remote_exec( remote_preprocess )
for i in range( len(mch) ):
        mch[i].send( preprocessor )

queue = mch.make_receive_queue( endmarker=-1 )
channels = itertools.cycle( mch )

num_splits = 4

messages = []
for i in range( num_splits ):
        start_pos = i*sample_size
        if start_pos > train_max_samples: continue
        messages.append( (file_path+train_file_name, sample_size, start_pos, True) )


print "send data"
for message in messages:
        
        channel = channels.next()
        channel.send( message )


clf = None
if train_parallel:
	print "needs implementation"
	print "distribution of training"
	print "merge classifiers"
	if classifier_merge == 'average':
		print "implement average merging"
	elif classifier_merge == 'vote':
		print "implement vote merging"
	else:
		print "Ooops, invalid merge method"
	
else:
	item_ids = []
	train_data = None
	train_label_sets=[]

	print "receive data"
	for i in range( len( messages ) ):
		channel, serialized_data = queue.get( )
		print "Split number " +str(i)+ " received"
		if serialized_data == -1: print "Error with data;" 
		item_id, train_feat, train_labels = pickle.loads( serialized_data )
		item_ids.extend( item_id )
		train_data = pc.csr_vappend( train_data, train_feat )
		train_label_sets.extend( train_labels )
	  
	#train_data, labels = pc.preprocess_data2( file_path+train_file_name, range(sample_size), targets=True )



	train_set, test_set, evaluation_set = pc.create_train_sets( train_data, train_label_sets, train_frc=1, test_frc=0, evaluation_frq=0, method='simple' )

	print "train classifier"
	#clf = svm.SVC()

	#clf.fit( train_set['data'], train_set['labels'] )

	if(len( test_set )>0):
		print "evaluate classifier"
		test_prediction = clf.predict( test_set['data'] )
		test_score = average_precision_score( test_set['labels'], test_prediction )
		print "score: " + str( test_score )



group.terminate()


####################################################################################
##      This is the classification tast                                           ##
####################################################################################

#test
print "test"

classifier_pipeline = pickle.dumps( pc.classify_pipeline )
classifier = pickle.dumps( clf ) 

group = execnet.Group( ['popen']*4 )
mch = group.remote_exec( remote_classify )
for i in range( len(mch) ):
        mch[i].send( classifier )
	mch[i].send( classifier_pipeline )

queue = mch.make_receive_queue( endmarker=-1 )
channels = itertools.cycle( mch )



print "preprocess test"

print "calculate number of splits for test_data"
num_splits = test_max_samples/sample_size +1
#num_splits = 5
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

item_ids = []
predicted_scores = []

print "receive data"
for i in range( len( messages ) ):
        channel, serialized_data = queue.get( )
        print "Split number " +str(i)+ " received"
        if serialized_data == -1: print "Error with data;" 
        item_id, prediction = pickle.loads( serialized_data )
        item_ids.extend( item_id )
	predicted_scores.extend( prediction )
        print "classify"


#test_data = pc.preprocess_data2( file_path+test_file_name, range(sample_size), targets=False )


print "done classifying"


solution =  pc.sort_solution( item_ids, predicted_scores )

solution_file_name = '../../data/data/solution' + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + '.csv'
#pc.write_solution( solution_file_name, solution )

