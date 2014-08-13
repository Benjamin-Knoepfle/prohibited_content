
import prohibited_code as pc

from sklearn.externals import joblib

from sklearn import svm
from sklearn.linear_model import Ridge
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import Perceptron

from sklearn.metrics import average_precision_score
from sklearn.cross_validation import train_test_split
from sklearn.grid_search import GridSearchCV

import execnet, remote_preprocess, remote_classify
import cPickle as pickle
import itertools
import math

import datetime

classifier_file_path = '../../data/data/'
classifier_file_name = 'clf.dump'

file_path = '../../data/raw_data/'
train_file_name = 'avito_train.tsv'
test_file_name = 'avito_test.tsv'
train_max_samples = 3995802
test_max_samples = 1351242

preprocess_filter = [] #['fifty_fifty']

sample_size = 50000
train_parallel = False
classifier_merge = None


#train
def train():
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

	
        print "calculate number of splits for train_data"
        num_splits = train_max_samples/sample_size +1
	print str(num_splits) + " splits needed for sample_size " + str(sample_size)

        messages = []
        for i in range( num_splits ):
                start_pos = i*sample_size
                if start_pos > train_max_samples: continue
                messages.append( (file_path+train_file_name, sample_size, start_pos, True, preprocess_filter) )


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
		        if serialized_data == -1: 
				print "Error with data at split " + str(i) 
				continue
		        item_id, train_feat, train_labels = pickle.loads( serialized_data )
		        item_ids.extend( item_id )
		        train_data = pc.csr_vappend( train_data, train_feat )
		        train_label_sets.extend( train_labels )
	          
	        #train_data, labels = pc.preprocess_data2( file_path+train_file_name, range(sample_size), targets=True )



	        train_set, test_set, evaluation_set = pc.create_train_sets( train_data, train_label_sets, train_frc=1, test_frc=0, evaluation_frq=0, method='simple' )

	        print "train classifier"
		#X_train, X_test, y_train, y_test = train_test_split( train_data, train_label_sets, test_size=0.5, random_state=0)
		#param_grid = [ { 'alpha':[ 0.0001, 0.001, 0.01, 0.1, 1 ] , 'class_prior':[ [0.93, 0.07], None] } ]
		#clf = GridSearchCV( MultinomialNB(), param_grid, cv=5, n_jobs=4, scoring='average_precision' )

		clf = MultinomialNB( alpha=0.0001, class_prior=[0.93, 0.07] )
	        clf.fit( train_set['data'], train_set['labels'] )

	       	#print("Best parameters set found on development set:")
    		#print()
		#print(clf.best_estimator_)
		#print()
		#print("Grid scores on development set:")
		#print()
		#for params, mean_score, scores in clf.grid_scores_:
		#	print("%0.3f (+/-%0.03f) for %r"
		#	      % (mean_score, scores.std() / 2, params))
		#print()


        joblib.dump( clf, classifier_file_path+classifier_file_name )


####################################################################################
##      This is the classification tast                                           ##
####################################################################################

#test
classify_sample_size = 25000

def classify():
        print "classify"

        clf = joblib.load( classifier_file_path+classifier_file_name, mmap_mode='c' )

        classifier_pipeline = pickle.dumps( pc.classify_pipeline )
        classifier = pickle.dumps( clf ) 

        group = execnet.Group( ['popen']*3 )
        mch = group.remote_exec( remote_classify )
        for i in range( len(mch) ):
                mch[i].send( classifier )
	        mch[i].send( classifier_pipeline )

        queue = mch.make_receive_queue( endmarker=-1 )
        channels = itertools.cycle( mch )



        print "preprocess test"

        print "calculate number of splits for test_data"
        num_splits = test_max_samples/classify_sample_size +1
        #num_splits = 5
        print str(num_splits) + " splits needed for sample_size " + str(sample_size)

        messages = []
        for i in range( num_splits ):
                start_pos = i*sample_size
                if start_pos > train_max_samples: continue
                messages.append( (file_path+test_file_name, sample_size, start_pos, []) )


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
        pc.write_solution( solution_file_name, solution )


train()
classify()
