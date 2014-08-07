
# coding= iso-8859-15

import sys
import csv

import pandas as pd
import numpy as np
import re
import os
from collections import defaultdict

from nltk.tokenize import RegexpTokenizer
from nltk import FreqDist

import scipy.sparse as sp



engChars = [ord(char) for char in "cCyoOBaAKpPeE"]
rusChars = [ord(char) for char in "сСуоОВаАКрРеЕ"]
eng_rusTranslateTable = dict(zip(engChars, rusChars))
rus_engTranslateTable = dict(zip(rusChars, engChars))


def read_items(file_name, items_index=[] ):
        """ Reads data file. """
    
        with open( file_name ) as items_fd:
                print "Reading data..."
                itemReader=csv.DictReader(items_fd, delimiter='\t', quotechar = '"')
                items_limit = len( items_index ) != 0
                itemNum = 0
                for i, item in enumerate(itemReader):
                    item = {featureName:featureValue.decode('utf-8') for featureName,featureValue in item.iteritems()}
                    if not items_limit or i in items_index:
                        itemNum += 1
                        yield itemNum, item
                    if itemNum == len( items_index ): return

def get_words( text, correction=False ):
        tokenizer = RegexpTokenizer( '\s+', gaps=True )
        words = tokenizer.tokenize( str(text).lower() )
        if correction:
                words = [ correct_word(word) for word in words ]
        return words

def file_to_data_frame( file_path, num_of_rows=0, skip_rows=0 ):
        
        if(num_of_rows == 0):
                return pd.read_csv( file_path, sep='\t')
        else:
                if( skip_rows == 0 ):
                        return pd.read_csv( file_path, sep='\t', nrows=num_of_rows )
                #Hack, but I do not know better :(
                names = pd.read_csv( file_path, sep='\t', nrows=1 ).columns
                return pd.read_csv( file_path, sep='\t', nrows=num_of_rows, skiprows=skip_rows, names=names )
        

def preprocess_data( file_name, number_items, start_pos=0, targets=False, _filter=[] ):
        """ Processing data. """
        print " Processing data. "

        items_list = []
        samples = []
        labels = []

        data = file_to_data_frame( file_name, number_items, start_pos )
        data[['category','subcategory','title','description']] = data[['category','subcategory','title','description']].astype( str ) 

        for processed_cnt, item in data.iterrows():
                
                features_category = item['category']
                features_subcategory = item['subcategory']
                features_title = item['title']
                features_description = item['description']

                # simple wordfrequency, no filtering
                words = get_words( features_category +' '+ features_subcategory+ ' '+ features_title+' '+ features_description )
                features_words = FreqDist( words )

                #insert here code for attr
                features_attr = {}
                # add this to words ???

                features_price = item['price']
                features_phone = item['phones_cnt']
                features_email = item['emails_cnt']
                features_urls = item['urls_cnt']

                features = { }
                features[ 'item_id' ] = item['itemid'] 
                features[ 'words' ] = features_words 
                features[ 'price' ] = float(features_price)
                features[ 'phone' ] = float(features_phone)
                features[ 'email' ] = float(features_email)
                features[ 'urls' ] = float(features_urls)
                
                samples.append( features )


                if processed_cnt%1000 == 0:
                        print str(processed_cnt)+" items processed"

        if targets:
                labels = data.is_blocked
                return samples, labels
        else:
                return samples



def tidy_data( data_list, label_list=[], feature_index = {} ):

        targets = len( label_list ) >0
        
        if (targets and len(data_list)!=len(label_list)):
                print "Error; Not enough labels for data"
                return
        
        labels = []
        item_ids = []

        col = []
        row = []
        cur_row = 0
        data = []

        if len( feature_index)==0:
                feature_index['price'] = 0
                feature_index['phone'] = 1
                feature_index['email'] = 2
                feature_index['urls'] = 3
                index = 3
        else:
                index = len( feature_index )-1

        for i in range( len( data_list ) ):

                print 'Processing Samplelist no ' + str(i)
                samples = data_list[i]

                if (targets and len(samples)!=len(label_list[i])):
                        print "Error; Not enough labels for data"
                        return   
                if targets:
                        labels.extend( label_list[i] )             

                for sample in samples:
                        
                        item_ids.append( int(sample[ 'item_id' ]) )

                        data.append( sample['price'] )
                        row.append(cur_row)
                        col.append( feature_index['price'] )

                        data.append( sample['phone'] )
                        row.append(cur_row)
                        col.append( feature_index['phone'] )

                        data.append( sample['email'] )
                        row.append(cur_row)
                        col.append( feature_index['email'] )

                        data.append( sample['urls'] )
                        row.append(cur_row)
                        col.append( feature_index['urls'] )

                        for word, count in sample['words'].iteritems():
                                if word not in feature_index:
                                        index += 1
                                        feature_index[ word ] = index
                                        
                                else:
                                        index = feature_index[ word ]
                                data.append( count )
                                row.append( cur_row )
                                col.append( index )


                        cur_row += 1
     
        features = sp.csr_matrix((data,(row,col)), shape=(cur_row, 1000000000), dtype=np.float64)

        if targets:
                return item_ids, features, labels
        else:
                return item_ids, features
                        


def create_train_sets( data, labels, train_frc=1, test_frc=0, evaluation_frq=0, method='simple' ):
        print "create_train_sets"

        number_of_samples = np.shape( data )[0]
        number_of_train_samples = int(number_of_samples * train_frc)
        number_of_test_samples = int(number_of_samples * test_frc)
        number_of_evaluation_samples =int( number_of_samples * evaluation_frq)

        train_set = {}
        test_set = {}
        evaluation_set = {}

        if method == 'simple':
        
                train_set['data'] = data[:number_of_train_samples,:]
                train_set['labels'] = labels[:number_of_train_samples]

                if test_frc != 0:

                        test_max = number_of_train_samples+number_of_test_samples
                        test_set['data'] = data[number_of_train_samples:test_max,:]
                        test_set['labels'] = labels[number_of_train_samples:test_max]

                        
                        evaluation_set['data'] = data[test_max:,:]
                        evaluation_set['labels'] = labels[test_max:]


        return train_set, test_set, evaluation_set


def sort_solution( item_ids, predicted_scores ):
        print "sort_solution"
        return sorted(zip(predicted_scores, item_ids), reverse = True)


def write_solution( file_name, solution ):
        print "write_solution"
        f = open( file_name, "w")
        f.write("id\n")
    
        for pred_score, item_id in solution:
                f.write("%d\n" % (item_id))
        f.close()




