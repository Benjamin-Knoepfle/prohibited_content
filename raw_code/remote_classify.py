
import cPickle as pickle

if __name__ == '__channelexec__':
        
        try:
		clf = pickle.loads( channel.receive() )
                classifier = pickle.loads( channel.receive() )

                for param in channel:
                        file_name = param[0]
                        sample_size = param[1]
                        start_pos = param[2]
			_filter = param[3]
                        classified = classifier( clf, file_name, sample_size, start_pos, _filter )
                        serialized_classified = pickle.dumps( classified )
                        channel.send( serialized_classified )
        except( BaseException ):
                channel.send( -1 )
