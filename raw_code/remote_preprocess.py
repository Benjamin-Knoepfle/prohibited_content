
import cPickle as pickle

if __name__ == '__channelexec__':
        
        try:
                preprocessor = pickle.loads( channel.receive() )

                for param in channel:
                        file_name = param[0]
                        sample_size = param[1]
                        start_pos = param[2]
                        targets = param[3]
                        processed = preprocessor( file_name, sample_size, start_pos, targets )
                        serialized_processed = pickle.dumps( processed )
                        channel.send( serialized_processed )
        except( BaseException ):
                channel.send( -1 )
