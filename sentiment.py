import numpy as np
import sqlite3 as lite
import sys
import sys
reload(sys)
sys.setdefaultencoding("utf-8") 

def readSentimentList(file_name):
    ifile = open(file_name, 'r')
    happy_log_probs = {}
    sad_log_probs = {}
    ifile.readline() #Ignore title row
    
    for line in ifile:
        tokens = line[:-1].split(',')
        happy_log_probs[tokens[0]] = float(tokens[1])
        sad_log_probs[tokens[0]] = float(tokens[2])
 
    return happy_log_probs, sad_log_probs
 
def classifySentiment(words, happy_log_probs, sad_log_probs):
    # Get the log-probability of each word under each sentiment
    happy_probs = [happy_log_probs[word.lower()] for word in words if word.lower() in happy_log_probs]
    sad_probs = [sad_log_probs[word.lower()] for word in words if word.lower() in sad_log_probs]
 
    # Sum all the log-probabilities for each sentiment to get a log-probability for the whole tweet
    tweet_happy_log_prob = np.sum(happy_probs)
    tweet_sad_log_prob = np.sum(sad_probs)
 
    # Calculate the probability of the tweet belonging to each sentiment
    prob_happy = np.reciprocal(np.exp(tweet_sad_log_prob - tweet_happy_log_prob) + 1)
    prob_sad = 1 - prob_happy
 
    return prob_happy, prob_sad
 
def main():
    # We load in the list of words and their log probabilities
    happy_log_probs, sad_log_probs = readSentimentList('twitter_sentiment_list.csv')
 
    # Here we have tweets which we have already tokenized (turned into an array of words)
    #filename = 'Conservatives-25-Apr'
    filename = sys.argv[1]
    filename = filename[5:-4]
    print filename
    conRead = lite.connect('db/tweets' + filename + '.db')
    conWrite = lite.connect('db/tweets-sentiment' + filename + '.db')
    
    with conRead:
        
        curRead = conRead.cursor()
        curWrite = conWrite.cursor()
        curRead.execute("SELECT * FROM tweets;")
        
        curWrite.execute("DROP TABLE IF EXISTS tweetsentiment;")
        curWrite.execute("CREATE TABLE tweetsentiment (id integer primary key, tweet text, location text, tweet_date text, position text,  pos_sent_prob real, neg_sent_prob real);")
        i=1;
        while True:
          
            row = curRead.fetchone()
            
            if row == None:
                print 'got none'
                break
                
            words = row[1].split(' ')
            for word in words:
                if len(word) > 0 and word[0] == '#':
                    word = word[1:]
            # Calculate the probabilities that the tweets are happy or sad
            tweet1_happy_prob, tweet1_sad_prob = classifySentiment(words, happy_log_probs, sad_log_probs)
            
            query = "insert into tweetsentiment values (" + str(i) + ",\"" + row[1] + "\",\"" +  row[2] + "\",\"" + row[3] + "\",\"" + str(row[4]) + "\"," + str(tweet1_happy_prob) + "," + str(tweet1_sad_prob) + ");"
            print query
            i=i+1
            curWrite.execute(query)

        conWrite.commit()
if __name__ == '__main__':
    main()
