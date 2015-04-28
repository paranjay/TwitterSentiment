import sqlite3 as lite
import math
import numpy as np
import json
import sys

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

def readTweets(file_name):
	ifile = open(file_name, 'r')
	#conRead = lite.connect('../data/btce-log.db')
	print file_name
	conWrite = lite.connect('db/tweets' + file_name[5:-4]+'.db')

	with conWrite:
        
		curWrite = conWrite.cursor()
        #curRead.execute("SELECT * FROM messages;")
        
		curWrite.execute("DROP TABLE IF EXISTS tweets;")
		curWrite.execute("CREATE TABLE tweets (id integer primary key,  tweet text, location text, tweet_date text, place text);")
		ifile.readline()
		i=1
		line = ''
		while True:
			line =line + ifile.readline() + ifile.readline()
			print line
			if line == '':
				break;
			tokens = line.split('---')
			#print tokens
			if len(tokens) < 3:
				#line = line + ifile.readline()
				continue
			#print line
			query = "insert into tweets values (" + str(i) + ",\"" + tokens[1] + "\",\"" + tokens[2] + "\",\"" + tokens[3] + "\",\"" + tokens[4] +"\");"
			i = i+1
			#print query
			curWrite.execute(query)

			line = ''
        conWrite.commit()

def main():
	filename = sys.argv[1]
	readTweets(filename)

if __name__ == '__main__':
    main()
