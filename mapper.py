#!/usr/bin/env python
#
# This file has been provided as a starting point. You need to modify this file.
# Reads whole lines stdin; writes key/value pairs to stdout
# --- DO NOT MODIFY ANYTHING ABOVE THIS LINE ---

import sys
import re
import datetime

Reg = re.compile('\[[0-9][0-9]\/[A-Z][a-z][a-z]\/[0-9][0-9][0-9][0-9]')


if __name__ == "__main__":
	for line in sys.stdin:
		for word in line.split():
			if Reg.match(word):
				word = word[4:12]
				word = datetime.datetime.strptime(word, '%b/%Y')
				word = word.strftime('%Y-%m')
				sys.stdout.write("{}\t1\n".format(word))
        
            
                
                
                
                
