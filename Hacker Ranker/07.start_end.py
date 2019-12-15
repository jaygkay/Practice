# These expressions return the indices of the start and end of the substring matched by the group.

# Code

# >>> import re
# >>> m = re.search(r'\d+','1234')
# >>> m.end()
# 4
# >>> m.start()
# 0
# Task
# You are given a string .
# Your task is to find the indices of the start and end of string  in .

# Input Format

# The first line contains the string .
# The second line contains the string .

# Constraints



# Output Format

# Print the tuple in this format: (start _index, end _index).
# If no match is found, print (-1, -1).

# Sample Input

# aaadaa
# aa
# Sample Output

# (0, 1)  
# (1, 2)
# (4, 5)

# Enter your code here. Read input from STDIN. Print output to STDOUT
s = str(input())
k = str(input())

import re

def start_end(s,k):
    if (0 < len(s) and len(s) < 100) and (0 < len(k) and len(k) < len(s)):
        all_matches = list(re.finditer(r'(?={})'.format(k), s))
        if all_matches:
            for i in all_matches:
                print((i.start(), i.end()+len(k)-1))
        else:
            print((-1,-1))
    else:
        print("String size s and k should be re-defined")

start_end(s,k)
