# Task
# Given an integer, , perform the following conditional actions:

# If  is odd, print Weird
# If  is even and in the inclusive range of  to , print Not Weird
# If  is even and in the inclusive range of  to , print Weird
# If  is even and greater than , print Not Weird
# Input Format

# A single line containing a positive integer, .

# Constraints

# Output Format

# Print Weird if the number is weird; otherwise, print Not Weird.

#!/bin/python

import math
import os
import random
import re
import sys

n = int(raw_input())
def if_else(n):
    if n > 1 and n < 101:
        if n % 2 == 1: 
            print("Weird")
        else:
            if n % 2 == 0 and 2 <= n and n <= 5:
                print("Not Weird")
            elif n % 2 == 0 and 6 <= n and n <= 20:
                print("Weird")
            elif n % 2 == 0 and n > 20:
                print("Not Weird")
    else:
        print("n should be in the range of (1, 100")

if_else(n)
