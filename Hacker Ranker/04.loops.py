# Task
# Read an integer . For all non-negative integers , print . See the sample for details.

# Input Format

# The first and only line contains the integer, .

# Constraints


# Output Format

# Print  lines, one corresponding to each .

# Sample Input 0

# 5
# Sample Output 0

# 0
# 1
# 4
# 9
# 16

if __name__ == '__main__':
    n = int(input())

def loops(N):
    # i = non -negative integers and i < N
    # N = an integer
    if N >=1 and N <=20:
        for i in range(0,N):
            print(i**2)
    else:
        print("N should be in the range of (1, 20)")

loops(n)
