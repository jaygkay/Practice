# Read an integer .

# Without using any string methods, try to print the following:


# Note that "" represents the values in between.

# Input Format

# The first line contains an integer .

# Output Format

# Output the answer as explained in the task.

# Sample Input 0

# 3
# Sample Output 0

# 123

if __name__ == '__main__':
    n = int(input())

def print_function(n):
    lst = []
    for i in range(0, n):
        cal = (n-i)
        lst.append(cal)
    print(''.join(map(str,lst[::-1])))

print_function(n)
