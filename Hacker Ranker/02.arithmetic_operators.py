# Task
# Read two integers from STDIN and print three lines where:

# The first line contains the sum of the two numbers.
# The second line contains the difference of the two numbers (first - second).
# The third line contains the product of the two numbers.
# Input Format

# The first line contains the first integer, . The second line contains the second integer, .

# Constraints



# Output Format

# Print the three lines as explained above.

if __name__ == '__main__':
    a = int(input())
    b = int(input())

def arithmetic_operators(a, b):
    if (a >= 1 and a <= 10**10) and (b >= 1 and b <= 10**10):
        first = a + b
        print(first)
        second = a - b
        print(second)
        third = a * b
        print(third)
    else:
        print("input integers should be in the range of (1, 10^(10))")
        
arithmetic_operators(a,b)
