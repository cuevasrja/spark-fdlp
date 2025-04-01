#!/bin/python

import sys

def main():

    if len(sys.argv) != 3:
        print("\033[91;1mInvalid number of arguments. Please provide an option and a file.\033[0m")
        print("\033[91;1mUsage: python main.py <option> <file>\033[0m")
        sys.exit(1)

    _, n, file = sys.argv
    
    if n == '1':
        pass
    elif n == '2':
        pass
    elif n == '3':
        pass
    elif n == '4':
        pass
    else:
        print("\033[91;1mInvalid option. Please choose a number between 1 and 4.\033[0m")
        print("\033[93;1mUsage:\033[0;93m python main.py <option> <file>\033[0m")
        print("\033[93;1mOptions:\033[0m")
        # TODO: Add options
        sys.exit(1)

if __name__ == "__main__":
    main()