#!/bin/python

import sys
from cases.classify_popularity import ClassifyPopularity
from cases.relation_popularity import RelationPopularity

def main():

    if len(sys.argv) != 3:
        print("\033[91;1mInvalid number of arguments. Please provide an option and a file.\033[0m")
        print("\033[91;1mUsage: python main.py <option> <file>\033[0m")
        sys.exit(1)

    _, n, file = sys.argv
    
    if n == '1':
        classifier = ClassifyPopularity(file)

        print("\033[92mClassifying popularity using DataFrame method...\033[0m") 
        classifier.dataframe_method()

        print("\033[92mClassifying popularity using SQL method...\033[0m")
        classifier.sql_method()

        classifier.stop_session()
    elif n == '2':
        pass
    elif n == '3':
        pass
    elif n == '4':
        relationer: RelationPopularity = RelationPopularity(file)

        print("\033[92mClassifying popularity using DataFrame method...\033[0m")
        relationer.dataframe_method()

        print("\033[92mClassifying popularity using SQL method...\033[0m")
        relationer.sql_method()

        relationer.stop_session()
    else:
        print("\033[91;1mInvalid option. Please choose a number between 1 and 4.\033[0m")
        print("\033[93;1mUsage:\033[0;93m python main.py <option> <file>\033[0m")
        print("\033[93;1mOptions:\033[0m")
        # TODO: Add options
        sys.exit(1)

if __name__ == "__main__":
    main()