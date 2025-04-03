#!/bin/python

import sys
import os
from cases.classify_popularity import ClassifyPopularity
from cases.artist_popularities import ArtistsPopularities
from cases.relation_popularity import RelationPopularity
from cases.genres_popularities import GenresPopularities

def main():
    if not os.path.exists("out"):
        os.makedirs("out")

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
        artists_pop = ArtistsPopularities(file)
        
        print("\033[92mAnalyzing artists popularities using DataFrame method...\033[0m")
        artists_pop.dataframe_method()
        # df_result = artists_pop.dataframe_method()
        
        print("\033[92mAnalyzing artists popularities using SQL method...\033[0m")
        artists_pop.sql_method()
        # sql_result = artists_pop.sql_method()
        
        # Opcional: guardar resultados
        # output_dir = "tests/artists_popularities"
        # artists_pop.save_results(df_result, f"{output_dir}/dataframe_result")
        # artists_pop.save_results(sql_result, f"{output_dir}/sql_result")
        
        artists_pop.stop_session()
    elif n == '3':
        analyzer = GenresPopularities(file)

        print("\033[92mAnalyzing genre popularities using DataFrame method...\033[0m")
        analyzer.dataframe_method()

        print("\033[92mAnalyzing genre popularities using SQL method...\033[0m")
        analyzer.sql_method()

        analyzer.stop_session()
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
        print("\033[93m  1: Classify song popularity\033[0m")
        print("\033[93m  2: Analyze artists popularities\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    main()