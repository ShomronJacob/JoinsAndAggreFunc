import csv
import pandas as pd
import numpy as np
import pprint
print("This code implements Joins and aggregate function called count on two csv files")
print("These csv files are ratings_small.csv and tags.csv")
uchoice = None
while uchoice is None:
    input_value = input("Please enter 1 for Joins and 2 for Aggregation function: ")
    try:
        # try and convert the string input to a number
        uchoice = int(input_value)
    except ValueError:
        # tell the user off
        print("{input} is not a number, please enter a number only".format(input=input_value))
if uchoice ==1:
    jchoice = None
    while jchoice is None:
        ij = input("Please enter 3 for Natural join or 4 for Left Inner Join: ")
        try:
            # try and convert the string input to a number
            jchoice = int(ij)
        except ValueError:
            # tell the user off
            print("{input} is not a number, please enter a number only".format(input=ij))
    if jchoice ==3:
        T1 = [] #List
        with open('ratings_small.csv', newline='') as f:
            reader1 = csv.reader(f)
            header_Table1 = next(reader1)
        T1 = header_Table1
        print("The following forms the header from table 1-:", T1)
        print(" ")

        T2 = () #Tuple
        with open('tags.csv', newline='') as f:
            reader2 = csv.reader(f)
            header_Table2 = next(reader2)
            T2 = header_Table2
            copy = T2
        print("The following forms the header from table 2-:", T2)
        print(" ")
        T2 = tuple(T2)

        common = []
        diff = []
        diff = set(copy) - set(T1)
        common = set(T1) & set(copy)
        print("The common headers are ", common)
        print(" ")
        print("The extra header is", diff)
        print(" ")
        print(" ")

        t1tuples = []
        with open('ratings_small.csv', newline='') as f:
            next(f)
            for line in f:
                t1tuples.append(tuple(line.strip().split(',')))

        # converting list of tuples to list of list
        t1tuples = [list(elem) for elem in t1tuples]

        t2tuples = []
        with open('tags.csv', newline='') as f:
            next(f)
            for line in f:
                t2tuples.append(tuple(line.strip().split(',')))

        t2tuples = [list(elem) for elem in t2tuples]

        t1columns = set(T1)
        t2columns = set(T2)

        t1map = {k: i for i, k in enumerate(T1)}
        t2map = {k: i for i, k in enumerate(T2)}

        join_on = t1columns & t2columns
        diff = t2columns - join_on


        def match(row1, row2):
            return all(row1[t1map[rn]] == row2[t2map[rn]] for rn in join_on)


        results = []

        for t1row in t1tuples:
            # print(t1row)
            for t2row in t2tuples:
                # print(t2t)
                if match(t1row, t2row):
                    row = t1row[:]
                    # print(row)
                    for rn in diff:
                        row.append(t2row[t2map[rn]])
                    results.append(row)
        print("Natural join on the above two csv files gives the following results-:\n")
        # pprint.pprint(results)

        df_NaturalJoin = pd.DataFrame(results)
        df_NaturalJoin.columns = ['User', 'movieId', 'rating', 'timestamp', 'tag']
        # df_NaturalJoin.columns = ['A', 'B', 'C', 'D', 'E']


        length = len(df_NaturalJoin)
        df_NaturalJoin.head(length).to_csv('NaturalJoin.csv',encoding='utf-8')
        print(df_NaturalJoin)
    if jchoice == 4:
        T1 = pd.read_csv("ratings_small.csv")
        T2 = pd.read_csv("tags.csv")
        df_t1 = pd.DataFrame(T1)
        df_t2 = pd.DataFrame(T2)

        # Implementation of inner join on dataframes
        # select * from df_t1 join df_t2 on A > E;

        ia, ib = np.where(np.less.outer(df_t1.movieId, df_t2.userId))
        # ia, ib = np.where(np.less.outer(df_t1.A, df_t2.E))
        print("First table is-:")
        print(df_t1)
        print(" ")
        print("Second table is-:")
        print(df_t2)
        df_result = pd.concat((df_t1.take(ia).reset_index(drop=True), df_t2.take(ib).reset_index(drop=True)), axis=1)
        # print(df_result)
        L = len(df_result)
        df_result.head(L).to_csv('InnerJoin.csv',encoding='utf-8') # Logging the results into a csv file
        print(df_result)

        # Implementing Left Inner Join
        # To calculate the left outer join, I am using numpy.setdiff1d()
        # to find all the rows of df_t1 that not in the inner join

        n_t1 = np.setdiff1d(np.arange(len(df_t1)), ia)
        n_t2 = -1 * np.ones_like(n_t1)
        o_t1 = np.concatenate((ia, n_t1))
        o_t2 = np.concatenate((ib, n_t2))
        df_LeftinnerJoin = pd.concat([df_t1.take(o_t1).reset_index(drop=True), df_t2.take(o_t2).reset_index(drop=True)],
                                     axis=1)
        L1 = len(df_LeftinnerJoin)
        df_result.head(L).to_csv('LeftInnerJoin.csv',encoding='utf-8')  # Logging the results into a csv file

if uchoice == 2:
    T1 = pd.read_csv("ratings_small.csv")
    T2 = pd.read_csv("tags.csv")
    df_t1 = pd.DataFrame(T1)
    df_t2 = pd.DataFrame(T2)

    print("Which table do you want to work on?")
    print("Table 1 gives the ratings of a movie by different users")
    print("Table 2 gives the tags of a movie")

    achoice=None
    while achoice is None:
        iaa = input("Please enter 5 for Table 1 or 6 for table 2: ")
        try:
            # try and convert the string input to a number
            achoice = int(iaa)
        except ValueError:
            # tell the user off
            print("{input} is not a number, please enter a number only".format(input=iaa))
    if achoice == 5:
        print("You have selected table 1!")
        print("")
        print(df_t1)
        print("Aggregate function count is implemented for this table")
        count_T1 = df_t1.apply(pd.Series.value_counts)
        print(count_T1)
        count_T1.to_csv('aggCount_T1.csv',encoding='utf-8')
    elif achoice == 6:
        print("You have selected table 2")
        print("")
        print(df_t2)
        print("Aggregate function count is implemented for this table")
        count_T2 = df_t2.apply(pd.Series.value_counts)
        print(count_T2)
        count_T2.to_csv('aggCount_T2.csv',encoding='utf-8')
    elif achoice != 5 or achoice != 6:
        print("Wrong choice entered")
