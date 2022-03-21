import argparse
import pandas as pd
import sys
import os

# CONSTANTS

from config import MEMORY_AVAILABLE


def skip_func(x, low_range, high_range):
    """
    Help function to filter out desired filerows.

    Function to parse through csv file and select rows with indexes belonging to
    corresponding cluster. Header with index 0 cannot be skipped, to keep the dataframe's
    format. All rows with indexes out of the expected range return True value, which means they
    should be ommited while reading.
    """

    if x == 0:
        return False
    elif x < low_range:
        return True
    elif x > high_range:
        return True
    else:
        return False


def get_cluster_size(file1, file2, memory_available):

    # Calculating number of rows in files
    nrows1 = -1  # Header should not be counted to dataset size
    for row in open(file1, encoding="utf-8"):
        nrows1 += 1

    nrows2 = -1
    for row in open(file2, encoding="utf-8"):
        nrows2 += 1

    # Algorithm for adjusting cluster size:
    # * read first 100 lines for both datasets
    # * calc memory per row for both datasets
    # * calc max estimated memory per row in joined df
    # * calc sum of max estimated memory per row
    # * calculate cluster size assuming memory limit is 80% of memory available

    sample_df1 = pd.read_csv(file1, encoding="utf-8", nrows=100)
    sample_df2 = pd.read_csv(file2, encoding="utf-8", nrows=100)

    memory_per_row1 = sample_df1.memory_usage().sum() / len(sample_df1)
    memory_per_row2 = sample_df2.memory_usage().sum() / len(sample_df2)
    max_join_memory_per_row = memory_per_row1 + memory_per_row2
    memory_limit = 0.8 * memory_available  # Let's have some safety factor.

    ## cluster_memory = 2*max_join_memory_per_row*cluster_size <= memory_limit
    # Joined df cannot be bigger than sum of length of its components.
    # The size of join set with maximal memory per row equals $$ max_join_memory_per_row * cluster_size $$.
    # The size of left set equals $$ memory_per_row1 * cluster_size $$. The same with right set.
    # So considering that $$ max_join_memory_per_row = memory_per_row1+memory_per_row2$$
    # The sum of left and right sets equals $$ max_join_memory_per_row * cluster_size $$.
    # So total memory needed for storing these sets is $$ 2*max_join_memory_per_row*cluster_size $$ and cannot exceed memory limit.

    max_cluster_size = memory_limit / 2 / max_join_memory_per_row
    cluster_size = round(max_cluster_size / 1000) * 1000

    nclusters1 = int(nrows1 / cluster_size) + 1
    nclusters2 = int(nrows2 / cluster_size) + 1

    return cluster_size, nclusters1, nclusters2, nrows1, nrows2


def join_function(file1, file2, col_name, join_type):
    """
    Function to join two files.

    Function joins two files on key given as col_name argument. Function relies on block nested loop join
    algorithm using pandas to process data. Type of join is determined by join_type argument. Default type is inner join.

    Parameters
    ---------
    file1 : str
        path to left side file.
    file2 : str
        path to right side file.
    col_name : str
        column name used to join two files
    join_type : str
        type of join

    Output
    ----------
    joined files printed to standard output in csv format

    """
    debug = False
    memory_available = MEMORY_AVAILABLE
    cluster_size, nclusters1, nclusters2, nrows1, nrows2 = get_cluster_size(
        file1, file2, memory_available
    )
    if debug:
        print(
            f"Left dataset was divided into {nclusters1} clusters with the size of {min(cluster_size,nrows1)} rows"
        )
        print(
            f"Right dataset was divided into {nclusters2} clusters with the size of {min(cluster_size, nrows2)} rows"
        )
        print(f"{join_type} join was selected on column {col_name}")
        print("Proceeding to joining files ...")

    # if nclusters2 is equal to 1, there is no need for 2nd, inner loop and reading csv over and over again,
    # it can be stored through the whole process, as it does not change
    # if left set is divided into 1 cluster (is not divided actually), then there's no need for the change,
    # because still only inner loop would be iterating

    if nclusters2 == 1:
        rc = pd.read_csv(file2, header=0, encoding="utf-8")
        for lci in range(nclusters1):
            # lci = 0,1,2,...
            # create left cluster
            low_range = lci * cluster_size + 1
            high_range = (lci + 1) * cluster_size
            lc = pd.read_csv(
                file1,
                header=0,
                skiprows=lambda x: skip_func(x, low_range, high_range),
                encoding="utf-8",
            )
            merged = pd.merge(lc, rc, on=col_name, how=join_type)

            # write results in csv format to standard output
            if lci == 0:
                # if this is the first join, then print results with header included
                merged.to_csv(sys.stdout, index=False, header=True)
            else:
                merged.to_csv(sys.stdout, index=False, header=False)
    else:
        for lci in range(nclusters1):
            # lci = 0,1,2,...
            # create left cluster
            low_range = lci * cluster_size + 1
            high_range = (lci + 1) * cluster_size
            lc = pd.read_csv(
                file1,
                header=0,
                skiprows=lambda x: skip_func(x, low_range, high_range),
                encoding="utf-8",
            )
            # loop over right file
            for rci in range(nclusters2):
                # create right cluster
                low_range = rci * cluster_size + 1
                high_range = (rci + 1) * cluster_size
                rc = pd.read_csv(
                    file2,
                    header=0,
                    skiprows=lambda x: skip_func(x, low_range, high_range),
                    encoding="utf-8",
                )
                # merge left and right cluster
                merged = pd.merge(lc, rc, on=col_name, how=join_type)

                # write results in csv format to standard output
                if lci == 0 and rci == 0:
                    # if this is the first join, then print results with header included
                    merged.to_csv(sys.stdout, index=False, header=True)
                else:
                    merged.to_csv(sys.stdout, index=False, header=False)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--file1", type=str, required=True)
    parser.add_argument("--file2", type=str, required=True)
    parser.add_argument("--col", type=str, required=True)
    parser.add_argument("--type", type=str, required=False)
    args = parser.parse_args()

    # Files input format is not tested due to assumption 1, that input files conform the rfc4180 standard.
    # But it's worth to check if filepaths exist

    assert os.path.exists(args.file1), "File1 path does not exist"
    assert os.path.exists(args.file2), "File2 path does not exist"

    # Default join type is inner join, like in SQL dbms. It's the most restrictive type of join and also most commonly used

    if args.type is None:
        args.type = "inner"

    # make join_type argument case-insensitive
    # and stripped (not sure whether spaces are possible to pass through terminal though)
    join_type = args.type.lower().strip() if isinstance(args.type, str) else args.type

    # only these three join types are available according to the task
    assert join_type in [
        "inner",
        "right",
        "left",
    ], "join type should be one of following: inner, left, right"

    # column name
    # check if column is in set1 and set2 column

    # # sample join on 100 rows -> check for KeyError
    try:
        sample_df1 = pd.read_csv(args.file1, encoding="utf-8", nrows=100)
        sample_df2 = pd.read_csv(args.file2, encoding="utf-8", nrows=100)

        if args.col not in list(sample_df1.columns) or args.col not in list(
            sample_df2.columns
        ):
            raise ValueError("Column name is not present in at least one of the files")

        sample_join = pd.merge(sample_df1, sample_df2, on=args.col, how=join_type)
    except ValueError as e:
        print(e)
        return
    except KeyError:
        print("Key error. Cannot join on given key")
        return

    ## seems like it is possible to join tables on selected key
    del sample_df1
    del sample_df2
    del sample_join

    join_function(args.file1, args.file2, args.col, join_type)


if __name__ == "__main__":
    main()


# command usage example
# python batch_small.py --file1 employees.csv --file2 departments.csv --col department_id
# sample_df1 = pd.read_csv(f2, encoding="utf-8", nrows=100)
# python3 join_func.py --file1 employees.csv --file2 departments.csv --col department_id

# join employees.csv departments.csv department_id
