# VirtusLab task

The task is to implement a program, which will read two csv files, join them using a specified column and then write the result to the standard output. Users are able to specify the join type. 
Assumptions:
1. Input files conform to the rfc4180
2. Header is always present
3. Rows may appear in any order
4. Each input file can be much bigger than there is available memory on the machine

My implementation is written with Python using Pandas package. Then it overrides the built-in shell *join* function with bash script to call the python program with specified arguments. 

## Usage
Program is designed to be used on linux system with python installed. The goal was to execute program with a command:
```bash
join file_path file_path column_name join_type
```
However, *join* command already exists, so it's necessary to override it.
In order to do it, before usage, call the *source* command with ```initialize.sh``` script.
```bash
source ./initialize.sh
```
The script apart from creating *join* function, makes sure that pandas is installed, which is used for joining tables in python program.
Python 3 with *pip* is required for this.

Then it is possible to call
```bash
join file1.csv file2.csv col_name join_type
```
I've created colab notebook containing example use of the function joinin employees and departments tables. 

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mjasak/VirtusLab_task/blob/dev/VLab.ipynb)

## Algorithm details

Algorithm is based on block nested loop join algorithm, but uses pandas package for speed and clarity in code. The assumption was, that the data can be bigger than available memory, so it's necessary to implement some joining algorithm.

Most basic method is performing a nested loop join. Nested loop join is done by taking one row from left (outer) table and iterate through the entire right (inner) table. Doing that in python, with extremely big datasets can take forever [[1]](https://towardsdatascience.com/guide-to-big-data-joins-python-sql-pandas-spark-dask-51b7f4fec810)
The other, much faster method is hash join, but it requires ordering the dataset and creating hash map, and I am not sure how it influences the memory usage.

| <b>Nested loop join scheme[[2]](https://www.geeksforgeeks.org/join-algorithms-in-database/)</b>|
|:--:|
| ![space-1.jpg](https://media.geeksforgeeks.org/wp-content/uploads/20190924111203/min.png) |


Method, that can work in limited memory is a modification of 1st algorithm - block nested-loop join. It divides dataset into blocks (I've called them clusters) that fits into memory and then joins every block from left table with every block from right table.
By that, I can use pandas *merge* function to join entire blocks without iterating through every row. 

| <b>Block nested loop join scheme[[2]](https://www.geeksforgeeks.org/join-algorithms-in-database/)</b>|
|:--:|
| ![space-1.jpg](https://media.geeksforgeeks.org/wp-content/uploads/20190924114850/blok-nest.png) |

The name nested loop comes from the double-loop structure of the program. Outer loop applies to left table (that's why it's called outer table) and inner loop concerns block from right table. 
It can be written by pseudo-code:
```python
for block_left in outer_table:
    for block_right in inner_table:
        pd.merge(block_left, block_right) on column_name, join specified by join_type
```

If the inner table is small enough to fit into cluster in the whole, then I performed something similar to broadcasting method used by f.e. Spark.
The right table is stored in memory, and looping is done only by outer table:
```python
block_right = entire_right_table
for block_left in outer_table:
    pd.merge(block_left, block_right) on column_name, join specified by join_type
```

Blocks are read from the csv file in every iteration, so that they're not stored in memory. Reading is done with skiprows argument set by function to contain only rows within specified index range and zero-row, which is a header to maintain the dataframe format.

The cluster size is determined based on available memory, which can be set in ```config.py``` file. The default value is 1GB, which is quite small, but it helped with testing how the algorithm works without finding any huge dataset.

It is evaluated and set in ```get_cluster_size()``` function and the steps taken are:
1. Read first 100 lines for both datasets.
2. Calculate memory per row for both datasets.
3. Calculate maximum memory per row for joined dataset as the sum of component dfs.
4. Estimate the memory per row needed.
5. Assume the memory limit is 80% of memory available (for safety reason).
5. Calculate approximated cluster size rounded to 1000.

Memory per row is calculated with ```pd.DataFrame.memory_usage()``` function. It returns the list of memory used by 
each dfs column. By summing it, I evaluate the total dataset memory usage. Comparing it with ```pd.info()``` result 
and the size of csv file, it comes as pretty accurate.

I performed some experiments on this cluster-adjusting algorithm described in section below. 

## Experiments

Experiments performed on function were done by setting various memory limits and observing memory usage with Windows Resource Manager.
Results are showed in table below.

| Memory limit | Sets clusters | Starting memory usage | Maximum memory usage | Absolute difference | Difference relative to memory limit |
|--------------|---------------|-----------------------|----------------------|---------------------|-------------------------------------|
| 1 GB         | 6-1           | 7.1 GB                | 8.3 GB               | 1.2 GB              | +20%                                |
| 2 GB         | 3-1           | 6.6 GB                | 8.9 GB               | 2.3 GB              | +15%                                |
| 4 GB         | 2-1           | 6.8 GB                | 9.7 GB               | 2.9 GB              | -27.5%                              |
| 8 GB         | 1-1           | 6.8 GB                | 11.3 GB              | 4.5 GB              | -43.75%                             |

Sets clusters column shows number of clusters in each table. For example, 3-1 means that left table was divided into 3 clusters, and right table into 1 cluster.
I registered starting memory usage (quite high due to Google Chrome opened) and maximum memory used in the process. Then calculated the difference, that I assume as memory used by program.
The last columns indicates memory usage relative to limit.  The '+' sign means limit was exceeded, '-' that limit was not reached.

With more strict memory limits, the relative error is higher and positive. For greater values, the program fits into memory limits.
It can be caused by the fact that right table is really small and fits into any cluster size.
Exceeded limits may be because of other sources of memory use in the program apart from stored data.

But the general trend is, that for higher memory limits, the usage also go higher, which means the algorithm is working, 
and with a little more tuning and testing on biggers datasets it can achieve nice results.

For this part, the tests were performed on game review database [[3]](https://www.kaggle.com/datasets/jvanelteren/boardgamegeek-reviews)
containg two tables - reviews (15m rows) and games(19k rows). 


## Sources
[1] https://towardsdatascience.com/guide-to-big-data-joins-python-sql-pandas-spark-dask-51b7f4fec810

[2] https://www.geeksforgeeks.org/join-algorithms-in-database/

[3] https://www.kaggle.com/datasets/jvanelteren/boardgamegeek-reviews