from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_list, explode, size, sum
import sys
import argparse

DEBUG=True

CONTRIB = "contrib"
FS_SIZE = "fs_size"
FS = "fs"
LINK_TO = "link_to"
RANK = "rank"
PAGE = "page"

DEFAULT_DUMPING_FACTOR = 0.85
DEFAULT_ITERATIONS = 20


def run(input_file, dumping_factor = 0.85, n_iter = 20, consider_n_pages = False):
    """
    :param input_file:
    :param dumping_factor:
    :param n_iter: maximum number of iterations
    :return:
    """
    spark_session = SparkSession.builder.master("local").getOrCreate()
    data_df = spark_session.read.csv(input_file).toDF(PAGE, LINK_TO)
    print("Input data:")
    data_df.show()

    forward_stars_df = data_df.groupBy(PAGE).agg(collect_list(LINK_TO).alias(FS)
                                                 , size(collect_list(LINK_TO)).alias(FS_SIZE))\
        .cache()
    print("Forward stars:")
    forward_stars_df.show()
    n_pages = 1
    if consider_n_pages:
        n_pages = forward_stars_df.count()
    ranks_df = forward_stars_df.select(col(PAGE),lit(1/n_pages).alias(RANK))
    ranks_df.cache()

    if DEBUG:
        print("Initial rank:")
        ranks_df.show()

    i=0

    while i < n_iter:

        joined_df = forward_stars_df.join(ranks_df, on=PAGE)

        if DEBUG:
            print("Join forward stars with ranks")
            joined_df.show()
        contrib_df = joined_df.select(explode(FS).alias(PAGE),col(RANK)/col(FS_SIZE))

        if DEBUG:
            print("Page Rank contrib of each page included in the BS of the page")
            contrib_df.show()
        contrib_df = contrib_df.toDF(PAGE, CONTRIB)

        partial_ranks_df = contrib_df.groupby(PAGE).agg(sum(CONTRIB).alias(RANK))

        if DEBUG:
            print("Partial ranks")
            partial_ranks_df.cache()
            partial_ranks_df.show()

        ranks_df = partial_ranks_df.select(col(PAGE)
                                           , col(RANK)*dumping_factor+(1-dumping_factor)/n_pages)\
            .toDF(PAGE,RANK)

        ranks_df.cache()

        if DEBUG:
            print("Ranks at iteration %d" % i)
            ranks_df.show()


        

        i+=1


    print("Final ranks")
    ranks_df.orderBy(RANK).show()



def main(argv):

    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="urls input file as csv")
    parser.add_argument("-d", help="Dumping factor", type=float, default=DEFAULT_DUMPING_FACTOR)
    parser.add_argument("--maxiter", help="Maximum number of iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--consider_pages", help="Consider the number of pages in the computation", type=bool, default=False)
    args = parser.parse_args()

    input_file = args.input_file
    dumping_factor = args.d
    n_iter = args.maxiter
    consider_n_pages = args.consider_pages

    print("input: %s" % input_file)
    print("dumping factor: %s" % dumping_factor)
    print("Max iterations: %s" % n_iter)
    print("Consider number of pages: %s" % consider_n_pages)

    run(input_file, dumping_factor, n_iter, consider_n_pages)

main(sys.argv[1:])