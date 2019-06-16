from pyspark.sql import SparkSession
import sys
import argparse

DEFAULT_DUMPING_FACTOR = 0.85
DEFAULT_ITERATIONS = 20

def run(input_file, dumping_factor = 0.85, n_iter = 20):
    """
    :param input_file:
    :param dumping_factor:
    :param n_iter: maximum number of iterations
    :return:
    """
    spark_session = SparkSession.builder.master("local").getOrCreate()
    data_df = spark_session.read.csv(input_file)
    data_df.show()



def main(argv):

    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="urls input file as csv")
    parser.add_argument("-d", help="Dumping factor", type=float, default=DEFAULT_DUMPING_FACTOR, dest="DUMPING_FACTOR")
    parser.add_argument("--maxiter", help="Maximum number of iterations", type=int, default=DEFAULT_ITERATIONS, dest="ITERATIONS")
    args = parser.parse_args()

    input = args.input_file
    d = args.d
    n_iter = args.maxiter

    print("input: %s" % input)
    print("dumoing factor: %s" % d)
    print("Max iterations: %s" % n_iter)

main(sys.argv[1:])