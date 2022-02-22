__author__ = 'Jeff Currin'
__copyright__ = 'Copyright 2021, Truth Data Insights'
__credits__ = 'Jeff Currin'
__version__ = '0.1'
__maintainer__ = 'Jeff Currin, Tracy Welterlen'
__email__ = 'jeff@truthdata.net'
__status__ = 'Development'

import sys
from argparse import ArgumentParser
import bz2
import csv
import re
from datetime import datetime as dt
import os
from typing import List
from os import listdir
from os.path import getsize
from os.path import isfile, join


def parse_command_arguments(argv):
    parser = ArgumentParser(
        description="Compresses all files in a folder using BZ2 compression. "
                    "Multiple modes available (see -m)")
    parser.add_argument(
        "-m",
        "--process_mode",
        required=False,
        default=0,
        help="Process mode: \
        0 = compress all files in source folder (-i) and write to compressed folder (-c) \
        1 = uncompress files in compressed folder (-c) and write to decompressed folder (-d) \
        2 = check decompressed files (in -d folder) against source files (in -i folder)",
        type=int
    )
    parser.add_argument(
        "-i",
        "--input_path",
        required=False,
        default=' ',
        help="Path to input files",
        type=str
    )
    parser.add_argument(
        "-c",
        "--compressed_path",
        required=True,
        default=' ',
        help="Path to compressed files",
        type=str
    )
    parser.add_argument(
        "-d",
        "--decompressed_path",
        required=False,
        default=' ',
        help="Path to decompressed files",
        type=str
    )
    parser.add_argument(
        "-l",
        "--log_path",
        required=False,
        default='./compression_log.csv',
        help="Path and name for the output log file (.csv format)",
        type=str
    )
    args = parser.parse_args()
    print(f'Process mode: {args.process_mode}')
    print(f'Input files path: {args.input_path}')
    print(f'Compressed files path: {args.compressed_path}')
    print(f'Decompressed files path: {args.decompressed_path}')
    print(f'Log file path: {args.log_path}')
    return args


def filesize(path: str, onlyfiles: List[str]):
    """

    :param path:
    :param onlyfiles:
    :return:
    """
    size = 0
    for f in onlyfiles:
        p = join(path, f)
        size += getsize(p)
    return size/1000


def retrieve_files(path: str):
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    #count = len(onlyfiles)
    #total_file_size = filesize(mypath, onlyfiles)
    #print(count, total_file_size)
    return onlyfiles


def compress(csvlog: object, path: str, destination_path: str):
    """
    Compresses files in path, writing results to destination_path.
    Writes information to log file.

    :param csvlog: CSV log file object
    :param path: path to source files
    :param destination_path: path for compressed files
    :return: none
    """
    onlyfiles = retrieve_files(path)
    for f in onlyfiles:
        new_f = f"{f}.bz2"
        with open(join(path, f), 'rb') as data:
            tarbz2contents = bz2.compress(data.read(), 9)
        with open(join(destination_path, new_f), "wb") as fh:
            fh.write(tarbz2contents)
        csvlog_write(csvlog, path, f, destination_path, new_f)


def decompress(csvlog: object, destination_path: str, destination_decompress: str):
    """
    Decompresses files in destination_path and writes results to
    destination_decompress. These results may be used to compare source
    files to decompressed files to ensure that no corruption occurred
    during compression.

    TODO: if we decide not to call csvlog_write from here, we don't need to
    TODO: pass in csvlog object

    :param csvlog: CSV log file object
    :param destination_path: path to folder with compressed files
    :param destination_decompress: path to folder with decompressed files
    :return: none
    """
    onlyfiles = retrieve_files(destination_path)
    for f in onlyfiles:
        new_f = f"{f[:-4]}"  # [-4] removes .bz2 in filename
        with open(join(destination_path, f), 'rb') as data:
            tarbz2contents = bz2.decompress(data.read())
        with open(join(destination_decompress, new_f), "wb") as fh:
            fh.write(tarbz2contents)
        # with open(join(destination_path, f), 'rb') as source, \
        #         open(join(destination_decompress, new_f), 'wb') as dest:
        #     dest.write(bz2.decompress(source.read()))


def filecmp(input_path, decompressed_path):
    onlyfiles = [f for f in listdir(input_path) if isfile(join(input_path, f))]

    match, mismatch, errors = filecmp.cmpfiles(input_path, decompressed_path, onlyfiles,
                                               shallow=False)

    # Print the result of
    # deep comparison
    print("Deep comparison:")
    print("Match :", match)
    print("Mismatch :", mismatch)
    print("Errors :", errors)


def csvlog_write(csvlog: object, path: str, f: str, destination_path: str, new_f: str):
    """
    Writes a line of data to the log file.

    TODO we need to talk about when we call this. Maybe only for compression

    :param csvlog: CSV log file object
    :param path: path to initial state file
    :param f: initial state file name
    :param destination_path: path to the destination state file
    :param new_f: destination state file name
    :return: none
    """
    origsize = filesize(path, [f])
    compsize = filesize(destination_path, [new_f])
    timestamp = dt.now()
    date_time_str = timestamp.strftime("%m/%d/%Y, %H:%M:%S")
    csvlog.writerow([date_time_str, path, f, destination_path,
                     origsize, compsize, origsize / compsize])


def main(argv):
# def main():
    """
    Command line tool for file compression with the following mode options:
    0 = compress all files in source folder (-i) and write to compressed folder (-c) \
    1 = uncompress files in compressed folder (-c) and write to decompressed folder (-d) \
    2 = check decompressed files (in -d folder) against source files (in -i folder)",

    Run with -h to see all command line options.

    :param argv:
    :return:
    """
    command_line_arguments = parse_command_arguments(argv)

    process_mode = command_line_arguments.process_mode
    input_path = command_line_arguments.input_path
    compressed_path = command_line_arguments.compressed_path
    decompressed_path = command_line_arguments.decompressed_path
    log_path = command_line_arguments.log_path

    # input_path = "D:\\Air Methods\\Compression Test\\Data Compress test"
    # compressed_path = "D:\\Air Methods\\Compression Test\\Compressed Data"
    # decompressed_path = "D:\\Air Methods\\Compression Test\\Decompressed data"
    # log_path = "./compression_log.csv"

    # open logfile
    if not isfile(log_path):
        logfile = open(log_path, 'w', newline='')
        writer = csv.writer(logfile)
        writer.writerow(['Date', 'Original_path', 'Filename', 'Destination_path',
                         'Original_file_size', 'New_file_size', 'Compression_factor'])
    else:
        logfile = open(log_path, 'a+', newline='')
        writer = csv.writer(logfile)

    # mode 0: compress files
    if process_mode == 0:
        if input_path == ' ' or compressed_path == ' ':
            print('ERROR: input_path (-i) and compressed_path (-c) '
                  'must be set for Mode 0')
            exit(-1)
        compress(writer, input_path, compressed_path)

    # mode 1: decompress files
    elif process_mode == 1:
        if compressed_path == ' ' or decompressed_path == ' ':
            print('ERROR: compressed_path (-c) and decompressed_path (-d)'
                  ' must be set for Mode 1')
            exit(-1)
        decompress(writer, compressed_path, decompressed_path)

    # mode 2: compare source files with decompressed files
    elif process_mode == 2:
        if compressed_path == ' ' or decompressed_path == ' ':
            print('ERROR: input_path (-i) and decompressed_path (-d)'
                  ' must be set for Mode 2')
            exit(-1)
        filecmp(input_path, decompressed_path)


    else:
        print("Error: not a choice")


if __name__ == "__main__":
    main(sys.argv[1:])
    # main()