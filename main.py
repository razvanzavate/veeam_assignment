import argparse
import hashlib
import logging
import os
import shutil
import sys
import time

from logging import Logger


def configure_logger(log_path) -> Logger:
    """ Configures and returns the logger object to be used

    :param log_path: path of the file to log to
    :return: Logger object
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path),
        ]
    )

    return logging.getLogger()


def create_parser() -> argparse.ArgumentParser:
    """ Creates and returns the argument parses for CLI

    :return: ArgumentParser object
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--source-path", type=str, required=True,
                        help="Source path from which to synchronize the replica")
    parser.add_argument("--replica-path", type=str, required=True,
                        help="Replica path to which to synchronize the source")
    parser.add_argument("--sync-interval", type=int, required=True,
                        help="Interval of synchronization between folders in seconds")
    parser.add_argument("--log-file", type=str, help="File to log actions", default="actions.log")

    return parser


def are_same_files(file1: str, file2: str) -> bool:
    """ Checks if 2 files are the same based on md5 checksum

    :param file1: Path to file to check
    :param file2: Path to file to check
    :return: Whether if the files are the same of not
    """
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        return hashlib.md5(f1.read()).hexdigest() == hashlib.md5(f2.read()).hexdigest()


def sync_folders(source: str, replica: str, logger: Logger):
    """ Synchronizes replica folder from source folder path

    :param source: source path
    :param replica: replica path
    :param logger: Logger object to be used
    :return:
    """
    if not os.path.isdir(source):
        raise ValueError(f"{source} is not a folder")

    if not os.path.isdir(replica):
        # also we can create it if it's not required to already exist
        raise ValueError(f"{replica} is not a folder")

    # adding files from sync to replica
    for cwd, folders, files in os.walk(source):
        for source_folder in folders:
            source_folder_path = os.path.join(cwd, source_folder)
            replica_folder_path = source_folder_path.replace(source, replica)

            if not os.path.isdir(replica_folder_path):
                logger.info(f"Creating folder {replica_folder_path}")
                os.makedirs(replica_folder_path)

        for source_file in files:
            source_file_path = os.path.join(cwd, source_file)
            replica_file_path = source_file_path.replace(source, replica)

            if not os.path.isfile(replica_file_path) or not are_same_files(source_file_path, replica_file_path):
                logger.info(f"Copying file {source_file_path} to {replica_file_path}")
                shutil.copy2(source_file_path, replica_file_path)

    # removing files from replica if they aren't anymore in source
    for cwd, folders, files in os.walk(replica):
        for replica_folder in folders:
            replica_folder_path = os.path.join(cwd, replica_folder)
            source_folder_path = replica_folder_path.replace(replica, source)

            if not os.path.isdir(source_folder_path):
                logger.info(f"Removing folder {replica_folder_path}")
                shutil.rmtree(replica_folder_path)

        for replica_file in files:
            replica_file_path = os.path.join(cwd, replica_file)
            source_file_path = replica_file_path.replace(replica, source)

            if not os.path.isfile(source_file_path):
                logger.info(f"Removing file {replica_file_path}")
                os.remove(replica_file_path)


def main():
    parser = create_parser()
    args = parser.parse_args()
    logger = configure_logger(args.log_file)

    while True:
        sync_folders(args.source_path, args.replica_path, logger)
        time.sleep(args.sync_interval)


if __name__ == "__main__":
    main()
