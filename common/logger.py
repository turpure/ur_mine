#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-01-02 11:06
# Author: turpure

import logging


def log(name, path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    fh = logging.FileHandler(path)
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    return logger


logger = log('Joom-crawler', 'fetching.log')



