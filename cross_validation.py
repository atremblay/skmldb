# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: atremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-04-26 12:04:50
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-17 09:18:17
# @File Name: cross_validation.py

from procedures import Transform
import uuid
from connection import conn

mldb = conn


def train_test_split(
        dataset,
        test_size=None,
        train_size=None,
        test_name=None,
        train_name=None):

    """
    Function to randomly split a dataset in a train and test set. Names of both
    are None by default to make sure it does not accidentally overwrite an
    existing dataset. It is recommended that you provide names.

    Parameters
        dataset : string

            dataset name to split

        test_size : float, int, or None (default is None)

            If float, should be between 0.0 and 1.0 and represent the proportion
            of the dataset to include in the test split. If int, represents the
            absolute number of test samples. If None, the value is automatically
            set to the complement of the train size. If train size is also None,
            test size is set to 0.25.

        train_size : float, int, or None (default is None)

            If float, should be between 0.0 and 1.0 and represent the proportion
            of the dataset to include in the train split. If int, represents the
            absolute number of train samples. If None, the value is
            automatically set to the complement of the test size.

        test_name : string, (default is None)

            Name to give to the test set

        train_name : string, (default is None)

            Name to give to the train set


    Returns
        splitting : tuple, length = 2

            tuple containing names of datasets for train-test split.
    """

    if test_size is None and train_size is None:
        test_size = 0.25

    if test_size is not None and train_size is None:
        train_size = 1-test_size

    if test_size is None and train_size is not None:
        test_size = 1 - train_size

    if test_size + train_size > 1:
        msg = "ValueError: The sum of train_size and test_size = {}, should sum"
        msg += " to maximum 1. Reduce test_size and/or "
        msg += "train_size".format(test_size + train_size)
        raise ValueError(msg)

    if train_name is None:
        train_name = "d"+str(uuid.uuid4().hex)
    response = mldb.connection.put(
        "/v1/procedures/train_test_split",
        Transform(
            inputData="""
                SELECT * FROM {} WHERE rowHash()%100<{}
            """.format(dataset, int(train_size*100)),
            outputDataset=train_name
        )()
    )

    if response.status_code != 201:
        raise Exception("could not create dataset.\n{}".format(
            response.content))

    if test_name is None:
        test_name = "d"+str(uuid.uuid4().hex)
    response = mldb.connection.put(
        "/v1/procedures/train_test_split",
        Transform(
            inputData="""
                SELECT * FROM {} WHERE rowHash()%100>={}
            """.format(dataset, int(100-test_size*100)),
            outputDataset=test_name
        )()
    )

    if response.status_code != 201:
        raise Exception("could not create dataset.\n{}".format(
            response.content))

    # TODO possibly return a kind of Dataset object that you can call delete on
    return (train_name, test_name)
