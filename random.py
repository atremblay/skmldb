# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: Alexis Tremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-05-16 13:34:59
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-17 09:20:34
# @File Name: random.py


from utils import generate_random_name, _create_output_dataset
from procedures import Transform
from connection import conn

mldb = conn


def stratified_sample(dataset, col, weights, outputDataset=None):
    """
    Stratification is the process of dividing members of the population into
    homogeneous subgroups before sampling. The strata should be mutually
    exclusive: every element in the population must be assigned to only one
    stratum.

    Sampling each stratum can be done proportionnally or not. You could want
    to keep the same proportions between your strata or unbalance it.
    https://en.wikipedia.org/wiki/Stratified_sampling

    Paramters:
        dataset: string

            Name of the dataset to sample.

        col: string (default None)

            Name of the column containing the class

        weights: dict

            A dictionnary containing the class as key and the number of rows
            to sample as value.
            e.g. If you have class A and B and you want respectively 1000 and
            500, this parameter would be {'A': 1000, 'B':500}

        outputDataset: OutputDataset (default None)


    """
    if outputDataset is None:
        outputDataset = _create_output_dataset(generate_random_name())
    q = """
        SELECT *
        FROM sample(%(dataset)s, {rows: %(value)s, withReplacement: FALSE})
        WHERE %(col)s = '%(key)s'
        """ % {"dataset": dataset, "col": col}

    subselects = []
    for key, value in weights.items():
        subselects.append(q % {"key": key, "value": value})

    merge = "SELECT * FROM merge({})".format(",".join(subselects))

    mldb.connection.put(
        "/v1/procedures/stratifiedSample",
        Transform(
            inputData=merge,
            outputData=outputDataset
        )()
    )
