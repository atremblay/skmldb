# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: Alexis Tremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-05-02 10:52:25
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-02 10:53:08
# @File Name: procedures.py

from utils import _create_output_dataset


class Transform(object):
    def __init__(self, inputData="", outputDataset="", runOnCreation=True):
        """Summary
        Create the basic payload for a transform procedure
        https://docs.mldb.ai/doc/#builtin/procedures/TransformDataset.md.html

        Args:
            inputData (str): A SQL statement to select the rows from
                a dataset to be transformed. This supports all MLDB's SQL
                expressions including but not limited to where, when, order by
                and group by clauses. These expressions can be used to refine
                the rows to transform.

            outputDataset (str): Output dataset configuration. This
                may refer either to an existing dataset, or a fully specified
                but non-existing dataset which will be created by the procedure.

            runOnCreation (bool): If true, the procedure will be run
                immediately. The response will contain an extra field called
                firstRun pointing to the URL of the run.
        """
        self.inputData = inputData
        self.outputDataset = outputDataset
        self.runOnCreation = runOnCreation

    def __call__(self):
        payload = {"type": "transform"}
        params = {
            "inputData": self.inputData,
            "outputDataset": _create_output_dataset(self.outputDataset),
            "runOnCreation": self.runOnCreation
        }
        payload['params'] = params
        return payload
