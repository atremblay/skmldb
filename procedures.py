# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: Alexis Tremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-05-02 10:52:25
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-11 13:39:10
# @File Name: procedures.py

from pymldb import Connection
from utils import _create_output_dataset
import uuid
import json
mldb = Connection("http://localhost")


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


class Probabilizer(object):
    """
    The probabilizer plays the role of transforming the output of a classifier,
    which may have any range, into a calibrated pseudo-probability that can be
    used for value calculations.
    https://docs.mldb.ai/doc/#builtin/procedures/Probabilizer.md.html
    """
    def __init__(
            self,
            link="logit",
            name="Probabilizer"):
        """
        Paramters:
            link: string (default=logit)

                Link function to use. Available values:
                    logit: Good generic link for probabilistic
                    probit: Advanced usage
                    comp_log_log: Also good for probabilistic
                    linear: Makes it solve linear least squares (identity)
                    log: Good for transforming the output of boosting

            name: string (default=Probabilizer)

                Name of the function
        """
        if link not in ["logit", "probit", "comp_log_log", "linear", "log"]:
            raise ValueError("link function value not allowed. Check doc.")

        super(Probabilizer, self).__init__()
        self.link = link.upper()
        self.name = name

    def fit(self, dataset, X, y):
        """
        Parameters:
            dataset: string

                Dataset name to use for training

            X: array of strings

                An array of all the features to use in the decision trees

            y: string

                Name of the target to use. a.k.a. label
        """
        self.feature = X
        self.label = y

        trainingData = """
            SELECT
                %(feature)s as score,
                %(label)s as label
            FROM %(campaign)s
        """ % {
            "feature": self.feature,
            "label": y,
            "campaign": dataset
        }

        self.training_payload = {
            "type": "probabilizer.train",
            "params": {
                "trainingData": trainingData,
                "modelFileUrl": "file://donotcare.cls",
                "functionName": self.name,
                "runOnCreation": True,
                "link": self.link
            }
        }

        response = mldb.put(
            "/v1/procedures/" + self.name,
            self.training_payload)

        if response.status_code != 201:
            raise Exception("could not train probabilizer.\n{}".format(
                response.content))

    def predict(self, dataset):
        """
        Parameters:
            dataset: string

                Dataset name to use for testing
        """

        predict_set_name = "d" + str(uuid.uuid4().hex)
        self.predict_payload = Transform(
            inputData="""
                SELECT
                    %(func)s({%(feature)s as score}) as predict
                FROM %(test)s
            """ % {
                "func": self.name,
                "feature": self.feature,
                "test": dataset
            },
            outputDataset=predict_set_name
        )()
        response = mldb.put(
            "/v1/procedures/train_test_split",
            self.predict_payload
        )
        if response.status_code != 201:
            raise Exception("could not create dataset.\n{}".format(
                response.content))
        return predict_set_name

    def __repr__(self):
        return json.dumps(self.configuration, indent=4)
