# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: Alexis Tremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-05-11 09:30:03
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-11 09:37:51
# @File Name: linear_model.py

from pymldb import Connection
import json
from utils import Transform
import uuid
mldb = Connection("http://localhost")


class LogisticRegression(object):
    """Logistic Regression (aka logit, MaxEnt) classifier."""
    def __init__(
            self,
            fit_intercept=True,
            ridge_regression=True,
            feature_proportion=1.0,
            name="LogisticRegression"):
        """
        Paramters:
            fit_intercept: boolean (default=True)

                Specifies if a constant (a.k.a. bias or intercept) should be
                added to the decision function.

            ridge_regression: boolean (default=True)

                Use a regularized regression with smaller coefficients

            feature_proportion: float in the range 0-1 (default=1.0)

                Use only a (random) portion of available features when training
                classifier
        """
        if feature_proportion < 0 or feature_proportion > 1:
            raise ValueError("feature_proportion must be between 0 and 1")

        super(LogisticRegression, self).__init__()
        self.fit_intercept = fit_intercept
        self.ridge_regression = ridge_regression
        self.name = name
        self.feature_proportion = feature_proportion
        self._mode = "boolean"
        self.configuration = {
            "logisticRegression": {
                "_note": """
                    Logistic Regression.
                    Very smooth but needs very good features""",
                "type": "glz",
                "verbosity": 3,
                "feature_proportion": feature_proportion,
                "add_bias": self.fit_intercept,
                "ridge_regression": self.ridge_regression
            }
        }

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
        self.features = X
        self.label = y

        trainingData = """
            SELECT
                {%(features)s} as features,
                %(label)s as label
            FROM %(campaign)s""" % {
                "features": ",".join(X),
                "label": y,
                "campaign": dataset
            }

        self.training_payload = {
            "type": "classifier.train",
            "params": {
                "trainingData": trainingData,
                "algorithm": "logisticRegression",
                "configuration": self.configuration,
                "mode": self._mode,
                "modelFileUrl": "file://donotcare.cls",
                "functionName": self.name,
                "runOnCreation": True
            }
        }

        response = mldb.put(
            "/v1/procedures/"+self.name,
            self.training_payload)

        if response.status_code != 201:
            raise Exception("could not train random forest.\n{}".format(
                response.content))

    def predict(self, dataset):
        """
        Predict class for X.

        The predicted class of an input sample is a vote by the trees in the
        forest, weighted by their probability estimates. That is, the predicted
        class is the one with highest mean probability estimate across the
        trees.

        Parameters:
            dataset: string

                Dataset name to use for testing
        """

        predict_set_name = "d"+str(uuid.uuid4().hex)
        self.predict_payload = Transform(
            inputData="""
                SELECT
                    %(func)s({{%(features)s} as features}) as predict
                FROM %(test)s
            """ % {
                "func": self.name,
                "features": ",".join(self.features),
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
