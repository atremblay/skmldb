# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: atremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-04-27 10:57:58
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-17 09:26:33
# @File Name: tree.py

import json
from procedures import Transform
from utils import generate_random_name
from connection import conn

mldb = conn


class DecisionTreeClassifier(object):
    """docstring for DecisionTreeClassifier"""
    def __init__(
            self,
            max_depth=-1,
            random_feature_propn=1,
            update_alg="prob",
            name="DecisionTreeClassifier"):

        super(DecisionTreeClassifier, self).__init__()
        self.max_depth = max_depth
        self.random_feature_propn = random_feature_propn
        self.name = name
        self.update_alg = update_alg
        self._mode = "boolean"
        self.configuration = {
            "dt": {
                "_note": "Plain decision tree",

                "type": "decision_tree",
                "max_depth": max_depth,
                "verbosity": 3,
                "update_alg": update_alg,
                "random_feature_propn": random_feature_propn
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
                "algorithm": "dt",
                "configuration": self.configuration,
                "mode": self._mode,
                "modelFileUrl": "file://donotcare.cls",
                "functionName": self.name,
                "runOnCreation": True
            }
        }

        response = mldb.connection.put(
            "/v1/procedures/"+self.name,
            self.training_payload)

        if response.status_code != 201:
            raise Exception("could not train random forest.\n{}".format(
                response.content))

    def predict(self, dataset, predict_set_name=None):
        """
        Predict class for X.

        The predicted class of an input sample is a vote by the trees in the
        forest, weighted by their probability estimates. That is, the predicted
        class is the one with highest mean probability estimate across the
        trees.

        Parameters:
            dataset: string

                Dataset name to use for testing

            predict_set_name: string (default None)

                Name to give to the dataset containing the predictions. If None,
                a randomly generated name will be given
        """
        if predict_set_name is None:
            predict_set_name = generate_random_name()
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
        response = mldb.connection.put(
                "/v1/procedures/train_test_split",
                self.predict_payload
            )
        if response.status_code != 201:
            raise Exception("could not create dataset.\n{}".format(
                response.content))
        return predict_set_name

    def __repr__(self):
        return json.dumps(self.configuration, indent=4)
