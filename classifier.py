# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: atremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-04-27 15:46:19
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-17 09:24:28
# @File Name: classifier.py

import json
from utils import _create_output_dataset
from exception import ArgumentError, ProcedureError
from connection import conn

mldb = conn


class BestMCC(object):
    """docstring for BestMCC"""
    def __init__(self, arg):
        super(BestMCC, self).__init__()
        self.arg = arg
        self.recall = arg["pr"]["recall"]
        self.precision = arg["pr"]["precision"]
        self.f = arg["pr"]["f"]
        self.mcc = arg["mcc"]
        self.gain = arg["gain"]
        self.threshold = arg["threshold"]
        self.falseNegatives = arg["counts"]["falseNegatives"]
        self.truePositives = arg["counts"]["truePositives"]
        self.trueNegatives = arg["counts"]["trueNegatives"]
        self.falsePositives = arg["counts"]["falsePositives"]

    def __repr__(self):
        r = "BestMCC\n"
        r += "\trecall = {}\n".format(self.recall)
        r += "\tprecision = {}\n".format(self.precision)
        r += "\tf = {}\n".format(self.f)
        r += "\tmcc = {}\n".format(self.mcc)
        r += "\tgain = {}\n".format(self.gain)
        r += "\tthreshold = {}\n".format(self.threshold)
        r += "\tfalseNegatives = {}\n".format(self.falseNegatives)
        r += "\ttruePositives = {}\n".format(self.truePositives)
        r += "\ttrueNegatives = {}\n".format(self.trueNegatives)
        r += "\tfalsePositives = {}\n".format(self.falsePositives)
        return r


class BestF(object):
    """docstring for BestF"""
    def __init__(self, arg):
        super(BestF, self).__init__()
        self.arg = arg
        self.recall = arg["pr"]["recall"]
        self.precision = arg["pr"]["precision"]
        self.f = arg["pr"]["f"]
        self.mcc = arg["mcc"]
        self.gain = arg["gain"]
        self.threshold = arg["threshold"]
        self.falseNegatives = arg["counts"]["falseNegatives"]
        self.truePositives = arg["counts"]["truePositives"]
        self.trueNegatives = arg["counts"]["trueNegatives"]
        self.falsePositives = arg["counts"]["falsePositives"]

    def __repr__(self):
        r = "BestF\n"
        r += "\trecall = {}\n".format(self.recall)
        r += "\tprecision = {}\n".format(self.precision)
        r += "\tf = {}\n".format(self.f)
        r += "\tmcc = {}\n".format(self.mcc)
        r += "\tgain = {}\n".format(self.gain)
        r += "\tthreshold = {}\n".format(self.threshold)
        r += "\tfalseNegatives = {}\n".format(self.falseNegatives)
        r += "\ttruePositives = {}\n".format(self.truePositives)
        r += "\ttrueNegatives = {}\n".format(self.trueNegatives)
        r += "\tfalsePositives = {}\n".format(self.falsePositives)
        return r


def Test(dataset, estimator=None, score=None, label=None, outputDataset=None):

    if estimator is not None:
        testingData = """
            SELECT
                %(func)s({features:{"%(features)s"}})[score] as score,
                %(label)s as label
            FROM %(dataset)s""" % {
                "func": estimator.name,
                "features": '","'.join(estimator.features),
                "label": estimator.label,
                "dataset": dataset
            }
        proc_name = estimator.name
    elif score is not None:
        if label is None:
            msg = "If estimator is not provided, both score and label must \
            be provided"
            raise ArgumentError(msg)

        testingData = """
            SELECT
                %(score)s as score,
                %(label)s as label
            FROM %(dataset)s""" % {
                "score": score,
                "label": label,
                "dataset": dataset
            }
        proc_name = dataset

    params = {
        "testingData": testingData,
        "runOnCreation": True
    }
    if estimator is not None:
        params["mode"] = estimator._mode

    payload = {
        "type": "classifier.test",
        "params": params
    }

    if outputDataset is not None:
        payload["outputDataset"] = _create_output_dataset(outputDataset)

    response = mldb.connection.put(
        "/v1/procedures/" + proc_name + "_test",
        payload
        )
    if response.status_code != 201:
        raise ProcedureError(response.content)

    content = json.loads(response.content)
    bestMCC = BestMCC(content["status"]["firstRun"]["status"]["bestMcc"])
    bestF = BestF(content["status"]["firstRun"]["status"]["bestF"])
    auc = content["status"]["firstRun"]["status"]["auc"]

    return (bestMCC, bestF, auc)


def experiment(estimator, dataset, X, y, kfold=0, name=None):
    if name is None:
        name = estimator.name + "_xp"
    trainingData = """
        SELECT
            {%(features)s} as features,
            %(label)s as label
        FROM %(campaign)s""" % {
            "features": ",".join(X),
            "label": y,
            "campaign": dataset
        }
    response = mldb.connection.put("/v1/procedures/dt", {
        "type": "classifier.experiment",
        "params": {
            "experimentName": name,
            "trainingData": trainingData,
            "kfold": kfold,
            "modelFileUrlPattern": """file://dt_donotcare.cls""",
            "algorithm": estimator.configuration.keys()[0],
            "configuration": estimator.configuration,
            "mode": estimator._mode,
            "outputAccuracyDataset": True,
            "runOnCreation": True
        }
    })

    return response