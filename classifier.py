# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: atremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-04-27 15:46:19
# @Last Modified by:   atremblay
# @Last Modified time: 2016-05-02 10:12:57
# @File Name: classifier.py

from pymldb import Connection
import json
from utils import Transform, _create_output_dataset
import uuid
mldb = Connection("http://localhost")


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


def test(estimator, dataset, outputDataset=None):
    testingData = """
        SELECT
            %(func)s({features:{"%(features)s"}})[score] as score,
            %(label)s as label
        FROM %(campaign)s""" % {
            "func": estimator.name,
            "features": '","'.join(estimator.features),
            "label": estimator.label,
            "campaign": dataset
        }

    payload = {
        "type": "classifier.test",
        "params": {
            "testingData": testingData,
            "mode": estimator._mode,
            "runOnCreation": True
        }
    }
    if outputDataset is not None:
        payload["outputDataset"] = _create_output_dataset(outputDataset)

    response = mldb.put("/v1/procedures/"+estimator.name+"_test", payload)
    if response.status_code != 201:
        raise Exception(response.content)

    content = json.loads(response.content)
    bestMCC = BestMCC(content["status"]["firstRun"]["status"]["bestMcc"])
    bestF = BestF(content["status"]["firstRun"]["status"]["bestF"])
    auc = content["status"]["firstRun"]["status"]["auc"]

    return bestMCC, bestF, auc


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
    response = mldb.put("/v1/procedures/dt", {
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