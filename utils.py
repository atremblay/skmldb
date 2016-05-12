# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: atremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-04-08 13:55:22
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-12 09:36:11
# @File Name: utils.py

from pymldb import Connection
import os
import json
import uuid
import tempfile

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


class ImportText(object):

    def __init__(
            self,
            dataFileUrl="",
            outputDataset="",
            headers=None,
            quotechar=None,
            delimiter=None,
            limit=None,
            offset=None,
            encoding=None,
            ignoreBadLines=None,
            replaceInvalidCharactersWith=None,
            select=None,
            where=None,
            named=None,
            timestamp=None,
            runOnCreation=None):
        """Summary
        Create the basic payload for a import.text procedure
        https://docs.mldb.ai/doc/#builtin/procedures/importtextprocedure.md.html

        Args:
            dataFileUrl (Url): URL of the text data to import
            outputDataset (OutputDatasetSpec ): Dataset to record the data into
            headers ([ str ]): List of headers for when first row
                doesn't contain headers
            quotechar (str): Character to enclose strings
            delimiter (str): Delimiter for column separation
            limit (int): Maximum number of lines to process. Bad lines including
                empty lines contribute to the limit. As a result, it is possible
                for the dataset to contain less rows that the requested limit.
            offset (int): Skip the first n lines
                (excluding the header if present).
            encoding (str): Character encoding of file: 'us-ascii', 'ascii',
                'latin1', 'iso8859-1', 'utf8' or 'utf-8'
            ignoreBadLines (bool): If true, any line causing a parsing error
                will be skipped. Empty lines are considered bad lines.
            replaceInvalidCharactersWith (bool): If this is set, it should be a
                single Unicode character will be used to replace badly-encoded
                    characters in the input. The default is nothing, which will
                        cause lines with badly-encoded characters to throw an
                        error.
            select (SqlValueExpression ): Which columns to use.
            where (SqlValueExpression ): Which lines to use to create rows.
            named (SqlValueExpression ): Row name expression for output dataset.
                Note that each row must have a unique name.
            timestamp (None): Expression for row timestamp.
            runOnCreation (bool): If true, the procedure will be run
                immediately. The response will contain an extra field called
                firstRun pointing to the URL of the run.
        """
        super(ImportText, self).__init__()
        self.dataFileUrl = dataFileUrl
        self.outputDataset = outputDataset
        self.headers = headers
        self.quotechar = quotechar
        self.delimiter = delimiter
        self.limit = limit
        self.offset = offset
        self.encoding = encoding
        self.ignoreBadLines = ignoreBadLines
        self.replaceInvalidCharactersWith = replaceInvalidCharactersWith
        self.select = select
        self.where = where
        self.named = named
        self.timestamp = timestamp
        self.runOnCreation = runOnCreation

    def __call__(self):
        payload = {"type": "import.text"}
        params = {
            "dataFileUrl": self.dataFileUrl,
            "outputDataset": self.outputDataset
        }
        if self.headers is not None:
            params["headers"] = self.headers
        if self.quotechar is not None:
            params["quotechar"] = self.quotechar
        if self.delimiter is not None:
            params["delimiter"] = self.delimiter
        if self.limit is not None:
            params["limit"] = self.limit
        if self.offset is not None:
            params["offset"] = self.offset
        if self.encoding is not None:
            params["encoding"] = self.encoding
        if self.ignoreBadLines is not None:
            params["ignoreBadLines"] = self.ignoreBadLines
        if self.replaceInvalidCharactersWith is not None:
            params["replaceInvalidCharactersWith"] = self.replaceInvalidCharactersWith
        if self.select is not None:
            params["select"] = self.select
        if self.where is not None:
            params["where"] = self.where
        if self.named is not None:
            params["named"] = self.named
        if self.timestamp is not None:
            params["timestamp"] = self.timestamp
        if self.runOnCreation is not None:
            params["runOnCreation"] = self.runOnCreation
        payload['params'] = params
        return payload


class ExportCSV(object):
    """docstring for ExportCSV"""
    def __init__(
            self,
            exportData,
            dataFileUrl,
            headers=None,
            delimiter=None,
            quoteChar=None,
            runOnCreation=None):
        """Summary
        Create the basic payload for a export.csv procedure
        https://docs.mldb.ai/doc/#builtin/procedures/CsvExportProcedure.md.html

        Args:
            exportData (InputQuery): An SQL query to select the data to be
                exported. This could be any query on an existing dataset.
            dataFileUrl (Url): URL where the csv file should be written to. If
                a file already exists, it will be overwritten.
            headers (bool): Whether to print headers
            delimiter (str): The delimiter to place between each value
            quoteChar (str): The character to enclose the values within when
                they contain either a delimiter or a quoteChar
            runOnCreation (bool): If true, the procedure will be run
                immediately. The response will contain an extra field called
                firstRun pointing to the URL of the run.
        """
        super(ExportCSV, self).__init__()
        self.exportData = exportData
        self.dataFileUrl = dataFileUrl
        self.headers = headers
        self.delimiter = delimiter
        self.quoteChar = quoteChar
        self.runOnCreation = runOnCreation

    def __call__(self):
        payload = {"type": "export.csv"}
        params = {
            "exportData": self.exportData,
            "dataFileUrl": self.dataFileUrl
        }

        if self.headers is not None:
            params["headers"] = self.headers
        if self.delimiter is not None:
            params["delimiter"] = self.delimiter
        if self.quoteChar is not None:
            params["quoteChar"] = self.quoteChar
        if self.runOnCreation is not None:
            params["runOnCreation"] = self.runOnCreation

        payload['params'] = params
        return payload


class OutputDataset(object):
    """docstring for OutputDataset"""
    def __init__(self, id, type=None, **params):
        super(OutputDataset, self).__init__()
        self.datasetID = id
        self.datasetType = type
        self.params = params

    def __call__(self):
        payload = {"id": self.datasetID}
        if self.datasetType is not None:
            payload["type"] = self.datasetType
        # Force the params unknownColumns if tabular. Don't know why but
        # this is kinda required
        if self.datasetType == "tabular":
            if self.params is None:
                self.params = {"unknownColumns": "add"}
            else:
                self.params["unknownColumns"] = "add"

        if self.params is not None:
            payload["params"] = self.params

        return payload


class MLDBrtbopt(object):
    """docstring for MLDBrtbopt"""
    def __init__(
            self,
            host="http://localhost",
            s3LogHost="http://rtbindexer.ops.datacratic.com:17000"):
        super(MLDBrtbopt, self).__init__()
        self.host = host
        self.s3LogHost = s3LogHost
        self.mldb = Connection(host)

    def mw_import(
            self,
            slug,
            start=None,
            end=None,
            columnNames=None,
            outputDataset=None,
            runOnCreation=True,
            threads=16,
            print_config=False):

        params = {
            "slug": slug,
            "s3LogHost": self.s3LogHost,
            "outputDataset": _create_output_dataset(outputDataset, slug),
            "runOnCreation": runOnCreation,
            "threads": threads
        }

        if columnNames is not None:
            params['columnNames'] = columnNames
        if start is not None:
            params['from'] = start
        if end is not None:
            params['to'] = end

        payload = {
            'type': 'rtbopt.import',
            'params': params
        }
        if print_config:
            print(json.dumps(payload, indent=4))

        return self.mldb.put("/v1/procedures/rtbimport", payload)

    def indexer(
            self,
            slug,
            outputDataset=None,
            runOnCreation=True,
            print_config=False):

        params = {
            "slug": slug,
            "indexerHost": self.s3LogHost,
            "outputDataset": _create_output_dataset(outputDataset, slug),
            "runOnCreation": runOnCreation
        }

        payload = {
            'type': 'rtbopt.indexer',
            'params': params
        }

        if print_config:
            print(json.dumps(payload, indent=4))

        return self.mldb.put("/v1/procedures/indexerdata", payload)


def import_dataset(dataset, sep=';'):
    try:
        mldb.delete("/v1/datasets/"+dataset)
        path = "file://" + os.path.join(os.getcwd(), "{}.csv".format(dataset))
        response = mldb.put("/v1/procedures/import", {
            "type": "import.text",
            "params": {
                "dataFileUrl": path,
                "outputDataset": dataset,
                "runOnCreation": True,
                "ignoreBadLines": True,
                "delimiter": sep,
                "named": "rowName"
            }
        })

        # mldb.put(
        #     '/v1/procedures/donotcare',
        #     {
        #         "type": "transform",
        #         "params": {
        #             "inputData": """
        #                 SELECT * EXCLUDING (_rowName) NAMED _rowName
        #                 FROM _{}
        #             """.format(dataset),
        #             "outputDataset": dataset,
        #             "runOnCreation": True
        #         }
        #     })
        # mldb.delete("/v1/datasets/_"+dataset)

        return response.status_code == 201
    except Exception as e:
        print("Could not import {}\n{}".format(dataset, e))
        return False


def export_dataset(dataset, sep=";", columns=None):

    try:
        # path = "file://" + os.path.join(os.getcwd(), "{}.csv".format(dataset))
        df = mldb.query("SELECT * FROM {}".format(dataset))
        df.to_csv(
            os.path.join(os.getcwd(), "{}.csv".format(dataset)),
            encoding='utf-8',
            sep=sep,
            columns=columns
        )
#         mldb.put("/v1/procedures/export", {
#             "type": "export.csv",
#             "params": {
#                 "exportData": "SELECT rowName() AS __index, * FROM {}".format(dataset),
#                 "dataFileUrl": path,
#                 "headers": True,
#                 "runOnCreation": True,
#                 "delimiter": sep
#             }
#         })
    except:
        try:
            # Just making sure we are not keeping half a file
            os.remove(os.path.join(os.getcwd(), "{}.csv".format(dataset)))
            raise
        except:
            pass


class Dataset(object):
    """
    This class provides wrapper function to manage csv files.
    """
    def __init__(
            self,
            dataset,
            compression=None):

        super(Dataset, self).__init__()
        self.dataset = dataset
        self.dir = ""
        # self._file_path = os.path.join(self.dir, "{}".format(self.dataset))
        self._compression = compression

    @property
    def file_path(self):
        file_path = os.path.join(self.dir, "{}".format(self.dataset))
        return file_path + "." + self.extension

    @property
    def extension(self):
        if self._compression == "gz":
            return "gzip"
        elif self._compression is None:
            return "csv"
        return self._compression

    def exists_on_disk(self):
        return os.path.exists(self.file_path)

    def to_csv(
            self,
            sep=";",
            columns=None,
            index=True,
            index_label="rowName",
            directory=None):
        if directory is None:
            directory = os.getcwd()
        self.dir = directory
        """Summary

        Args:
            sep (str): Delimiter for column separation
            columns ([str]): Columns to write
            index (bool): Write row names (index)
            index_label (str): Name given to the index column
        """

        # response = mldb.get("/v1/datasets/" + self.dataset)
        # content = json.loads(response.content)
        # row_count = content['status']['rowCount']
        # column_count = content['status']['columnCount']

        # block_size = 10000000.  # Max number of cells to ask for
        # lines_per_block = int(block_size / column_count)
        # pages = 1 + int(row_count / float(lines_per_block))

        if columns is None:
            cols = "*"
        else:
            cols = ",".join(columns)
        try:
            query = """
                    SELECT {}
                    FROM {}
                    """.format(cols, self.dataset)

            df = mldb.query(query)
            # for page in range(pages):
            #     # print("{} of {}".format(page+1, pages))
            #     offset = lines_per_block * page
            #     limit = lines_per_block * (page+1)
            #     query = """
            #         SELECT {}
            #         FROM {}
            #         LIMIT {}
            #         OFFSET {}
            #     """.format(cols, self.dataset, limit, offset)
            #     dfs.append(mldb.query(query))

            # df = pd.concat(dfs)
            df.to_csv(
                self.file_path,
                encoding='utf-8',
                sep=sep,
                index=index,
                index_label=index_label
            )
            # mldb.put("/v1/procedures/export", {
            #     "type": "export.csv",
            #     "params": {
            #         "exportData": "SELECT rowName() AS __index, * FROM {}".format(dataset),
            #         "dataFileUrl": path,
            #         "headers": True,
            #         "runOnCreation": True,
            #         "delimiter": sep
            #     }
            # })
        except:
            # Just making sure we are not keeping half a file
            count = 0
            while count < 3 and not self.rollback():
                count += 1
            if count == 3:
                print("Could not delete {}".format(self.path))
            raise

    def from_csv(
            self,
            sep=';',
            directory=None,
            outputDataset=None,
            index_label="rowName",
            print_config=False):
        """Summary
        This will load from a csv with the same name as the provided dataset
        name in the initialization of this object.

        Args:
            sep (str): Delimiter for column separation
            directory (None): By default it will look in the current directory.
                If you wish to look somewhere else, put the relative path to
                where the file is. This will concatenate this relative path
                with the assumed csv file <dataset>.csv
            outputDataset (None): In case you would like to provide another
                name to the imported dataset. You can also provide a full
                OutputDatasetSpec
            index_label (str): Column to use as index. If None, will not use any
                column.

        Returns:
            TYPE: Description
        """

        if directory is None:
            directory = os.getcwd()
        self.dir = directory

        mldb.delete("/v1/datasets/"+self.dataset)
        path = "file://" + self.file_path

        payload = {"type": "import.text"}
        params = {
            "dataFileUrl": path,
            "outputDataset": _create_output_dataset(
                outputDataset, self.dataset),
            "runOnCreation": True,
            "ignoreBadLines": True,
            "delimiter": sep
            }
        if index_label is not None:
            params["select"] = "* EXCLUDING ({})".format(index_label)
            params["named"] = "cast({} as string)".format(index_label)
        payload["params"] = params

        if print_config:
            print(json.dumps(payload, indent=4))

        return mldb.put("/v1/procedures/import", payload)

        # mldb.put(
        #     '/v1/procedures/donotcare',
        #     {
        #         "type": "transform",
        #         "params": {
        #             "inputData": """
        #                 SELECT * EXCLUDING (_rowName) NAMED _rowName
        #                 FROM _{}
        #             """.format(dataset),
        #             "outputDataset": dataset,
        #             "runOnCreation": True
        #         }
        #     })
        # mldb.delete("/v1/datasets/_"+dataset)

    def rollback(self):
        try:
            os.remove(self.file_path)
            return not os.path.exists(self.file_path)
        except (OSError):
            return True


def _create_output_dataset(outputDataset, dataset_name=None):
    if isinstance(outputDataset, OutputDataset):
        return outputDataset()
    elif outputDataset is None:
        return OutputDataset(dataset_name, "tabular")()
    elif isinstance(outputDataset, str):
        return OutputDataset(outputDataset, "tabular")()
    elif isinstance(outputDataset, dict):
        return outputDataset


def stratified_sample(table, col, weights, outputDataset=None):
    q = """
        SELECT *
        FROM sample(%(table)s, {rows: %(value)s, withReplacement: FALSE})
        WHERE %(col)s = '%(key)s'
        """ % {"table": table, "col": col}

    subselects = []
    for key, value in weights.items():
        subselects.append(q % {"key": key, "value": value})

    merge = "SELECT * FROM merge({})".format(",".join(subselects))

    mldb.put(
        "/v1/procedures/stratifiedSample",
        Transform(
            inputData=merge,
            outputData=_create_output_dataset(
                outputDataset, table+"_stratified")))


def dataset_from_dataframe(df, name=None, index_name=None):
    """
    Paramters:
        df: DataFrame

            DataFrame to import in MLDB

        name: string (default None)

            Name to give to the dataset that will contain the DataFrame. If
            None, a random name will be generated

        index_name: string (default None)

            Should the name of the index on the DataFrame be None, this value
            will be used on import. If also None, the index will not be
            imported. This means that if the index of the DataFrame has a name,
            this parameter will be ignored.
    """
    if name is None:
        name = "d" + str(uuid.uuid4().hex)


    tmp_file = tempfile.NamedTemporaryFile(dir=".")
    params = {
        "ignoreBadLines": True,
        "runOnCreation": True,
        "delimiter": ";",
        "outputDataset": {
            "params": {
                "unknownColumns": "add"
            },
            "type": "tabular",
            "id": name
        },
        "dataFileUrl": "file://{}".format(tmp_file.name)
    }

    if df.index.name is not None:
        df.to_csv(tmp_file, sep=";", index=True)
        params['named'] = df.index.name
        params["select"] = "* EXCLUDING ({})".format(df.index.name)
    elif index_name is not None:
        df.index.name = index_name
        df.to_csv(tmp_file, sep=";", index=True)
        params['named'] = index_name
        params["select"] = "* EXCLUDING ({})".format(index_name)
    else:
        df.to_csv(tmp_file, sep=";", index=False)
    tmp_file.file.flush()

    payload = {
        "params": params,
        "type": "import.text"
    }
    response = mldb.put("/v1/procedures/import", payload)
    tmp_file.close()
    if response.status_code != 201:
        raise Exception("Could not create dataset")

    return name

