#!/usr/bin/env python3
import logging
from io import StringIO
from typing import List
import pandas as pd
from dotenv import load_dotenv
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import get_database_handle

load_dotenv()
logger = logging.getLogger(__name__)

def transformer(inputs: dict):
    return retval

def get_details(**config):
    sources = [
        [
            SourceDetails(
                'cryptolist',
                'json:json',
                'cryptolist.json'
            )
        ]

    exports = [
        ExportDetails(
            'report',
            "csv:csv",
            'report.csv'
        ) 
    ]

    return PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        transformer=transformer
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    pipeline_details = get_details()
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()
