#!/usr/bin/env python3
import logging
import os
import pandas as pd
from dotenv import load_dotenv
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from balances import get_evm_balance, get_tron_balance
from balances import get_evm_token_balance, get_trc20_token_balance
from balances import get_btc_balance

load_dotenv()
logger = logging.getLogger(__name__)
PIPELINE_NAME = 'Virtual Asset report'
ETHERSCAN_KEY = os.environ.get('ETHERSCAN_KEY')
BSCSCAN_KEY = os.environ.get('BSCSCAN_KEY')
CONTRACTS = {
    'eth': {
        'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    },
    'bnb': {
        'USDT': '0x55d398326f99059fF775485246999027B3197955'
    },
    'trx': {
        'USDT': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'
    }
}

def get_value(row):
    wallet = row['wallet']
    chain = row['blockchain']
    if chain == 'btc':
        object = get_btc_balance(wallet)
        print(object)
    elif chain == 'eth':
        object = get_evm_balance(
            wallet, 'api.etherscan.io', 'ethereum', ETHERSCAN_KEY
        )
        print(object)
        for token, address in CONTRACTS[chain].items():
            object = get_evm_token_balance(
                wallet,
                'api.etherscan.io',
                'ethereum',
                address,
                ETHERSCAN_KEY
            )
            print(object)
    elif chain == 'bnb':
        object = get_evm_balance(
            wallet, 'api.bscscan.com', 'binancecoin', BSCSCAN_KEY
        )
        print(object)
        for token, address in CONTRACTS[chain].items():
            object = get_evm_token_balance(
                wallet,
                'api.bscscan.com',
                'binance-smart-chain',
                address,
                BSCSCAN_KEY
            )
            print(object)
    elif chain == 'trx':
        object = get_tron_balance(wallet)
        print(object)
        for token, address in CONTRACTS[chain].items():
            object = get_trc20_token_balance(wallet, address)
            print(object)
    return None

def transformer(inputs: dict):
    outputs = pd.DataFrame()
    inputs['wallets'].apply(get_value, axis=1)
    return outputs

def get_details(**config):
    sources = [
        SourceDetails(
            'wallets',
            'csv',
            'wallets.csv'
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
