"""
"""
import pandas as pd


def convert(csv_file, odb_file, comment='#', nrows=100000):
    import pyodc
    try:
        with open(odb_file, 'wb') as f:
            for chunk in pd.read_csv(csv_file, comment=comment, chunksize=nrows):
                chunk.reset_index(drop=True, inplace=True)
                pyodc.encoder.encode_odb(chunk, f)
        return True
    except:
        return False

