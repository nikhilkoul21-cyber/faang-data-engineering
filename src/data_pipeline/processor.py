import logging
from typing import Tuple
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s-%(lvelname)s-%(message)s",
    )
logger=logging.getLogger(__name__)


def read_csv(path: str) -> pd.DataFrame:
    """Reads a CSV file into a DataFrame."""
    try:
        df=pd.read_csv(path)
        logger.info("Read %d rows from %s",len(df),path)
        return df
    except Exception as exc:
        logger.error("Failed to read CSV: %s",exc)
        raise


REQUIRED_COLUMNS={
    "transaction_id",
    "user_id",
    "amount",
    "transaction_date",
}
"""schema validation"""
def validate_schema(df:pd.DataFrame) -> None:
    """ Valdate that required columns exist"""
    missing=REQUIRED_COLUMNS - set(df.columns)
    if missing:
        error_msg=f"Missing required column: {missin}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("Schema validation passed.")


def clean_data(df:pd.DataFrame) -> Tuple[pd.DataFrame,pd.DataFrame]:
    """Clean data by applyin business rules"""
    initial_count=len(df)

    invalid_mask=(
        df['transaction_id'].isna()|
        df['user_id'].isna()|
        (df['amount']<=0)
    )

    rejected_df=df[invalid_mask]
    cleaned_df=df[~invalid_mask]

    logger.info(
        'Dropped %d invalid rows out of %d', 
        len(rejected_df),
        initial_count,
        )
    
    return cleaned_df,rejected_df


"""Write dataframe to CSV"""
def write_csv(df:pd.DataFrame,path:str)->None:
    df.to_csv(path,index=False)
    logger.info("Written %d rows to %s",len(df),path)


if __name__=="__main__":
    input_path="data/input/transactions.csv"
    cleaned_path="data/output/cleaned_transactions.csv"
    rejected_path="data/output/rejected_transactions.csv"

    df=read_csv(input_path)
    validate_schema(df)

    cleaned_df,rejected_df=clean_data(df)
    write_csv(cleaned_df,cleaned_path)

    