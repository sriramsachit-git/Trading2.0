import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('convert_csv_to_parquet.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def convert_csv_to_parquet(input_csv, output_dir, partition_by=None):
    """
    Convert a CSV file to Parquet format, optionally partitioning by a column.
    
    Parameters:
    - input_csv: Path to input CSV file
    - output_dir: Directory to save Parquet files
    - partition_by: Column to partition by (e.g., 'ticker'), or None for no partitioning
    """
    logger.info(f"Starting conversion of {input_csv} to Parquet")
    
    try:
        # Read CSV in chunks to handle large file
        chunk_size = 100000  # Adjust based on memory
        chunks = pd.read_csv(input_csv, chunksize=chunk_size)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # If partitioning is enabled
        if partition_by:
            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}")
                # Ensure the partition column exists
                if partition_by not in chunk.columns:
                    logger.error(f"Partition column '{partition_by}' not found in CSV")
                    raise ValueError(f"Partition column '{partition_by}' not found")
                
                # Group by partition column and save each group
                for value, group in chunk.groupby(partition_by):
                    # Clean partition value for filesystem compatibility
                    safe_value = str(value).replace('/', '_')
                    partition_dir = os.path.join(output_dir, f"{partition_by}={safe_value}")
                    os.makedirs(partition_dir, exist_ok=True)
                    
                    # Convert to Parquet table
                    table = pa.Table.from_pandas(group)
                    parquet_path = os.path.join(partition_dir, f"part-{i}.parquet")
                    pq.write_table(table, parquet_path, compression='snappy')
                    logger.info(f"Saved partition {partition_by}={safe_value}, part {i} to {parquet_path}")
        else:
            # No partitioning, save as single Parquet file
            output_file = os.path.join(output_dir, 'data.parquet')
            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}")
                table = pa.Table.from_pandas(chunk)
                if i == 0:
                    # Write first chunk with overwrite mode
                    pq.write_table(table, output_file, compression='snappy')
                else:
                    # Append subsequent chunks
                    pq.write_table(table, output_file, compression='snappy', append=True)
                logger.info(f"Appended chunk {i+1} to {output_file}")
        
        logger.info("Conversion completed successfully")
    
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}")
        raise

if __name__ == "__main__":
    input_csv = "HS_3650_Minute_All.csv"  # Your 6 GB CSV file
    output_dir = "parquet_data"  # Directory to store Parquet files
    partition_by = "ticker"  # Partition by ticker for faster queries
    
    convert_csv_to_parquet(input_csv, output_dir, partition_by=partition_by)
    
    # Optional: Verify the Parquet data
    logger.info("Verifying Parquet data")
    try:
        parquet_data = pq.read_table(output_dir)
        df = parquet_data.to_pandas()
        logger.info(f"Parquet data head:\n{df.head()}")
        logger.info(f"Parquet data tail:\n{df.tail()}")
        logger.info(f"Total rows in Parquet: {len(df)}")
    except Exception as e:
        logger.error(f"Error reading Parquet data: {str(e)}")