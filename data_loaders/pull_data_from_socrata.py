import io
import polars as pl
import requests
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed # Concurrency
from tqdm import tqdm # Progress bar
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def load_data_from_api(schema, 
                       total_n_rows, 
                       last_record_in_db, 
                       *args, **kwargs):
    """
    Pulls the data from the SODA data set endpoint and reads it into a Polars data frame concurrently.
    
    Returns:
        polars.DataFrame: a data frame with the content pulled from the Socrata API.    
    
    Args: 
        schema (dict): a dictionary that specifies the type (values) of the columns (keys).
        total_n_rows (int): total of rows in the API's data set. Used to decide if the database must be updated.
        last_record_in_db (int): number of rows in our DuckDB database. It is taken as a reference so the system knows where to continue pulling the data from the API.
    """
    DOMAIN = 'data.iowa.gov'
    DATASET_ID = 'm3tr-qhgy'
    BATCH_SIZE = 50000 # API batch size limit is limited to 50k rows by SODA
    offset = last_record_in_db
    
    # Added this to regulate the amount of data we will pull
    # Helpful for fast testing, and to avoid overloading the SODA server
    custom_update_size = 5000000 # Delete if you want to pull all the data in a single process. Not recomended

    print("SODA data pull started.")
    print(f" Total of rows in this data set: {total_n_rows}\n Total of records in our DuckDB database: {last_record_in_db}")

    # Records that we haven't pulled from the the SODA DB
    reccords_left = total_n_rows - last_record_in_db
    print(f"Records left to fetch from the API: {reccords_left}")
    print(f"Records that will be fetched in this job: {custom_update_size}")
    
    # To check for validity of the rows left
    if custom_update_size is not None:
        if custom_update_size < reccords_left:
            reccords_left = custom_update_size
    
    if reccords_left == 0:
        raise Exception("Your database is up to date.")
    if reccords_left < 0:
        raise Exception("Your data base should not contain more data than the source API. Check for issues.")

    # Calculate number of batches
    n_iterations = ceil(reccords_left / BATCH_SIZE)
    print(f"Number of batches: {n_iterations}")
    print(f"Number of rows that will be pulled in each batch: {BATCH_SIZE}")

    def fetch_batch(batch_offset):
        """Fetch a single batch of data."""
        try:
            #print(f"Fetching batch with offset={batch_offset}") # Commented to avoid excess of noise during execution, but useful for debugging.
            data_url = f"https://{DOMAIN}/resource/{DATASET_ID}.csv?$limit={BATCH_SIZE}&$offset={batch_offset}&$order=invoice_line_no"
            response = requests.get(data_url)
            response.raise_for_status()  # Raise an error for bad responses
            #print(f"Batch with offset={batch_offset} fetched successfully.") # Commented to avoid excess of noise during execution, but useful for debugging.
            return pl.read_csv(io.StringIO(response.text), schema=schema)
        except Exception as e:
            #print(f"Error fetching batch with offset={batch_offset}: {e}")
            return pl.DataFrame()  # Return an empty DataFrame on failure

    # Use ThreadPoolExecutor for concurrent API calls
    offsets = [offset + i * BATCH_SIZE for i in range(n_iterations)]

    # Add tqdm to track progress
    df_list = []
    with ThreadPoolExecutor(max_workers = 3) as executor:  # Be careful here
        # Submit tasks to the executor
        futures = {executor.submit(fetch_batch, offset): offset for offset in offsets}
        
        # Wrap the as_completed generator with tqdm for progress tracking
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching data batches"):
            offset = futures[future]
            try:
                data = future.result()
                df_list.append(data)
            except Exception as e:
                print(f"Batch for offset={offset} generated an exception: {e}")

    # Combine all fetched data
    all_pulled_data = pl.concat(df_list, how="vertical")

    return all_pulled_data


@test
def test_output(output, *args) -> None:
    """
    Validates the output of data pulling block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pl.DataFrame), "The output is not a a Polars data frame"
    assert len(output) > 0, "The data frame is empty"

@test
def test_invoice_line_no_not_null_output(output, *args) -> None:
    """
    Test the new invoice_line_no column contains no nulls.
    """
    assert output["invoice_line_no"].is_null().sum() == 0, "The invoice_line_no column contain null values, it shouldn't"

@test
def test_date_not_null_output(output, *args) -> None:
    """
    Test the new date column contains no nulls.
    """
    assert output["date"].is_null().sum() == 0, "The date column contain null values, it shouldn't"