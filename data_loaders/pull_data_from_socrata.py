import io
import polars as pl
import requests
from math import ceil
from concurrent.futures import ThreadPoolExecutor
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
    Pulls the data from the data set endpoint and reads it into a Polars data frame concurrently.
    
    Returns:
        polars.DataFrame: a data frame with the content pulled from the Socrata API.    
    
    Args: 
        schema (dict): a dictionary that specifies the type (values) of the columns (keys).
        total_n_rows (int): total of rows in the API's data set. Used to decide if the database must be updated.
        last_record_in_db (int): number of rows in our DuckDB database. It is taken as a reference so the system knows where to continue pulling the data from the API.
    """
    DOMAIN = 'data.iowa.gov'  # Replace with kwargs.get('DOMAIN') if needed
    DATASET_ID = 'm3tr-qhgy'  # Replace with kwargs.get('DATASET_ID') if needed
    BATCH_SIZE = 100
    offset = last_record_in_db
    custom_update_size = 1000

    print(f"Starting data loader with total_n_rows={total_n_rows} and last_record_in_db={last_record_in_db}")

    # Check for validity of the rows left
    reccords_left = total_n_rows - last_record_in_db
    print(f"Records left to fetch from the API: {reccords_left}")
    print(f"Records that will be fetched in this job: {custom_update_size}")

    if custom_update_size is not None:
        if custom_update_size < reccords_left:
            reccords_left = custom_update_size
    
    if reccords_left == 0:
        raise Exception("Your database is up to date.")
    if reccords_left < 0:
        raise Exception("Your data base should not contain more data than the source API. Check for issues.")

    # Calculate number of iterations (batches)
    n_iterations = ceil(reccords_left / BATCH_SIZE)
    print(f"Number of iterations (batches): {n_iterations}")

    def fetch_batch(batch_offset):
        """Fetch a single batch of data."""
        try:
            #print(f"Fetching batch with offset={batch_offset}")
            data_url = f"https://{DOMAIN}/resource/{DATASET_ID}.csv?$limit={BATCH_SIZE}&$offset={batch_offset}&$order=invoice_line_no"
            response = requests.get(data_url)
            response.raise_for_status()  # Raise an error for bad responses
            #print(f"Batch with offset={batch_offset} fetched successfully.")
            return pl.read_csv(io.StringIO(response.text), schema=schema)
        except Exception as e:
            print(f"Error fetching batch with offset={batch_offset}: {e}")
            return pl.DataFrame()  # Return an empty DataFrame on failure

    # Use ThreadPoolExecutor for concurrent API calls
    offsets = [offset + i * BATCH_SIZE for i in range(n_iterations)]

    with ThreadPoolExecutor(max_workers=20) as executor:  # Be careful here
        df_list = list(executor.map(fetch_batch, offsets))

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