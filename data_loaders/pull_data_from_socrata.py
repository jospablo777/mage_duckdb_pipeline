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
                       last_year_in_local_db, 
                       records_per_year, 
                       *args, **kwargs):
    """
    Pulls the data from the SODA data set endpoint and reads it into a Polars data frame concurrently.
    
    Returns:
        polars.DataFrame: a data frame with the content pulled from the Socrata API.    
    
    Args: 
        schema (dict): a dictionary that specifies the type (values) of the columns (keys).
        last_year_in_local_db (int): year of the newest record in our local DB. Used to decide if the database must be updated.
        records_per_year (pl.DataFrame): number of records available for each year in the SODA DB. It is taken as a reference to set the limit for the records to retrieve according each year.
    """

    DOMAIN     = 'data.iowa.gov'
    DATASET_ID = 'm3tr-qhgy'
    base_url   = f"https://{DOMAIN}/resource/{DATASET_ID}.csv?"
    query_url  = "$where=date_extract_y(date)={}&$limit={}"
    records_per_year = records_per_year.with_columns(
    pl.format(base_url + query_url, pl.col("year"), pl.col("rows")).alias("url")
    )

    # If last year in our DuckDB is the last year in SODA DB, fetch only that year
    records_per_year = records_per_year.filter(pl.col("year") >= last_year_in_local_db)

    # Fetch only five years. Trims out the latest year since it is already in our DB
    # It doesn't trim the first year if it is the first time we will load into our local DB
    # The first load into the local DB is symbolized with last_year_in_local_db == 0
    if (last_year_in_local_db != records_per_year["year"].max()) & last_year_in_local_db != 0:
        # We're limiting to five years per job so our machine dont explode
        records_per_year = records_per_year.sort("year").head(6)
        records_per_year = records_per_year.filter(pl.col("year") != last_year_in_local_db) # Excludes last year to avoid a redundant fetch
    
    # We're limiting to five years per job so our machine dont explode
    records_per_year = records_per_year.sort("year").head(5)

    # Years we will request to the API
    requests_list = records_per_year["url"].to_list()

    print("SODA data pull started.")

    # Report to the user which years we will be working with
    years_to_fetch = records_per_year["year"].to_list()
    print("Years to be fetched: {}.".format(", ".join(map(str, years_to_fetch))))
    
    def fetch_batch(data_url):
        """Fetch data for a given URL."""
        try:
            #print(f"Fetching data from: {data_url}") # Useful to debug, but too vebose
            response = requests.get(data_url)
            response.raise_for_status()  # Raise an error for bad responses
            #print(f"Data fetched successfully from: {data_url}") # Useful to debug, but too vebose
            return pl.read_csv(io.StringIO(response.text), schema = schema)
        except Exception as e:
            print(f"Error fetching data from {data_url}: {e}")
            return pl.DataFrame(schema = schema)  # Return an empty DataFrame on failure

    # Use ThreadPoolExecutor for concurrent API calls
    df_list = []
    with ThreadPoolExecutor(max_workers = 3) as executor:
        # Submit tasks to the executor
        futures = {executor.submit(fetch_batch, url): url for url in requests_list}

        # Wrap the as_completed generator with tqdm for progress tracking
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching data"):
            url = futures[future]
            try:
                data = future.result()
                if not data.is_empty():
                    df_list.append(data)
            except Exception as e:
                print(f"Error processing URL {url}: {e}")

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