import polars as pl
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_time(data, *args, **kwargs):
    """
    Ingest data from the upstring parent block and creates new volume related variables. 

    New variables: 
        week_day (UInt32): week day (Monday = 1 and Sunday = 7).
        is_weekend (Boolean): true when the day of the week is Saturday or Sunday.
        quarter (UInt32): quarter of the year (Q1 = 1, Q2 = 2, ...).

    Args:
        data (pl.DataFrame): The output from the upstream parent block

    Returns:
        data (pl.DataFrame): a data frame with new variables added.
    """
    data = data.with_columns(
        # Day of the week
        pl.col("date").dt.weekday().alias("week_day")
    ).with_columns(
        # Is it a weekend?
        pl.col("week_day").is_in([6, 7]).alias("is_weekend"),
        # Year quarter
        pl.col("date").dt.quarter().alias("quarter")
    )

    print("Time-based features, computed.")
    return data


@test
def test_output(output, *args) -> None:
    """
    Test the output exists.
    """
    assert output is not None, 'The output is undefined'

@test
def test_week_day_output(output, *args) -> None:
    """
    Test the new week_day column.
    """
    assert output.get_column("week_day") is not None, 'The column week_day is undefined'
    assert output.get_column("week_day").dtype is pl.UInt32, "The new variable type doesn't match"

@test
def test_is_weekend_output(output, *args) -> None:
    """
    Test the new is_weekend column.
    """
    assert output.get_column("is_weekend") is not None, 'The column is_weekend is undefined'
    assert output.get_column("is_weekend").dtype is pl.Boolean, "The new variable type doesn't match"

@test
def test_quarter_output(output, *args) -> None:
    """
    Test the new quarter column.
    """
    assert output.get_column("quarter") is not None, 'The column quarter is undefined'
    assert output.get_column("quarter").dtype is pl.UInt32, "The new variable type doesn't match"