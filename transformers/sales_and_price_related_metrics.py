import polars as pl
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    data = data.with_columns(
        # Government profit margin
        (
            pl.col("state_bottle_retail") - pl.col("state_bottle_cost")
        ).alias("gov_profit_margin"),
        # Retail markup (Gov)
        (
            (pl.col("state_bottle_retail") - pl.col("state_bottle_cost"))
            / pl.col("state_bottle_cost")
            * 100
        ).alias("gov_retail_markup_percentage"),
        # Price per Liter
        (
            pl.col("state_bottle_retail") / 
            (pl.col("bottle_volume_ml") /
            1000) # mL in a Liter
        ).alias("price_per_liter"),
        # Price per gallon
        (
            pl.col("state_bottle_retail") / 
            (pl.col("bottle_volume_ml") /
            3785.411784) # mL in a gallon
        ).alias("price_per_gallon")

    )

    return data

@test
def test_gov_profit_margin_col(output, *args) -> None:
    """
    Test the new gov_profit_margin column.
    """
    assert output.get_column("gov_profit_margin") is not None, 'The column gov_profit_margin is undefined'
    assert output.get_column("gov_profit_margin").dtype is pl.Float64, "The new variable type doesn't match"

@test
def test_gov_retail_markup_col(output, *args) -> None:
    """
    Test the new retail_markup_percentage column.
    """
    assert output.get_column("gov_retail_markup_percentage") is not None, 'The column gov_retail_markup_percentage is undefined'
    assert output.get_column("gov_retail_markup_percentage").dtype is pl.Float64, "The new variable type doesn't match"

@test
def test_price_per_liter_col(output, *args) -> None:
    """
    Test the new price_per_liter column.
    """
    assert output.get_column("price_per_liter") is not None, 'The column price_per_liter is undefined'
    assert output.get_column("price_per_liter").dtype is pl.Float64, "The new variable type doesn't match"

@test
def test_price_per_gallon_col(output, *args) -> None:
    """
    Test the new price_per_gallon column.
    """
    assert output.get_column("price_per_gallon") is not None, 'The column price_per_gallon is undefined'
    assert output.get_column("price_per_gallon").dtype is pl.Float64, "The new variable type doesn't match"