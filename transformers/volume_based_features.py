import polars as pl
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_volume(data, *args, **kwargs):
    """
    Ingest data from the upstring parent block and creates new volume related variables. 

    New variables: 
        total_volume_ordered_L (Float64): total volume ordered in Liters of the product.
        volume_to_revenue_ratio (Float64): Indicates how much volume was sold per dollar earned

    Args:
        data (pl.DataFrame): The output from the upstream parent block

    Returns:
        data (pl.DataFrame): a data frame with new variables added.
    """
    data = data.with_columns(
        # Volume ordered
        (
            (pl.col("bottle_volume_ml") * pl.col("sale_bottles")) /
            1000 # mL in a Liter
        ).alias("total_volume_ordered_L"),
        # Volume/revenue ratio
        (
            pl.col("sale_liters") / pl.col("sale_dollars")
        ).alias("volume_to_revenue_ratio")
        

    )

    return data


@test
def test_output(output, *args) -> None:
    """
    Test the output exists.
    """
    assert output is not None, 'The output is undefined'

@test
def test_total_volume_output(output, *args) -> None:
    """
    Test the new total_volume_ordered_L column.
    """
    assert output.get_column("total_volume_ordered_L") is not None, 'The column total_volume_ordered_L is undefined'
    assert output.get_column("total_volume_ordered_L").dtype is pl.Float64, "The new variable type doesn't match"

@test
def test_volume_to_revenue_output(output, *args) -> None:
    """
    Test the new volume_to_revenue_ratio column.
    """
    assert output.get_column("volume_to_revenue_ratio") is not None, 'The column volume_to_revenue_ratio is undefined'
    assert output.get_column("volume_to_revenue_ratio").dtype is pl.Float64, "The new variable type doesn't match"
