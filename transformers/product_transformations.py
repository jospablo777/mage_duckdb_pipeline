import polars as pl
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Ingest data from the upstring parent block and creates new product related variables. 

    New variables: 
        liquor_type (Utf8): classifies the type of the liquor in a category (Whisky, Tequila, ...).
        is_premium (Boolean): if the bottle cost 30 USD or more, we considere it premium.
        bottle_size (Utf8): category by bottle size (small, medium, and large).

    Args:
        data (pl.DataFrame): The output from the upstream parent block

    Returns:
        data (pl.DataFrame): a data frame with new variables added.
    """
    data = data.with_columns(
        # Categorize liquors
        pl.when(pl.col("category_name").str.contains("VODK")).then("Vodka")
        .when(pl.col("category_name").str.contains("WHISK")).then("Whisky")
        .when(pl.col("category_name").str.contains("RUM")).then("Rum")
        .when(pl.col("category_name").str.contains("SCHN")).then("Schnapps")
        .when(pl.col("category_name").str.contains("TEQ")).then("Tequila")
        .when(
            pl.col("category_name").str.contains("BRANDIE")
            | pl.col("category_name").str.contains("BRANDY")
            ).then("Brandy")
        .when(pl.col("category_name").str.contains("GIN")).then("Gin")
        .when(pl.col("category_name").str.contains("MEZC")).then("Mezcal")
        .when(
            pl.col("category_name").str.contains("CREM")
            | pl.col("category_name").str.contains("CREAM")
            ).then("Cream")
        .otherwise("Other")
        .alias("liquor_type"),
        # Is premium
        (pl.col("state_bottle_retail") >= 30).alias("is_premium"),
        # Bottle size category
        pl.when(pl.col("bottle_volume_ml") < 500).then("small")
        .when((pl.col("bottle_volume_ml") >= 500) & (pl.col("bottle_volume_ml") < 1000)).then("medium")
        .otherwise("large")
        .alias("bottle_size")
        )

    print("Product-related new variables, generated.")
    return data


@test
def test_output(output, *args) -> None:
    """
    Test the output exists.
    """
    assert output is not None, 'The output is undefined'

@test
def test_liquor_type_col(output, *args) -> None:
    """
    Test the new liquor_type column.
    """
    assert output.get_column("liquor_type") is not None, 'The column liquor_type is undefined'
    assert output.get_column("liquor_type").dtype is pl.Utf8, "The new variable type doesn't match"

@test
def test_is_premium_col(output, *args) -> None:
    """
    Test the new is_premium column.
    """
    assert output.get_column("is_premium") is not None, 'The column is_premium is undefined'
    assert output.get_column("is_premium").dtype is pl.Boolean, "The new variable type doesn't match"

@test
def test_bottle_size_col(output, *args) -> None:
    """
    Test the new bottle_size column.
    """
    assert output.get_column("bottle_size") is not None, 'The column bottle_size is undefined'
    assert output.get_column("bottle_size").dtype is pl.Utf8, "The new variable type doesn't match"
