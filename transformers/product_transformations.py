import polars as pl
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Ingest data from the upstring parent block and creates new product related variables. 

    New variables are: 
        liquor_type (Utf8):

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

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
        .alias("liquor_type")
        )

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test
def test_liquor_type_col(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.get_column("liquor_type") is not None, 'The column liquor_type is undefined'
    assert output.get_column("liquor_type").dtype is pl.Utf8, "The new variable type doesn't match"
