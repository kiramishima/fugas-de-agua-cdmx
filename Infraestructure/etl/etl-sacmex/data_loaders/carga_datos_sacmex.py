import polars as pl

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    
    df_actual = pl.read_csv("https://datos.cdmx.gob.mx/dataset/57ffc13a-7207-4b69-a9a9-2fef3fce6331/resource/a8069e94-c7cb-45d7-8166-561e80884422/download/reportes_agua_2024_01.csv")
    return df_actual


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
