import polars as pl

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    # Carga de los datos desde Portal ed Datos abiertos de la SACMEX
    df_historicos = pl.read_csv("https://datos.cdmx.gob.mx/dataset/57ffc13a-7207-4b69-a9a9-2fef3fce6331/resource/65a6b1a6-5d6e-49b9-aeed-ca7b22e8de03/download/reportes_agua_hist.csv",
    encoding='latin')

    return df_historicos


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
