import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df: pl.DataFrame, *args, **kwargs):
    
    # Parse/Cast de los datos al tipo de dato correcto
    df_hist_cast = df.select(
        pl.col("folio").cast(pl.Utf8),
        pl.col("tipo_de_falla").cast(pl.Utf8),
        pl.col("quien_atiende").cast(pl.Utf8),
        pl.col("latitud").cast(pl.Float64),
        pl.col("longitud").cast(pl.Float64),
        pl.col("codigo_postal").cast(pl.Int16, strict=False),
        pl.col("fecha").str.to_date(),
        pl.col("colonia_registro_sacmex").cast(pl.Utf8),
        pl.col("colonia_datos_abiertos").cast(pl.Utf8),
        pl.col("alcaldia").cast(pl.Utf8)
    )

    # Quitamos los que son NA
    df_hist2 = df_hist_cast.filter(pl.col('tipo_de_falla') != 'NA')
    df_fugas_hist = df_hist2.filter(pl.col('tipo_de_falla') == 'Fuga')

    return df_fugas_hist, "reportes_agua_hist"


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
