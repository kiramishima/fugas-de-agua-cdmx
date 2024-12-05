import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    df_cast = data.select(
        pl.col("folio_incidente").cast(pl.Utf8).alias('folio'),
        pl.col("fecha_registro_incidente").str.to_date(),
        pl.col("id_reporte").cast(pl.Utf8),
        pl.col("fecha_reporte").str.to_date(),
        pl.col("hora_reporte").cast(pl.Utf8),
        pl.col("clasificacion").cast(pl.Utf8),
        pl.col("reporte").cast(pl.Utf8).alias("tipo_de_falla"),
        pl.col("medio_recepcion").cast(pl.Utf8),
        pl.col("alcaldia_catalogo").cast(pl.Utf8),
        pl.col("colonia_catalogo").cast(pl.Utf8),
        pl.col("latitud").cast(pl.Float64),
        pl.col("longitud").cast(pl.Float64)
    )
    # print(df_cast.head())
    # Quitamos los que son NA
    df2 = df_cast.filter(pl.col('tipo_de_falla') != 'NA')
    # Seleccionamos los datos de fugas de agua
    df_fugas = df2.filter(pl.col('tipo_de_falla') == 'Fuga')

    return df_fugas, "reportes_agua_2024_01"


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
