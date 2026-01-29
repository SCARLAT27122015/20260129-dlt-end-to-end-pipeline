import dlt

@dlt.table(
    name = 'sales_bronze'
)
def sales_bronze():
    df = (spark.readStream.format('cloudFiles')
                        .option('cloudFiles.format', 'csv')
                        .load('/Volumes/databricks_bootcamp_catalog/bronze/bronze_volume/sales/')
    )

    return df


@dlt.table(
    name = 'stores_bronze'
)
def stores_bronze():
    df = (spark.readStream.format('cloudFiles')
                        .option('cloudFiles.format', 'csv')
                        .load('/Volumes/databricks_bootcamp_catalog/bronze/bronze_volume/stores/')
    )

    return df

@dlt.table(
    name = 'products_bronze'
)
def products_bronze():
    df = (spark.readStream.format('cloudFiles')
                        .option('cloudFiles.format', 'csv')
                        .load('/Volumes/databricks_bootcamp_catalog/bronze/bronze_volume/products/')
    )

    return df

@dlt.table(
    name = 'customers_bronze'
)
def customers_bronze():
    df = (spark.readStream.format('cloudFiles')
                        .option('cloudFiles.format', 'csv')
                        .load('/Volumes/databricks_bootcamp_catalog/bronze/bronze_volume/customers/')
    )

    return df

