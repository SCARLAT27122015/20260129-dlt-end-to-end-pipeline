import dlt
from pyspark.sql.functions import *

#straming view (ideal to perform transformations)
@dlt.view(
    name = 'stores_silver_view'
)
def stores_silver_view():
    df = (spark.readStream.table('stores_bronze')
                    .withColumn(
                        'store_name'
                        , regexp_replace(
                            col('store_name')
                            , '_'
                            , ''
                        )
                    )
                    .withColumn(
                        'processDate'
                        , current_timestamp()
                    )
    )

    return df

#stores silver table with CDC(to perform upsert)
dlt.create_streaming_table(name = 'stores_silver')    

dlt.create_auto_cdc_flow(
    target='stores_silver'
    , source='stores_silver_view'
    , keys=['store_id']
    , sequence_by=col('processDate')
    , stored_as_scd_type = 1
)
