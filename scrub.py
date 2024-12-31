from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, upper, when, trim, concat, initcap, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("state_abbr", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("pop_estimate_2022", IntegerType(), True),
    StructField("county_fips", StringType(), True),
    StructField("division", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("place_fips", StringType(), True),
    StructField("sum_lev", IntegerType(), True),
    StructField("lsadc", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("url", StringType(), True)
])

spark = SparkSession.builder.appName("pursuit").getOrCreate()

places_df = spark.read.csv("data/csv/places.csv", header=True, schema=schema)
places_df = (
    places_df
    .withColumn("status", lit("unknown"))
    .withColumn("fields", lit(""))
)

# this is checking for names

places_df = (
    places_df
    .withColumn("name", trim(places_df["name"]))
    .withColumn(
        "status",
        when(col("name").isNull(), lit("dirty"))
    )
    .withColumn(
        "fields",
        when(col("name").isNull(), concat(col("fields"), lit("|name|"))).otherwise(col("fields"))
    )
)

############################################


# this is checking for state abbreviations

valid_state_abbrs = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                     "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                     "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                     "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                     "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

places_df = (
    places_df
    .withColumn("state_abbr", upper(trim(places_df["state_abbr"])))
    .withColumn(
        "status",
        when(~col("state_abbr").isin(valid_state_abbrs), lit("dirty"))
        .otherwise(col("status"))
    )
    .withColumn(
        "fields",
        when((col("state_abbr").isNull()) | (~col("state_abbr").isin(valid_state_abbrs)), concat(col("fields"), lit("|state_abbr|"))).otherwise(col("fields"))
    )
)

############################################

# this is checking for lat & long

places_df = (
    places_df
    .withColumn(
        "status",
        when(
            (col("lat") < -90) | (col("lat") > 90) | (col("long") < -180) | (col("long") > 180),
            lit("dirty")
        )
        .otherwise(col("status"))
    )
    .withColumn(
        "fields",
        when((col("lat") < -90) | (col("lat") > 90) | (col("long") < -180) | (col("long") > 180), concat(col("fields"), lit("|lat or long|"))).otherwise(col("fields"))
    )
)

# Notes: hit up a service that gets you the lat and long of a `name`

############################################

# this is checking for pop_estimate_2022

places_df = (
    places_df
    .withColumn(
        "status",
        when(
            (col("pop_estimate_2022") < 0),
            lit("dirty")
        )
        .otherwise(col("status"))
    )
    .withColumn(
        "fields",
        when(col("pop_estimate_2022") < 0, concat(col("fields"), lit("|pop_estimate_2022|"))).otherwise(col("fields"))
    )
)

############################################

# this is checking for county_fips

# https://github.com/hadley/data-counties/blob/master/county-fips.txt

# The last three digits represent the specific county or county-equivalent within the state.
places_df = places_df.withColumn(
    "county_fips",
    lpad(col("county_fips").cast("string"), 5, "0")
)

############################################

# this is checking for division

places_df = (
    places_df
    .withColumn(
        "status",
        when(
            ((col("division") < 1) | (col("division") > 9)),
            lit("dirty")
        )
        .otherwise(col("status"))
    )
    .withColumn(
        "fields",
        when((col("division") < 1) | (col("division") > 9), concat(col("fields"), lit("|division|"))).otherwise(col("fields"))
    )
)

############################################

# this is checking for full_name

places_df = (
    places_df
    .withColumn("full_name", initcap(trim(places_df["full_name"])))
)

############################################

# this is checking for place_fips

places_df = places_df.withColumn(
    "place_fips",
    lpad(col("place_fips").cast("string"), 5, "0")
)

############################################

# this is checking for sum_lev

valid_sum_lev_codes = [10, 20, 30, 40, 50, 60, 160, 360, 500, 610, 850, 970]

places_df = (
    places_df
    .withColumn(
        "status",
        when(
            (~col("sum_lev").isin(valid_sum_lev_codes)),
            lit("dirty")
        )
        .otherwise(col("status"))
    )
    .withColumn(
        "fields",
        when(~col("sum_lev").isin(valid_sum_lev_codes), concat(col("fields"), lit("|sum_lev|"))).otherwise(col("fields"))
    )
)

############################################

# this is checking for lsadc

lsadc_codes = [
    "00", "03", "04", "05", "06", "07", "10", "12", "13", "15",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "36", "37", "39", "41", "42", "43", "44",
    "45", "46", "47", "49", "51", "53", "55", "57", "62", "68",
    "69", "70", "71", "72", "73", "74", "75", "76", "77", "78",
    "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "Z1"
]

places_df = (
    places_df
    .withColumn(
        "status",
        when(
            (~col("lsadc").isin(lsadc_codes)),
            lit("dirty")
        )
        .otherwise(col("status"))
    )
    .withColumn(
        "fields",
        when(~col("lsadc").isin(lsadc_codes), concat(col("fields"), lit("|sum_lev|"))).otherwise(col("fields"))
    )
)

############################################


places_df.filter(col("status") == "dirty").show()