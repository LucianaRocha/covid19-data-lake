# Import the necessary packages
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def create_spark_session():
    """Create a Spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_covid_dimension(spark, input_data, covid19_lake, output_data):
    """
    Write country, province and time dimensions to parquet files on S3.

    Keyword arguments:
    spark -- a spark session
    input_data -- the script reads data from S3 or public datalake
    covid19_lake -- the script reads data from S3 or public datalake
    output_data -- the script writes dimension to partitioned parquet on S3
    """

    # get filepath to dimensions
    covid_global_data = covid19_lake + \
        'archived/tableau-jhu/csv/COVID-19-Cases.csv'
    covid_brazil_data = input_data + 'COVID-19-Brazil.csv.gz'
    brazil_provinces = input_data + 'provinces_brazil.csv'

    # define the data frames
    global_data_df = spark.read.load(
        covid_global_data, format="csv", sep=",",
        inferSchema="true", header="true")
    global_data_df = global_data_df.dropDuplicates()
    global_data_df.createOrReplaceTempView("global_data")

    brazil_data_df = spark.read.load(
        covid_brazil_data, format="csv", sep=",",
        inferSchema="true", header="true")
    brazil_data_df = brazil_data_df.dropDuplicates()
    brazil_data_df.createOrReplaceTempView("brazil_data")

    brazil_provinces_df = spark.read.load(
        brazil_provinces, format="csv", sep=",",
        inferSchema="true", header="true")
    brazil_provinces_df = brazil_provinces_df.dropDuplicates()
    brazil_provinces_df.createOrReplaceTempView("brazil_provinces")

    date_df = global_data_df.filter("country_region = 'Brazil'")
    date_df = date_df.select("Date")
    date_df = date_df.dropDuplicates()
    split_date = F.split(date_df["Date"],'/')
    date_df = date_df.withColumn('Month', split_date.getItem(0))
    date_df = date_df.withColumn('Day', split_date.getItem(1))
    date_df = date_df.withColumn('Year', split_date.getItem(2))
    date_df = date_df.select(
        date_df.Date,
        F.lpad(date_df.Month,2,'0').alias('Month'),
        F.lpad(date_df.Day,2,'0').alias('Day'),
        F.lpad(date_df.Year,4,'0').alias('Year'))
    date_df = date_df.select(
        date_df.Date,
        date_df.Month,
        date_df.Day,
        date_df.Year,
        F.to_date(F.concat_ws('-',date_df.Month, date_df.Day, date_df.Year),
                  'MM-dd-yyyy').alias('Date_format'))
    date_df = date_df.select(
        date_df.Date,
        date_df.Date_format,
        date_df.Year,
        F.weekofyear(date_df.Date_format).alias('Week'),
        date_df.Month,
        date_df.Day,
        F.dayofweek(date_df.Date_format).alias('Week_Day'))
    date_df.show(2)
    date_df.createOrReplaceTempView("date")

    # extract columns to create tables
    dim_country = spark.sql(
        "SELECT DISTINCT \
            Country_Region, \
            iso2, \
            iso3, \
            sum(Population_Count) as population_count \
        FROM global_data \
        WHERE \
            case_type = 'Deaths' \
            AND Date = '5/19/2020' \
        GROUP BY \
            Country_Region, \
            iso2, \
            iso3"
        )

    dim_province_us = spark.sql(
        "SELECT DISTINCT \
            country_region, \
            province_state, \
            max(Population_Count) as population_count \
        FROM global_data \
        WHERE \
            case_type = 'Deaths' \
            AND country_region = 'US' \
            AND Date = '5/19/2020' \
        GROUP BY \
            Country_Region, \
            province_state"
        )

    dim_province_br = spark.sql(
        "SELECT DISTINCT \
            'Brazil' as country_region, \
            bp.province_state as province_state, \
            sum(estimated_population_2019) as population_count \
        FROM brazil_data bd \
            JOIN brazil_provinces bp ON bd.state = bp.state \
        WHERE \
            place_type = 'state' \
            AND date = '2020-05-19' \
        GROUP BY \
            Country_Region, \
            province_state"
        )

    dim_province = dim_province_us.unionByName(dim_province_br)

    dim_time = spark.sql(
        "SELECT DISTINCT \
            date, year, week, month, day, week_day \
        FROM date"
        )

    # write tables to partitioned parquet files
    dim_country.write.mode('overwrite') \
        .partitionBy('country_region') \
        .parquet(output_data + 'dim_country')

    dim_province.write.mode('overwrite') \
        .partitionBy('country_region', 'province_state') \
        .parquet(output_data + 'dim_province')

    dim_time.write.mode('overwrite') \
        .partitionBy('year','month') \
        .parquet(output_data + 'dim_time')

    count_dim = {}
    count_dim['country'] = spark.sql(
        "SELECT COUNT (DISTINCT country_region) as countries \
        FROM global_data").collect()[0].countries
    count_dim['date'] = spark.sql(
        "SELECT COUNT (DISTINCT date) as dates \
        FROM global_data").collect()[0].dates
    return count_dim


def process_covid_brazil_fact(spark, input_data, output_data):
    """
    Write brazil data detail to parquet files on S3.

    Keyword arguments:
    spark -- a spark session
    input_data -- the script reads data from S3 or public datalake
    output_data -- writes province and country to partitioned parquet on S3
    """
    # get filepath to brazil fact
    covid_brazil_data = input_data + 'COVID-19-Brazil.csv.gz'
    brazil_provinces = input_data + 'provinces_brazil.csv'

    # define the data frames
    brazil_data_df = spark.read.load(
        covid_brazil_data, format="csv", sep=",",
        inferSchema="true", header="true")
    brazil_data_df = brazil_data_df.dropDuplicates()
    brazil_data_df = brazil_data_df.withColumn(
        'previous', F.date_format('date', 'MM/dd/yyyy'))
    brazil_data_df.createOrReplaceTempView("brazil_data")
    brazil_data_df.createOrReplaceTempView("brazil_p_data")
    brazil_data_df.show(5)

    provinces_brazil_df = spark.read.load(
        brazil_provinces, format="csv", sep=",",
        inferSchema="true", header="true")
    provinces_brazil_df = provinces_brazil_df.dropDuplicates()
    provinces_brazil_df.createOrReplaceTempView("brazil_provinces")
    provinces_brazil_df.show(5)

    # extract columns to create fact_covid_province_country about Brazil
    print('inicia sql')
    fact_covid_country_province = spark.sql(
        "SELECT DISTINCT \
            'Brazil' as country_name, \
            pr.province_state as province_state, \
            to_date(bd.date) as date, \
            bd.confirmed - bp.confirmed as confirmed_cases, \
            bd.deaths - bp.deaths as death_cases, \
            bd.confirmed as sum_confirmed_cases, \
            bd.deaths as sum_death_cases \
        FROM brazil_data bd \
            JOIN brazil_p_data bp \
                ON (date_add(to_date(bd.date),-1) = to_date(bp.date) \
                    AND bd.state = bp.state) \
            JOIN brazil_provinces pr \
                ON (trim(bd.state) = trim(pr.state) \
                    AND trim(bp.state) = trim(pr.state)) \
        WHERE \
            bd.place_type = 'state' \
            AND bp.place_type = 'state'"
        )

    fact_covid_country_province.show(10)

    # extract columns to create fact_covid_country about Brazil
    fact_covid_country = spark.sql(
        "SELECT DISTINCT \
            'Brazil' as country_name, \
            to_date(bd.date) as date, \
            sum(bd.confirmed - bp.confirmed) as confirmed_cases, \
            sum(bd.deaths - bp.deaths) as death_cases, \
            sum(bd.confirmed) as sum_confirmed_cases, \
            sum(bd.deaths) as sum_death_cases \
        FROM brazil_data bd \
            JOIN brazil_p_data bp \
                ON (date_add(to_date(bd.date),-1) = to_date(bp.date) \
                    AND bd.state = bp.state) \
        WHERE \
            bd.place_type = 'state' \
            AND bp.place_type = 'state' \
        GROUP BY \
            country_name, bd.date"
        )

    fact_covid_country.show(10)
 
    # write tables to partitioned parquet files
    fact_covid_country_province.write.mode('overwrite') \
        .partitionBy('country_name','province_state', 'date') \
        .parquet(output_data + 'fact_covid_country_province')

    fact_covid_country.write.mode('overwrite') \
        .partitionBy('country_name', 'date') \
        .parquet(output_data + 'fact_covid_country')


def process_covid_usa_fact(spark, covid19_lake, output_data):
    """
    Write USA data detail to parquet files on S3.

    Keyword arguments:
    spark -- a spark session
    covid19_lake -- the script reads data from S3 or public datalake
    output_data -- writes province and country to partitioned parquet on S3
    """

    # get filepath to usa fact
    covid_usa_data = covid19_lake + \
        'enigma-aggregation/json/us_states/*.json'

    # define the data frames
    usa_data_df = spark.read.json(covid_usa_data)
    usa_data_df = usa_data_df.dropDuplicates()
    usa_data_df = usa_data_df.withColumn(
        'previous', F.date_format('date', 'yyyy-mm-dd'))
    usa_data_df.createOrReplaceTempView("usa_current_data")
    usa_data_df.createOrReplaceTempView("usa_previous_data")
    usa_data_df.show(5)

    # extract columns to create tables
    fact_covid_country_province = spark.sql(
        "SELECT DISTINCT \
            'United States' as country_name, \
            bd.state_name as province_state, \
            to_date(bd.date) as date, \
            bd.cases - bp.cases as confirmed_cases, \
            bd.deaths - bp.deaths as death_cases, \
            bd.cases as sum_confirmed_cases, \
            bd.deaths as sum_death_cases \
        FROM usa_current_data bd \
            JOIN usa_previous_data bp \
                ON (date_add(to_date(bd.date),-1) = to_date(bp.date) \
                    AND bd.state_name = bp.state_name \
                    AND bd.state_fips = bp.state_fips)"
        )

    fact_covid_country_province.show(5)

    fact_covid_country = spark.sql(
        "SELECT \
            'United States' as country_name, \
            to_date(bd.date) as date, \
            sum(bd.cases - bp.cases) as confirmed_cases, \
            sum(bd.deaths - bp.deaths) as death_cases, \
            sum(bd.cases) as sum_confirmed_cases, \
            sum(bd.deaths) as sum_death_cases \
        FROM usa_current_data bd \
            JOIN usa_previous_data bp \
                ON (date_add(to_date(bd.date),-1) = to_date(bp.date) \
                    AND bd.state_name = bp.state_name \
                    AND bd.state_fips = bp.state_fips) \
        GROUP BY \
            country_name, to_date(bd.date)"
        )

    fact_covid_country.show(5)

    # write tables to partitioned parquet files 
    fact_covid_country_province.write.mode('append') \
        .partitionBy('country_name','province_state', 'date') \
        .parquet(output_data + 'fact_covid_country_province')

    fact_covid_country.write.mode('append') \
        .partitionBy('country_name', 'date') \
        .parquet(output_data + 'fact_covid_country')    


def process_covid_country_fact(spark, covid19_lake, output_data):
    """
    Write countries covid data to parquet files on S3.
    
    Keyword arguments:
    spark -- a spark session
    covid19_lake -- the script reads data from S3 or public datalake
    output_data -- writes province and country to partitioned parquet on S3
    """
    # get filepath to country fact table
    covid_country_data = covid19_lake + \
        'archived/tableau-jhu/csv/COVID-19-Cases.csv'

    # define the data frames
    country_data_df = spark.read.load(
        covid_country_data, format="csv", sep=",",
        inferSchema="true", header="true")
    country_data_df = country_data_df.dropDuplicates()
    split_date = F.split(country_data_df["Date"], '/')
    country_data_df = country_data_df.withColumn(
        'Month',
        split_date.getItem(0))
    country_data_df = country_data_df.withColumn(
        'Day',
        split_date.getItem(1))
    country_data_df = country_data_df.withColumn(
        'Year',
        split_date.getItem(2))

    country_data_df.show(5)

    country_data_df = country_data_df.select(
        country_data_df.Country_Region, \
        country_data_df.Cases, \
        country_data_df.Case_Type, \
        country_data_df.Combined_Key, \
        country_data_df.Date, \
        F.lpad(country_data_df.Month,2,'0').alias('Month'), \
        F.lpad(country_data_df.Day,2,'0').alias('Day'), \
        F.lpad(country_data_df.Year,4,'0').alias('Year'), \
        F.to_date(
            F.concat_ws(
                '-', country_data_df.Month,
                country_data_df.Day, country_data_df.Year), \
            'MM-dd-yyyy').alias('Date_format'))

    country_data_df.createOrReplaceTempView("country_data")
    country_data_df.createOrReplaceTempView("country_p_data")

    country_data_df.show(5)

    fact_covid_country_confirmed = spark.sql(
        "SELECT DISTINCT \
            bd.country_region as country_name, \
            to_date(bd.Date_format) as date, \
            case when bd.case_type = 'Confirmed' \
                then (bd.cases - bp.cases) end as confirmed_cases, \
            case when bd.case_type = 'Deaths' \
                then (bd.cases - bp.cases) end as death_cases, \
            case when bd.case_type = 'Confirmed' \
                then bd.cases end as sum_confirmed_cases, \
            case when bd.case_type = 'Deaths' \
                then bd.cases end as sum_death_cases \
        FROM country_data bd \
            JOIN country_p_data bp \
                ON (date_add(to_date(bd.Date_format), -1) \
                     = to_date(bp.Date_format) \
                    AND bd.case_type = bp.case_type \
                    AND bd.country_region = bp.country_region \
                    AND bd.combined_key = bp.combined_key) \
        WHERE \
            bd.case_type = 'Confirmed' \
            AND bp.case_type = 'Confirmed'"
        )

    fact_covid_country_confirmed.show(10)

    fact_covid_country_death = spark.sql(
        "SELECT DISTINCT \
            bd.country_region as country_name, \
            to_date(bd.Date_format) as date, \
            case when bd.case_type = 'Confirmed' \
                then (bd.cases - bp.cases) end as confirmed_cases, \
            case when bd.case_type = 'Deaths' \
                then (bd.cases - bp.cases) end as death_cases, \
            case when bd.case_type = 'Confirmed' \
                then bd.cases end as sum_confirmed_cases, \
            case when bd.case_type = 'Deaths' \
                then bd.cases end as sum_death_cases \
        FROM country_data bd \
            JOIN country_p_data bp \
                ON (date_add(to_date(bd.Date_format),-1) \
                    = to_date(bp.Date_format) \
                    AND bd.case_type = bp.case_type \
                    AND bd.country_region = bp.country_region \
                    ANd bd.combined_key = bp.combined_key) \
        WHERE \
            bd.country_region not in ('Brazil', 'US') \
            AND bp.country_region not in ('Brazil', 'US') \
            AND bd.case_type = 'Deaths' \
            AND bp.case_type = 'Deaths'"
        )
    
    fact_covid_country = fact_covid_country_confirmed \
        .unionByName(fact_covid_country_death)

    fact_covid_country.createOrReplaceTempView("fact_covid_country")

    fact_covid_country = spark.sql(
        "SELECT \
            country_name, \
            date, \
            sum(confirmed_cases) as confirmed_cases, \
            sum(death_cases) as death_cases, \
            sum(sum_confirmed_cases) as sum_confirmed_cases, \
            sum(sum_death_cases) as sum_death_cases \
        FROM fact_covid_country \
        GROUP BY \
            country_name, date"
        )

    fact_covid_country.show(10)

    # write tables to partitioned parquet files
    fact_covid_country.write.mode('append') \
        .partitionBy('country_name', 'date') \
        .parquet(output_data + 'fact_covid_country')

    count_fact = {}
    count_fact['country'] = spark.sql(
        "SELECT COUNT (DISTINCT country_name) as countries \
        FROM fact_covid_country").collect()[0].countries
    count_fact['date'] = spark.sql(
        "SELECT COUNT (DISTINCT date) as dates \
        FROM fact_covid_country").collect()[0].dates
    return count_fact


def data_quality_check(count_dim, count_fact):
    if count_dim['country'] == count_fact['country']:
        print('Data quality check: SUCCESS. '
              f'Count countries: {count_dim["country"]}')
    else:
        print('Data quality check: Failed. '
              f'dimension contains {count_dim["country"]} distinct countries, '
              f'fact contains {count_fact["country"]} distinct countries.')

    if count_dim['date'] == count_fact['date']:
        print('Data quality check: SUCCESS. '
              f'Count dates: {count_dim["date"]}')
    else:
        print(f'Data quality check: Failed. '
              f'dimension contains {count_dim["date"]} distinct dates, '
              f'fact contains {count_fact["date"]} distinct dates.')


def main():
    """Create a spark session"""
    spark = create_spark_session()

    input_data = "s3a://covid19-input/raw-data/"
    output_data = "s3a://covid19-global-datalake/"
    covid19_lake = "s3a://covid19-lake/"

    count_dim = process_covid_dimension(
        spark, input_data, covid19_lake, output_data)
    process_covid_brazil_fact(spark, input_data, output_data)
    process_covid_usa_fact(spark, covid19_lake, output_data)
    count_fact = process_covid_country_fact(spark, covid19_lake, output_data)

    data_quality_check(count_dim, count_fact)


if __name__ == "__main__":
    main()

