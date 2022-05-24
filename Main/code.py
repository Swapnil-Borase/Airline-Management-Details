from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    spark=SparkSession.builder.appName("Airline_Management").master("local[*]").getOrCreate()
    print(spark)

                                ##Airline Schema##
    Airline_Schema=StructType([StructField("Airline_Id",IntegerType()),
                               StructField("Name",StringType()),
                               StructField("Alias", StringType()),
                               StructField("IATA_Code", StringType()),
                               StructField("ICAO_Code", StringType()),
                               StructField("Callsign", StringType()),
                               StructField("Country", StringType()),
                               StructField("Active", StringType())])


                                ##Airport Schema##
    Airport_Schema=StructType([StructField("Airport_Id",IntegerType()),
                               StructField("Name",StringType()),
                               StructField("City",StringType()),
                               StructField("Country",StringType()),
                               StructField("IATA_Code",StringType()),
                               StructField("ICAO_Code",StringType()),
                               StructField("Latitude",DecimalType()),
                               StructField("Longitude",DecimalType()),
                               StructField("Altitude",IntegerType()),
                               StructField("Timezone",DecimalType()),
                               StructField("DST",StringType()),
                               StructField("Tz",StringType()),
                               StructField("Type_Aprt",StringType()),
                               StructField("Source",StringType())])

                                ##Plane Schema##

    Plane_Schema=StructType([StructField("name", StringType()),
                            StructField("IATA_Code",StringType()),
                            StructField("ICAO_Code", StringType())])


                   ## Files read and creating Dataframe##
    Airline_DF=spark.read.csv(path=r"E:\Data Cloud Engineer\Pyspark\Pysparkinput files\Airline Management\airline.csv",schema=Airline_Schema)
    Airline_DF.show()

    #
    Airport_DF=spark.read.csv(path=r"E:\Data Cloud Engineer\Pyspark\Pysparkinput files\Airline Management\airport.csv",schema=Airport_Schema)
    Airport_DF.show()

    Plane_DF=spark.read.options(delimiter="").csv(path=r"E:\Data Cloud Engineer\Pyspark\Pysparkinput files\Airline Management\plane.csv", schema=Plane_Schema)
    Plane_DF.show()

    Route_DF=spark.read.parquet(r"E:\Data Cloud Engineer\Pyspark\Pysparkinput files\Airline Management\routes.snappy.parquet")
    Route_DF.show()


    ## Questions##


        ## --->Q.(1) Replace \N or Null values if its integer col then by -1 & if its string col then by (Unknown)


    Airline_DF1=Airline_DF.na.fill("Unknown").na.replace(("\\N"),("Uknown"))
    Airline_DF1.show()
    #
    #
    Airport_DF1 = Airport_DF.na.fill(value=-1)
    Airport_DF1.show()

    Plane_DF1=Plane_DF.na.replace(("\\N"),("Unknown"))
    Plane_DF1.show()

    Route_DF1 = Route_DF.withColumn("codeshare", when(Route_DF.codeshare.isNull(), "(Unknown)")
                                    .otherwise("codeshare")).na.replace("\\N", "(Unknown)")
    Route_DF1.show()

    ## Q.(2) Find country name which is having both airline and airport

    # Airport_DF1.join(Airline_DF1, on="Country", how="inner").select(Airport_DF1.Country.alias("comm_countries")) \
    #         .distinct().orderBy("comm_countries").show()


    ## Creating view for SQL Query

    Airline_DF1.createOrReplaceTempView("airline")
    Airport_DF1.createOrReplaceTempView("airport")
    Route_DF1.createOrReplaceTempView("route")
    Plane_DF1.createOrReplaceTempView("Plane")


    ## Using SQL
    # spark.sql("select distinct(apt.Country) from airline ail,airport apt where ail.Country=apt.Country order by apt.Country").show()


        ## Q.(3) Get airline details like name,id  which has taken off more than 3 times from same airport
    # Airline_DF1.join(Route_DF1, on="Airline_id", how="inner").groupBy("Airline_id", "Name", "src_airport") \
    #         .agg(count("src_airport").alias("takeoff")).filter(col("takeoff")>3).orderBy(col("takeoff")).show()

    ## Using SQL
    # spark.sql("select ai.Airline_id,ai.Name,src_airport,count(*) takeoff from airline ai inner join route rt on ai.Airline_id=rt.airline_id " +
    #           "group by ai.Airline_id,ai.Name,src_airport having count(*)>3 order by takeoff").show()

    ## Q.(4) Get airport details which has minimum number of takeoffs and landing



    # Airport_DF1.join(Route_DF1, Airport_DF1.Airport_Id == Route_DF1.src_airport_id, "inner")\
    # .groupBy("Airport_Id","Name","src_airport","dest_airport")\
    # .agg(count("src_airport").alias("takeoff"),count("dest_airport").alias("landing"))\
    # .filter((col("landing")==1) & (col("takeoff")==1)).show()

    ## By sql
    # spark.sql("select Airport_Id,Name,src_airport,dest_airport,count(src_airport) takeoff,count(dest_airport) landing "+
    #           "from airport ap inner join route rt on Airport_Id=src_airport_id " +
    #           "group by Airport_Id,Name,src_airport,dest_airport " +
    #           "order by takeoff asc,landing asc").show()

    ##Q(5). get airport details which is having maximum number of takeoff and landing

    # Airport_DF1.join(Route_DF1, Airport_DF1.Airport_Id == Route_DF1.src_airport_id, "inner") \
    #         .groupBy("Airport_Id", "Name", "src_airport", "dest_airport") \
    #     .agg(count("src_airport").alias("takeoff"), count("dest_airport").alias("landing")) \
    #     .orderBy(col("takeoff").desc(), col("landing").desc()).show()

    ## By SQL
    # spark.sql("select Airport_Id,Name,src_airport,dest_airport,count(src_airport) takeoff,count(dest_airport) landing "+
    #           "from airport ap inner join route rt on Airport_Id=src_airport_id " +
    #           "group by Airport_Id,Name,src_airport,dest_airport " +
    #           "order by takeoff desc,landing desc").show()

    ##  Q (6). Get the airline details, which is having direct flights.
                    # details like airline id, name, source airport name, and destination airport name

    # Airline_DF1.join(Route_DF1, on="Airline_Id", how="inner") \
    # .select("Airline_Id","Name","src_airport","dest_airport","stops").filter(col("stops")==0).show()


    ## By SQL
    spark.sql("select ai.Airline_id,Name,src_airport,dest_airport,stops "+
              "from airline ai inner join route rt on ai.airline_id=rt.airline_id where stops=0").show()