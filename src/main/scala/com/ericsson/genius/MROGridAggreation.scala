package com.ericsson.genius

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object MROGridAggreation {
    def getGridId(lng:Double, lat:Double, broadcastLongitude:Broadcast[Array[(Double,Double)]], broadcastLatitude:Broadcast[Array[(Double,Double)]]): (Integer, Integer, Integer) = {
        var low, high, mid : Int = 0
        var a,b=0
        /*先对经度进行二分查找*/
        low = 0
        high = 1323 - 1
        while(low <= high)
        {
            mid = (low+high) / 2;
            if(broadcastLongitude.value(mid)._2 >= lng && broadcastLongitude.value(mid)._1 <= lng)
                a = mid
            if(broadcastLongitude.value(mid)._2 > lng)
                high = mid-1
            if(broadcastLongitude.value(mid)._1 < lng)
                low = mid+1
        }
        /*对纬度进行二分查找*/
        low = 0
        high = 830 - 1
        while(low <= high)
        {
            mid = (low+high) / 2;
            if(broadcastLatitude.value(mid)._2 >= lat && broadcastLatitude.value(mid)._1 <= lat)
                b = mid
            if(broadcastLatitude.value(mid)._2 > lat)
                high = mid-1
            if(broadcastLatitude.value(mid)._1 < lat)
                low = mid+1
        }
        if(a==0 || b==0)
            (-1, -1, -1)
        else
            (1323 * b + (a + 1), a, b)
    }

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("yarn").setAppName("mro_rsrp_grid_aggreation")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "1g")
                .set("spark.kryoserializer.buffer", "256m")
                .set("spark.executor.memory", "6g")
                .set("spark.driver.memory", "6g")
                .set("spark.executor.memoryOverhead", "4g")
                .set("spark.driver.memoryOverhead", "4g")
                .set("spark.network.timeout", "300")
                .set("spark.sql.broadcastTimeout", "1200")
                .set("spark.rdd.compress", "true")
                //                .set("spark.default.parallelism", "60")
                .set("spark.sql.shuffle.partitions", "500")
                .set("spark.task.maxFailures", "1")

        val spark: SparkSession = SparkSession.builder
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate

        //insert to mysql directly
        val url : String = args(0)
        val dbtable_aggre : String = args(1)
        val dbtable_scell : String = args(2)
        val dbtable_ncell : String = args(3)
        val dbtable_scell_avg : String = args(4)
        val dbtable_ncell_avg : String = args(5)

        import spark.implicits._

        /* get gridinfo by csv */
        val df_Gridinfo = spark.read.option("header", true).csv("hdfs://gdb-nt189:8020/data/resources/GridInfo_NT_100.csv")
        df_Gridinfo.cache()
        df_Gridinfo.createOrReplaceTempView("GridInfo")

        val broadcastLongitude:Broadcast[Array[(Double,Double)]] = spark.sparkContext.broadcast(df_Gridinfo.groupBy("longitudeMin", "longitudeMax").count().orderBy("longitudeMin")
                .select("longitudeMin", "longitudeMax").collect()
                .map(row => (row.getString(0).toDouble, row.getString(1).toDouble)))
        val broadcastLatitude:Broadcast[Array[(Double,Double)]] = spark.sparkContext.broadcast(df_Gridinfo.groupBy("latitudeMin", "latitudeMax").count().orderBy("latitudeMin")
                .select("latitudeMin", "latitudeMax").collect()
                .map(row => (row.getString(0).toDouble, row.getString(1).toDouble)))

        spark.udf.register("getGridId", getGridId(_:Double, _:Double, _:Broadcast[Array[(Double,Double)]], _:Broadcast[Array[(Double,Double)]]))

        /*
           get siteLte info from jdbc
           */
        val prop = new java.util.Properties
        prop.setProperty("user","root")
        prop.setProperty("password","mongs")
        prop.setProperty("batchsize", "1000")
        val dburl = "jdbc:mysql://10.39.148.186:3306/mongs"
        val tableName = "siteLte"
        val columnName = "id"
        val lowerBound = 1
        val upperBound = 200000
        val numPartitions = 100
        val df_sitelte = spark.read.jdbc(dburl, tableName, columnName, lowerBound, upperBound, numPartitions, prop)
                .select("ecgi", "pci", "earfcn", "longitude", "latitude")
        df_sitelte.cache()
        df_sitelte.createOrReplaceTempView("siteLte")
        val df_sitelte_final =spark.sql("select substring_index(ecgi, '-', -2) as ecgi, pci, earfcn, longitude, latitude from siteLte")
                .map(r => {
                    val gridinfo = getGridId(r.getString(3).trim.toDouble, r.getString(4).trim.toDouble, broadcastLongitude, broadcastLatitude)
                    (r.getString(0), r.getInt(1), r.getInt(2), gridinfo._1, gridinfo._2, gridinfo._3)
                })
                .filter(r => !r._4.equals(-1))
                .toDF("ecgi", "pci", "earfcn", "id", "xIndex", "yIndex")
        df_sitelte_final.cache()
        df_sitelte_final.createOrReplaceTempView("siteLte")
        df_sitelte.unpersist()


        val df_MRO = spark.read.textFile("hdfs://gdb-nt189:8020/data/mro/20181103*", "hdfs://gdb-nt189:8020/data/mro/20181104*", "hdfs://gdb-nt189:8020/data/mro/20181105*")
//        val df_MRO = spark.read.textFile("hdfs://gdb-nt189:8020/data/mro/2018110310")
                .map(_.split("\\|", -1))
                .filter(r => !r(8).equals("NIL") && !r(9).equals("NIL") && !r(34).equals("NIL") && !r(35).equals("NIL"))
                .map(r => (r(5), r(6), r(8).trim.toDouble, r(9).trim.toDouble, r(10).trim.toDouble, r(11).trim.toDouble, r(16), r(17), r(18), r(19), r(34).trim.toDouble, r(35).trim.toDouble))
                //.filter(r => !r._1.equals("NIL") && !r._2.equals("NIL") && !r._3.equals("NIL"))
                .toDF("ecgi", "reportid", "LteScRSRP", "LteNcRSRP", "LteScRSRQ", "LteNcRSRQ", "LteScEarfcn", "LteScPci", "LteNcEarfcn", "LteNcPci", "UELongitude", "UELatitude")
        df_MRO.cache()
        df_MRO.createOrReplaceTempView("MRO")

        val df_grid = spark.sql("select UELongitude, UELatitude from MRO group by UELongitude,UELatitude")
                .map(r => {
                    val gridinfo = getGridId(r.getDouble(0), r.getDouble(1), broadcastLongitude, broadcastLatitude)
                    (r.getDouble(0), r.getDouble(1), gridinfo._1, gridinfo._2, gridinfo._3)
                })
                .filter(r => !r._3.equals(-1))
                .toDF("UELongitude", "UELatitude", "id", "xIndex", "yIndex")
        val df_MRO_withid = df_grid.join(df_MRO, Seq("UELongitude", "UELatitude"))
        df_MRO_withid.createOrReplaceTempView("MRO")
        spark.sqlContext.cacheTable("MRO")
//
//        val df_query_aggre = spark.sql("select a.id, first(a.latitudeMin) as latitudeMin, first(a.latitudeMax) as latitudeMax, first(a.longitudeMin) as longitudeMin, first(a.longitudeMax) as longitudeMax, " +
//                "avg(b.LteScRSRP) as LteScRSRP, count(*) as reportNum, count(distinct ecgi) as scellNum " +
//                "from GridInfo a join MRO b " +
//                "on a.id=b.id " +
//                "group by a.id ")

        //Scell维度  round(CAST(avg(LteScRSRP) AS decimal(10,2)),2)
        val df_query_detail_scell = spark.sql("select id, ecgi, first(LteScEarfcn) as LteScEarfcn, first(LteScPci) as LteScPci, " +
                "first(xIndex) as xIndex, first(yIndex) as yIndex, count(distinct reportid) as reportNum, " +
                "round(avg(LteScRSRP), 1) as LteScRSRP, round(avg(LteScRSRQ), 1) as LteScRSRQ " +
                "from MRO " +
                "group by id, ecgi")
        df_query_detail_scell.cache()
        //Ncell维度
        val df_query_detail_ncell = spark.sql("select id, LteNcEarfcn, LteNcPci, first(xIndex) as xIndex, first(yIndex) as yIndex, count(*) as reportNum, " +
                "round(avg(LteNcRSRP), 1) as LteNcRSRP, round(avg(LteNcRSRQ), 1) as LteNcRSRQ " +
                "from MRO " +
                "group by id, LteNcEarfcn, LteNcPci")
        df_query_detail_ncell.cache()
        df_query_detail_scell.createOrReplaceTempView("MRO_Scell")
        df_query_detail_ncell.createOrReplaceTempView("MRO_Ncell")

        //compute ncell's ecgi
        val df_ncell_withecgi = spark.sql("select a.*, b.ecgi, 'Scell匹配' as matchType " +
                "from MRO_Ncell a left join MRO_Scell b " +
                "on a.id=b.id and a.LteNcEarfcn=b.LteScEarfcn and a.LteNcPci=b.LteScPci")
        df_ncell_withecgi.cache()
        df_ncell_withecgi.createOrReplaceTempView("Ncell_ecgi")
        val df_ncell_withecgi_final = spark.sql("select id, LteNcEarfcn, LteNcPci, reportNum, LteNcRSRP, LteNcRSRQ, ecgi, '最近小区匹配' as matchType " +
                "from (select *, row_number() OVER (PARTITION BY id,LteNcEarfcn,LteNcPci ORDER BY distance) rank " +
                "from (select a.id, a.LteNcEarfcn, a.LteNcPci, a.reportNum, a.LteNcRSRP, a.LteNcRSRQ, " +
                "b.ecgi, (a.xIndex-b.xIndex)*(a.xIndex-b.xIndex)+(a.yIndex-b.yIndex)*(a.yIndex-b.yIndex) as distance " +
                "from (select * from Ncell_ecgi where ecgi is null) a left join siteLte b " +
                "on a.LteNcEarfcn=b.earfcn and a.LteNcPci=b.pci) c) tmp where rank <= 1").union(df_ncell_withecgi.na.drop().drop("xIndex", "yIndex"))

//        val df_ncell_withecgi = spark.sql("select a.id, a.LteNcEarfcn, a.LteNcPci, a.reportNum, a.LteNcRSRP, a.LteNcRSRQ, b.ecgi " +
//                "from MRO_Ncell a left join siteLte b " +
//                "on abs(a.xIndex-b.xIndex)<3 and abs(a.yIndex-b.yIndex)<3 and a.LteNcEarfcn=b.earfcn and a.LteNcPci=b.pci").distinct()

        //write to mysql, Ncell table
        df_ncell_withecgi_final.write.mode("overwrite").format("jdbc").options(
            Map(
                "driver" -> "com.mysql.jdbc.Driver",
                "url" -> url,
                "dbtable" -> dbtable_ncell,
                "user" -> "root",
                "password" -> "mongs",
                "batchsize" -> "1000")).save()
        //write to mysql, Scell table
        df_query_detail_scell.drop("xIndex", "yIndex").write.mode("overwrite").format("jdbc").options(
            Map(
                "driver" -> "com.mysql.jdbc.Driver",
                "url" -> url,
                "dbtable" -> dbtable_scell,
                "user" -> "root",
                "password" -> "mongs",
                "batchsize" -> "1000")).save()

        val df_query_aggre = spark.sql("select a.id, first(a.latitudeMin) as latitudeMin, first(a.latitudeMax) as latitudeMax, first(a.longitudeMin) as longitudeMin, first(a.longitudeMax) as longitudeMax, " +
                "sum(b.reportNum) as reportNum, count(distinct b.ecgi) as scellNum " +
                "from GridInfo a join MRO_Scell b " +
                "on a.id=b.id " +
                "group by a.id ")
//        df_query_aggre = df_query_aggre.withColumn("LteScRSRP", df_query_aggre("LteScRSRPsum")/df_query_aggre("reportNum")).drop("LteScRSRPsum")

        //write to mysql, aggreation table
        df_query_aggre.write.mode("overwrite").format("jdbc").options(
            Map(
                "driver" -> "com.mysql.jdbc.Driver",
                "url" -> url,
                "dbtable" -> dbtable_aggre,
                "user" -> "root",
                "password" -> "mongs",
                "batchsize" -> "1000")).save()

        val df_scell_avg = spark.sql("select id, LteScEarfcn, round(sum(LteScRSRP*reportNum)/sum(reportNum), 1) as avgRSRP, " +
                "round(sum(LteScRSRQ*reportNum)/sum(reportNum), 1) as avgRSRQ, sum(reportNum) as reportNumSum " +
                "from MRO_Scell " +
                "group by id, LteScEarfcn")

        //write to mysql, scell_avg table
        df_scell_avg.write.mode("overwrite").format("jdbc").options(
            Map(
                "driver" -> "com.mysql.jdbc.Driver",
                "url" -> url,
                "dbtable" -> dbtable_scell_avg,
                "user" -> "root",
                "password" -> "mongs",
                "batchsize" -> "1000")).save()

        val df_ncell_avg = spark.sql("select id, LteNcEarfcn, round(sum(LteNcRSRP*reportNum)/sum(reportNum), 1) as avgRSRP, " +
                "round(sum(LteNcRSRQ*reportNum)/sum(reportNum), 1) as avgRSRQ, sum(reportNum) as reportNumSum " +
                "from MRO_Ncell " +
                "group by id, LteNcEarfcn")

        //write to mysql, ncell_avg table
        df_ncell_avg.write.mode("overwrite").format("jdbc").options(
            Map(
                "driver" -> "com.mysql.jdbc.Driver",
                "url" -> url,
                "dbtable" -> dbtable_ncell_avg,
                "user" -> "root",
                "password" -> "mongs",
                "batchsize" -> "1000")).save()

        //write csv to hdfs
//        df_query_aggre.orderBy("id").show(100)
//        df_query_aggre.orderBy("id")
//                .repartition(1).write.mode("overwrite")
//                .format("csv").option("header", true)
//                .save("hdfs://gdb-nt189:8020/result/MRO_Grid.csv")

        spark.sqlContext.uncacheTable("MRO")
        spark.sqlContext.dropTempTable("MRO")

        println("execute finished")
        spark.stop()
    }
}
