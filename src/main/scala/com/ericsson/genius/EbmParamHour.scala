package com.ericsson.genius

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueExcludeFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}


object EbmParamHour extends Serializable {
    /*
    get date_id,hour_id and eventTime range(for rowkey scaning) we need
    * */
    def dateHourStr(): (String, String, String, String) = {
        val now:Date = new Date()
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"))
        val hourFormat: SimpleDateFormat = new SimpleDateFormat("H")
        hourFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"))
        val eventTimeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-H")
        eventTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"))
        val cal: Calendar = Calendar.getInstance()
        cal.setTime(now)
        cal.add(Calendar.HOUR, -1)
        val newTime= cal.getTime()
        val startEventTime = eventTimeFormat.parse(eventTimeFormat.format(newTime)).getTime
        cal.add(Calendar.HOUR, 1)
        val endEventTime = eventTimeFormat.parse(eventTimeFormat.format(cal.getTime)).getTime
        (dateFormat.format(newTime), hourFormat.format(newTime), startEventTime.toString, endEventTime.toString)
    }

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("yarn").setAppName("ebm_param_hour")
                .set("spark.kryoserializer.buffer.max", "1g")
                .set("spark.kryoserializer.buffer", "256m")
                .set("spark.executor.memory", "6g")
                .set("spark.driver.memory", "4g")
//                .set("spark.default.parallelism", "60")
                .set("spark.sql.shuffle.partitions", "500")

        val spark: SparkSession = SparkSession.builder
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "gdb-cz29,gdb-cz32,gdb-cz35")

        val hbaseContext = new HBaseContext(spark.sparkContext, conf)
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._

        //get date_id, hour_id (current time minus 1 hour),startEventTime, endEventTime
        val p_time = dateHourStr()
        val p_date_id = p_time._1
        val p_hour_id = p_time._2
        val startEventTime = p_time._3
        val endEventTime = p_time._4
        println("execute_data_time_range:" + p_date_id + "-" + p_hour_id)

        //hbase tablename
        val tableName = List("L_ATTACH", "L_SERVICE_REQUEST", "L_DEDICATED_BEARER_ACTIVATE", "L_TAU")
//        val tableName = List("L_SERVICE_REQUEST")
        //columnFamily
        val hbase_info = "info"
        //columnQualifier
        val info_date_id = "date_id"
        val info_hour_id = "hour_id"
        val info_tai = "tai"
        val info_mme_info = "mme_info"
        val info_result = "result"
        val info_l_service_req_trigger = "l_service_req_trigger"

        //set filters
//        val dateFilter = new SingleColumnValueFilter(Bytes.toBytes(hbase_info), Bytes.toBytes(info_date_id), CompareOp.EQUAL, Bytes.toBytes(p_date_id))
//        val hourFilter = new SingleColumnValueFilter(Bytes.toBytes(hbase_info), Bytes.toBytes(info_hour_id), CompareOp.EQUAL, Bytes.toBytes(p_hour_id))
//        val filters = new FilterList(FilterList.Operator.MUST_PASS_ALL)
//        filters.addFilter(dateFilter)
//        filters.addFilter(hourFilter)

        for(elem <- tableName){
            var df : org.apache.spark.sql.DataFrame = null
            var dftemp : org.apache.spark.sql.DataFrame = null
            //4 servers, 6 regions per server, hbase01 - hbase24
            for(i <- 1 to 24){
                var prefix = ""
                if(i < 10){
                    prefix = "hbase0" + String.valueOf(i)
                }
                else{
                    prefix = "hbase" + String.valueOf(i)
                }
                val scan = new Scan()
                scan.setStartRow(Bytes.toBytes(prefix + "-" + startEventTime))
                scan.setStopRow(Bytes.toBytes(prefix +"-" + endEventTime))
                if(elem.equals("L_SERVICE_REQUEST")){
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_date_id))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_hour_id))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_tai))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_mme_info))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_result))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_l_service_req_trigger))
                }else{
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_date_id))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_hour_id))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_tai))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_mme_info))
                    scan.addColumn(Bytes.toBytes(hbase_info), Bytes.toBytes(info_result))
                }
                scan.setCaching(20000)
                scan.setCacheBlocks(false)

                val hbaseRdd = hbaseContext.hbaseRDD(TableName.valueOf(elem), scan)
                if(elem.equals("L_SERVICE_REQUEST")){
                    dftemp = hbaseRdd.map(r => (
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_date_id))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_hour_id))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_tai))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_mme_info))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_result))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_l_service_req_trigger)))
                    )).toDF("date_id", "hour_id", "tai", "mme_info", "result", "l_service_req_trigger")
                }else{
                    dftemp = hbaseRdd.map(r => (
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_date_id))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_hour_id))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_tai))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_mme_info))),
                            Bytes.toString(r._2.getValue(Bytes.toBytes(hbase_info), Bytes.toBytes(info_result)))
                    )).toDF("date_id", "hour_id", "tai", "mme_info", "result")
                }
                if(i == 1)
                    df = dftemp
                else
                    df = df.union(dftemp)
            }
//            if(elem.equals("L_SERVICE_REQUEST"))
//                println(elem + "_count_all:" + df.count())
            df.createOrReplaceTempView(elem)
        }
//        val df_count = spark.sql("select count(*) from L_SERVICE_REQUEST")
//        println("L_SERVICE_REQUEST:"+df_count.show())

        val df_L_ATTACH = spark.sql("select a.date_id, a.hour_id, 'L_ATTACH' eventName, 'ATTACH成功率' application,b.mme_info, 'tac' as parameter, a.tai as parameterValue, a.totalSample, b.succSample from " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) as tai, count(*) as totalSample from L_ATTACH group by mme_info, substring_index(tai,'-',-1)) a join " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) tai, count(*) as succSample from L_ATTACH where result='SUCCESS' GROUP BY mme_info, substring_index(tai,'-',-1)) b " +
                "on a.mme_info=b.mme_info and a.tai=b.tai")

        val df_L_SERVICE_REQUEST = spark.sql("select a.date_id, a.hour_id, 'L_SERVICE_REQUEST' eventName, 'Service建立成功率' application,b.mme_info, 'tac' as parameter, a.tai as parameterValue, a.totalSample, b.succSample from " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) as tai, count(*) as totalSample from L_SERVICE_REQUEST group by mme_info, substring_index(tai,'-',-1)) a join " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) tai, count(*) as succSample from L_SERVICE_REQUEST where result='SUCCESS' GROUP BY mme_info, substring_index(tai,'-',-1)) b " +
                "on a.mme_info=b.mme_info and a.tai=b.tai")

        val df_L_DEDICATED_BEARER_ACTIVATE = spark.sql("select a.date_id, a.hour_id, 'L_DEDICATED_BEARER_ACTIVATE' eventName, '专载建立成功率' application,b.mme_info, 'tac' as parameter, a.tai as parameterValue, a.totalSample, b.succSample from " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) as tai, count(*) as totalSample from L_DEDICATED_BEARER_ACTIVATE group by mme_info, substring_index(tai,'-',-1)) a join " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) tai, count(*) as succSample from L_DEDICATED_BEARER_ACTIVATE where result='SUCCESS' GROUP BY mme_info,substring_index(tai,'-',-1)) b " +
                "on a.mme_info=b.mme_info and a.tai=b.tai")

        val df_L_TAU = spark.sql("select a.date_id, a.hour_id, 'L_TAU' eventName, 'TAU成功率' application,b.mme_info, 'tac' as parameter, a.tai as parameterValue, a.totalSample, b.succSample from " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) as tai, count(*) as totalSample from L_TAU group by mme_info, substring_index(tai,'-',-1)) a join " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) tai, count(*) as succSample from L_TAU where result='SUCCESS' GROUP BY mme_info, substring_index(tai,'-',-1)) b " +
                "on a.mme_info=b.mme_info and a.tai=b.tai")

        val df_L_SERVICE_REQUEST2 = spark.sql("select a.date_id, a.hour_id, 'L_SERVICE_REQUEST' eventName, '寻呼成功率' application,b.mme_info, 'tac' as parameter, a.tai as parameterValue, a.totalSample, b.succSample from " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) as tai, count(*) as totalSample from L_SERVICE_REQUEST where l_service_req_trigger !='UE' group by mme_info, substring_index(tai,'-',-1)) a join " +
                "(select first(date_id) as date_id, first(hour_id) as hour_id, mme_info, substring_index(tai,'-',-1) tai, count(*) as succSample from L_SERVICE_REQUEST where l_service_req_trigger !='UE' and result='SUCCESS' GROUP BY mme_info, substring_index(tai,'-',-1)) b " +
                "on a.mme_info=b.mme_info and a.tai=b.tai")

        val df_ebm_param_hour = df_L_ATTACH.union(df_L_SERVICE_REQUEST).union(df_L_DEDICATED_BEARER_ACTIVATE).union(df_L_TAU).union(df_L_SERVICE_REQUEST2)

        //save as csv
//        df_ebm_param_hour.repartition(1).write.mode("overwrite")
//                .format("csv").option("header", true)
//                .save("hdfs://100.93.253.106:8020/result/L_ATTACH.csv")

        //insert to mysql directly
        val url : String = args(0)
        val dbtable : String = args(1)
        df_ebm_param_hour.write.mode("append").format("jdbc").options(
            Map(
                "driver" -> "com.mysql.jdbc.Driver",
                //"url" -> "jdbc:mysql://100.93.253.105:3306",
                //"dbtable" -> "test.ebm_param_hour",
                "url" -> url,
                "dbtable" -> dbtable,
                "user" -> "root",
                "password" -> "mongs",
                "batchsize" -> "1000")).save()
        println(p_date_id + "-" + p_hour_id + ":" + "execute finished")
        spark.stop()
    }
}
