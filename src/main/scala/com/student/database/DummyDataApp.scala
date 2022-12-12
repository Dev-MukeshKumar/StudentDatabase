package com.student.database

import data.generate.MainTable._
import data.generate.MetaTable._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

object DummyDataApp extends Serializable {
  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Started DummyDataApp!")

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()
    import spark.implicits._

    val studentsDataList = studentsTableList()
    val marksDataList = marksTableList(studentsDataList)

    //meta tables dataset generate
    val groupsDS = spark.sparkContext.parallelize(groups()).toDS()
    val subjectsDS = spark.sparkContext.parallelize(subjects()).toDS()
    val classesDS = spark.sparkContext.parallelize(classes()).toDS()
    val testsDS = spark.sparkContext.parallelize(tests()).toDS()
    val classGroupMapperDS = spark.sparkContext.parallelize(classGroupMapper()).toDS()
    val subjectGroupMapperDS = spark.sparkContext.parallelize(subjectGroupMapper).toDS()

    groupsDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "groups"))
      .mode("append")
      .save()

    subjectsDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "subjects"))
      .mode("append")
      .save()

    classesDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "classes"))
      .mode("append")
      .save()

    testsDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "tests"))
      .mode("append")
      .save()

    classGroupMapperDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "class_group_mapper"))
      .mode("append")
      .save()

    subjectGroupMapperDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "subject_group_mapper"))
      .mode("append")
      .save()

    val studentsDataDS = spark.sparkContext.parallelize(studentsDataList).toDS()
    val marksDataDS = spark.sparkContext.parallelize(marksDataList).toDS()

    studentsDataDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "students"))
      .mode("append")
      .save()

    marksDataDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "marks"))
      .mode("append")
      .save()

    spark.stop()
    logger.info("Stopped DummyDataApp!")

  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf
    val props = new Properties
    props.load(Source.fromFile("spark.conf").reader())
    props.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }

}
