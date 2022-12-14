package com.student.database.operations

import data.models._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession, functions}

object Operation3 {

  def executeOperation3(
                         spark: SparkSession,
                         logger: Logger): Unit = {


    val passPercentage = calculatePassPercentage(spark, logger,"1,2")

    passPercentage.show()

    passPercentage.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "pass_percentage"))
      .mode("append")
      .save()
  }

  def calculatePassPercentage(spark:SparkSession,logger: Logger,classIds:String):Dataset[PassPercentage] = {
    val marksListAverageMark = getMarks(spark, logger,classIds)

    import spark.implicits._

    val studentCountList = marksListAverageMark.groupBy("subject_id", "class_id").agg(functions.countDistinct("student_id").as("student_count")).as[StudentCountSubjectClassIdKey]

    val studentsPassCountList = marksListAverageMark.filter(data => data.average > 40).groupBy("subject_id", "class_id").agg(functions.countDistinct("student_id").as("student_pass")).as[StudentPassSubjectClassIdKey]

    val combinedData = studentCountList.join(studentsPassCountList, Seq("subject_id", "class_id"), "fullouter").as[StudentPassCountSubjectClassIdKey]

    combinedData.map(data => PassPercentage(Option(data.class_Id), Option(data.subject_id), Option(((data.student_pass.toDouble / data.student_count.toDouble) * 100)))).as[PassPercentage]
  }

  def getMarks(spark: SparkSession,logger: Logger,classIds:String):Dataset[AverageMark] = {
    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "marks"))
      .load
      .where(s"class_id in ($classIds)")
      .as[Mark]

    readData.map(data => data match {
      case Mark(groupId,classId,subjectId,studentId,Some(marks)) => AverageMark(groupId, classId, subjectId, studentId, marks.values.map(value => value.getOrElse(0.0)).sum/marks.values.size)
    })
  }

}
