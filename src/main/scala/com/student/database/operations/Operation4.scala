package com.student.database.operations

import data.models._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.io.StdIn.readLine

object Operation4 {
  def executeOperation4(
                         spark: SparkSession,
                         logger: Logger,
                         classes: Map[Int, String]): Unit = {

    val className = getClassName()
    val classId = getClassId(className,classes)
    val testWiseMarks = getMarksTestWise(spark, logger,classId)

    import spark.implicits._

    val testWiseAverageMark = testWiseMarks.groupBy("student_id","test_id").agg(avg("mark").as("average_marks")).as[(Int,Int,Double)].cache()

    val totalAverageOfStudent = testWiseAverageMark.groupBy("student_id").agg(avg("average_marks").as("total_average")).orderBy(desc("total_average")).as[(Int,Double)]

    val test1AverageMarks = testWiseAverageMark.filter(data => data._2 == 1).orderBy(desc("average_marks"))

    val test2AverageMarks = testWiseAverageMark.filter(data => data._2 == 2).orderBy(desc("average_marks"))

    println(s"Top 3 in test 1 and test 2 of class ${className}")
    test1AverageMarks.show(3)
    test2AverageMarks.show(3)

    println(s"Overall Top 3 in tests of class ${className}")
    totalAverageOfStudent.show(3)

  }

  def getMarksTestWise(spark: SparkSession, logger: Logger,classId:Int): Dataset[MarkTestKey] = {

    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "marks"))
      .load
      .where(s"class_id = ${classId}")
      .as[Mark]

    readData.flatMap(data => data match {
      case Mark(groupId, classId, subjectId, studentId, marks) => {
        marks.get.map( x => x match {
          case (key,Some(value)) => MarkTestKey(groupId.getOrElse(0),classId.getOrElse(0), subjectId.getOrElse(0), studentId.getOrElse(0), key, value)
        })
      }
    })
  }

  @tailrec
  private def getClassName(): String = {
    println(s"Classes list: XI, XII")
    print("Class name: ")
    val className = Try(readLine())
    className match {
      case Success(value) if value.toUpperCase == "XII" || value.toUpperCase == "XI" => value.toUpperCase
      case Success(value) => {
        println("Please enter a valid class from the list!")
        getClassName()
      }
      case Failure(exception) => {
        println("Please enter a valid string data!")
        getClassName()
      }
    }
  }

  @tailrec
  private def getClassId(className:String,classes:Map[Int,String]):Int = {
    val classId = classes.find(_._2 == className).getOrElse((0, ""))._1
    if (classId == 0) getClassId(className,classes) else classId
  }
}
