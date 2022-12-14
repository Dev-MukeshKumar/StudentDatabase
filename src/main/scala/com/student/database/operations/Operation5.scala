package com.student.database.operations

import com.datastax.spark.connector.toRDDFunctions
import data.models._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import java.time.LocalDate

object Operation5 {

  def executeOperation5(
                         spark: SparkSession,
                         logger: Logger): Unit
  = {

    val studentToDelete = getStudent(spark,logger)
    println("Student data to be moved to history:")
    studentToDelete.show()

    val studentData = studentToDelete.take(1)(0)

    val studentId = studentData.student_id.getOrElse(0)
    val classId = studentData.class_id.getOrElse(0)
    val groupId = studentData.group_id.getOrElse(0)

    val marks = getMarks(spark,logger,classId,groupId,studentId)

    println("marks going to be moved to history:")
    marks.show()

    val time = LocalDate.now()
    import spark.implicits._
    val studentHistoryData = spark.sparkContext.parallelize(Seq(StudentHistory(studentData.group_id,studentData.class_id,Option(time),studentData.student_id,studentData.firstname,studentData.lastname))).toDS()
    studentHistoryData.show()

    studentHistoryData.rdd.deleteFromCassandra("assignment2","students")

    marks.rdd.deleteFromCassandra("assignment2","students")

    studentHistoryData.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "students_history"))
      .mode("append")
      .save()

    marks.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "marks_history"))
      .mode("append")
      .save()
  }

  @tailrec
  private def getStudent(spark: SparkSession, logger: Logger, id: Int=getStudentId()): Dataset[Student] = {
    import spark.implicits._

    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "students"))
      .load
      .where(s"student_id=$id")
      .as[Student]

    if (readData.count() == 0) {
      logger.info(s"Student with ID: $id not found!")
      getStudent(spark,logger,getStudentId())
    }
    else readData
  }

  @tailrec
  private def getStudentId(): Int = {
    print("Student ID: ")
    val pinCode = Try(readInt())
    pinCode match {
      case Success(value) if value >= 1 && value <= 900 => value
      case Success(value) if !value.toString.matches("^[1-9][0-9]{5}$") => {
        println("Please enter an ID between 1 to 900!")
        getStudentId()
      }
      case Failure(exception) => {
        println("Please enter numbers only!")
        getStudentId()
      }
    }
  }

  def getMarks(spark: SparkSession, logger: Logger, classId: Int,groupId:Int,studentId:Int): Dataset[Mark] = {
    import spark.implicits._
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "marks"))
      .load
      .where(s"group_id = $groupId and class_id = $classId and student_id=$studentId")
      .as[Mark]
  }
}
