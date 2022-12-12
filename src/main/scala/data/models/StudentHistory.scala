package data.models

import org.joda.time.DateTime

import java.util.UUID

case class StudentHistory(group_id: Option[Int], class_id: Option[Int], date_of_leaving: Option[DateTime], student_id: Option[UUID], firstname: Option[String], lastname:Option[String])