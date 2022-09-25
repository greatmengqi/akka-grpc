/*
 * Copyright (C) 2018-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.grpc.scaladsl

import akka.actor.ActorSystem
import example.myapp.student.grpc.studenthttp._
import scala.concurrent.Future

class PowerStudentServiceImpl()(implicit system: ActorSystem) extends HttpStudentServicePowerApi {
  override def getStudent(in: StudentRequest, metadata: Metadata): Future[StudentReply] = {
    println(system)
    Future.successful(StudentReply(name = in.name, age = in.age, isStudent = in.isStudent, weight = in.weight))
  }

}
