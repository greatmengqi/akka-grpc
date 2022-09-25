/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.grpc.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import example.myapp.student.grpc.studenthttp.HttpStudentServicePowerApiHandler
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class HttpApiSpecNetty extends HttpApiSpec("netty")
class HttpApiSpecAkkaHttp extends HttpApiSpec("akka-http")

abstract class HttpApiSpec(backend: String)
    extends TestKit(ActorSystem(
      "GrpcExceptionHandlerSpec",
      ConfigFactory.parseString(s"""akka.grpc.client."*".backend = "$backend" """).withFallback(ConfigFactory.load())))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with Directives
    with BeforeAndAfter
    with BeforeAndAfterAll {

  override implicit val patienceConfig = PatienceConfig(100.seconds, Span(10, org.scalatest.time.Millis))

  val server =
    Http()
      .newServerAt("localhost", 0)
      .bind(HttpStudentServicePowerApiHandler(new PowerStudentServiceImpl()))
      .futureValue

  override protected def afterAll(): Unit = {
    server.terminate(3.seconds)
    super.afterAll()
  }

  "The power API" should {
    "successfully pass metadata from client to server" in {

      val entity = HttpEntity.apply(
        ContentTypes.`application/json`,
        """
          |{"name":{"head_name":"chen","tail_name":"mike"},"age":123,"is_student":true, "extra":"ok"}
          |""".stripMargin.getBytes())

      val responseFuture =
        Http()
          .singleRequest(Post(uri = s"http://localhost:${server.localAddress.getPort}/student", entity))
          .flatMap(resp => Unmarshal(resp).to[String])

      val futureValue = responseFuture.futureValue

      println("***")
      println(futureValue)
      println("***")
    }

  }

}
