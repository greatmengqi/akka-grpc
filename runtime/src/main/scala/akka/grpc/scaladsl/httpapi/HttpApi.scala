package akka.grpc.scaladsl.httpapi

import akka.grpc.Options
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.stream.Materializer
import com.google.api.annotations.AnnotationsProto
import com.google.api.http.HttpRule
import com.google.protobuf.Descriptors.{ FileDescriptor, MethodDescriptor }

import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters.asScalaBufferConverter

object HttpApi {

  def serve(fileDescriptor: FileDescriptor, handler: (HttpRequest, String) => Future[HttpResponse])(
      implicit mat: Materializer,
      ec: ExecutionContext): PartialFunction[HttpRequest, Future[HttpResponse]] = {
    val handlers = for {
      service <- fileDescriptor.getServices.asScala
      method <- service.getMethods.asScala
      rules = getRules(method)
      binding <- rules
    } yield {
      new HttpHandler(method, binding, req => handler(req, method.getName))
    }
    handlers.foldLeft(NoMatch) {
      case (NoMatch, first)    => first
      case (previous, current) => current.orElse(previous) // Last goes first
    }
  }

  private def getRules(methDesc: MethodDescriptor): Seq[HttpRule] = {
    AnnotationsProto.http.get(Options.convertMethodOptions(methDesc)) match {
      case Some(rule) =>
        rule +: rule.additionalBindings
      case None =>
        Seq.empty
    }
  }

  private final val NoMatch = PartialFunction.empty[HttpRequest, Future[HttpResponse]]

}
