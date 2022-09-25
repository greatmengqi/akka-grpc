package akka.grpc.scaladsl.httpapi

import akka.grpc.internal.{ Codecs, GrpcProtocolNative, Identity }
import akka.grpc.scaladsl.headers.`Message-Accept-Encoding`
import akka.grpc.scaladsl.httpapi.HttpHandler._
import akka.grpc.{ GrpcProtocol, ProtobufSerializer }
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling
import akka.http.scaladsl.model.StatusCodes.ClientError
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.parboiled2.util.Base64
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.util.ByteString
import akka.{ ConfigurationException, NotUsed }
import com.google.api.http.HttpRule
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors._
import com.google.protobuf.any.{ Any => ProtobufAny }
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.{ DynamicMessage, ListValue, MessageOrBuilder, Struct, Value }

import java.lang.{ Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong }
import java.net.URLDecoder
import java.util.regex._
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

final class HttpHandler(methDesc: MethodDescriptor, rule: HttpRule, grpcHandler: HttpRequest => Future[HttpResponse])(
    implicit ec: ExecutionContext,
    mat: Materializer)
    extends PartialFunction[HttpRequest, Future[HttpResponse]] {

  private[this] final val (methodPattern, pathTemplate, pathExtractor, bodyDescriptor, responseBodyDescriptor) =
    extractAndValidate()

  private type PathParameterEffect = (FieldDescriptor, Option[Any]) => Unit
  private type ExtractPathParameters = (Matcher, PathParameterEffect) => Unit

  private[this] final def extractAndValidate() = {

    def parsePathExtractor(pattern: String): (PathTemplateParser.ParsedTemplate, ExtractPathParameters) = {
      val template = PathTemplateParser.parse(pattern)
      val pathFieldParsers = template.fields.iterator
        .map {
          case tv @ PathTemplateParser.TemplateVariable(fieldName :: Nil, _) =>
            lookupFieldByName(methDesc.getInputType, fieldName) match {
              case null =>
                configError(
                  s"Unknown field name [$fieldName] in type [${methDesc.getInputType.getFullName}] reference in path template for method [${methDesc.getFullName}]")
              case field =>
                if (field.isRepeated)
                  configError(s"Repeated parameters [${field.getFullName}] are not allowed as path variables")
                else if (field.isMapField)
                  configError(s"Map parameters [${field.getFullName}] are not allowed as path variables")
                else (tv, field, FieldMsgDecodeFunction(field, configError))
            }
          case multi =>
            // todo implement field paths properly
            configError("Multiple fields in field path not yet implemented: " + multi.fieldPath.mkString("."))
        }
        .zipWithIndex
        .toList

      (
        template,
        (matcher, effect) => {
          pathFieldParsers.foreach {
            case ((_, field, parser), idx) =>
              val rawValue = matcher.group(idx + 1)
              // When encoding, we need to be careful to only encode / if it's a single segment variable. But when
              // decoding, it doesn't matter, we decode %2F if it's there regardless.
              val decoded = URLDecoder.decode(rawValue, "utf-8")
              val value = parser(decoded)
              effect(field, value)
          }
        })
    }

    // Validate selector
    if (rule.selector != "" && rule.selector != methDesc.getFullName)
      configError(s"Rule selector [${rule.selector}] must be empty or [${methDesc.getFullName}]")

    // Validate pattern
    val (methodPattern, pattern) = {
      import HttpRule.Pattern._
      import akka.http.scaladsl.model.HttpMethods._

      rule.pattern match {
        case Empty           => configError(s"Pattern missing for rule [$rule]!") // TODO improve error message
        case Get(pattern)    => (GET, pattern)
        case Put(pattern)    => (PUT, pattern)
        case Post(pattern)   => (POST, pattern)
        case Delete(pattern) => (DELETE, pattern)
        case Patch(pattern)  => (PATCH, pattern)
        case Custom(chp) =>
          if (chp.kind == "*")
            (ANY_METHOD, chp.path) // FIXME is "path" the same as "pattern" for the other kinds? Is an empty kind valid?
          else configError(s"Only Custom patterns with [*] kind supported but [${chp.kind}] found!")
      }
    }
    val (template, pathParameters) = parsePathExtractor(pattern)

    // Validate body value
    val bodyDescriptor = rule.body match {
      case "" => methDesc.getInputType
      case "*" =>
        if (!methodPattern.isEntityAccepted)
          configError(s"Body configured to [*] but HTTP Method [$methodPattern] does not have a request body.")
        else
          methDesc.getInputType
      case fieldName =>
        val field = lookupFieldByName(methDesc.getInputType, fieldName)
        if (field == null)
          configError(s"Body configured to [$fieldName] but that field does not exist on input type.")
        else if (field.isRepeated)
          configError(s"Body configured to [$fieldName] but that field is a repeated field.")
        else if (!methodPattern.isEntityAccepted)
          configError(s"Body configured to [$fieldName] but HTTP Method $methodPattern does not have a request body.")
        else {
          // body must be message
          field.getMessageType
        }
    }

    // Validate response body value
    val responseBodyDescriptor =
      rule.responseBody match {
        case "" => None
        case fieldName =>
          lookupFieldByName(methDesc.getOutputType, fieldName) match {
            case null =>
              configError(
                s"Response body field [$fieldName] does not exist on type [${methDesc.getOutputType.getFullName}]")
            case field => Some(field)
          }
      }

    if (rule.additionalBindings.exists(_.additionalBindings.nonEmpty))
      configError(s"Only one level of additionalBindings supported, but [$rule] has more than one!")

    (methodPattern, template, pathParameters, bodyDescriptor, responseBodyDescriptor)
  }

  override def isDefinedAt(req: HttpRequest): Boolean = {
    def matches(path: Uri.Path): Boolean =
      pathTemplate.regex.pattern.matcher(path.toString()).matches()
    (methodPattern == ANY_METHOD || req.method == methodPattern) && matches(req.uri.path)
  }

  override def apply(req: HttpRequest): Future[HttpResponse] = {
    assert((methodPattern == ANY_METHOD || req.method == methodPattern))
    val pathTemplateMatcher = pathTemplate.regex.pattern.matcher(req.uri.path.toString())
    assert(pathTemplateMatcher.matches())
    handlerRequest(req, pathTemplateMatcher)
  }

  private[this] final def handlerRequest(req: HttpRequest, pathTemplateMatcher: Matcher): Future[HttpResponse] = {
    transformRequest(req, pathTemplateMatcher)
      .transformWith {
        case Success(request) =>
          val response = grpcHandler(request).map { resp =>
            val headers = resp.headers
            val grpcReader = GrpcProtocolNative.newReader(Codecs.detect(resp).get)
            val body = resp.entity.dataBytes.viaMat(grpcReader.dataFrameDecoder)(Keep.none).map { payload =>
              ProtobufAny(
                typeUrl = "type.googleapis.com/" + methDesc.getOutputType.getFullName,
                value = com.google.protobuf.ByteString.copyFrom(payload.asByteBuffer))
            }
            headers.toList -> body
          }
          transformResponse(request, response)
        case Failure(e) =>
          requestError(e.getMessage)
      }
      .recover {
        case ire: IllegalRequestException => HttpResponse(ire.status.intValue, entity = ire.status.reason)
        case NonFatal(error)              => HttpResponse(StatusCodes.InternalServerError, entity = error.getMessage)
      }
  }

  final def transformRequest(req: HttpRequest, pathTemplateMatcher: Matcher): Future[HttpRequest] = {
    val jsonParser =
      JsonFormat.parser
        .ignoringUnknownFields()
        .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder().add(bodyDescriptor).build)

    if (rule.body.nonEmpty && req.entity.contentType != ContentTypes.`application/json`) {
      Future.failed(IllegalRequestException(StatusCodes.BadRequest, "Content-type must be application/json!"))
    } else {
      val inputBuilder = DynamicMessage.newBuilder(methDesc.getInputType)
      rule.body match {
        case "" => // Iff empty body rule, then only query parameters
          req.discardEntityBytes()
          parseRequestParametersInto(req.uri.query().toMultiMap, inputBuilder)
          parsePathParametersInto(pathTemplateMatcher, inputBuilder)
          Future.successful(updateRequest(req, inputBuilder.build))
        case "*" => // Iff * body rule, then no query parameters, and only fields not mapped in path variables
          Unmarshal(req.entity)
            .to[String]
            .map(str => {
              jsonParser.merge(str, inputBuilder)
              parsePathParametersInto(pathTemplateMatcher, inputBuilder)
              updateRequest(req, inputBuilder.build)
            })
        case fieldName => // Iff fieldName body rule, then all parameters not mapped in path variables
          Unmarshal(req.entity)
            .to[String]
            .map(str => {
              val subField = lookupFieldByName(methDesc.getInputType, fieldName)
              val subInputBuilder = DynamicMessage.newBuilder(subField.getMessageType)
              jsonParser.merge(str, subInputBuilder)
              parseRequestParametersInto(req.uri.query().toMultiMap, inputBuilder)
              parsePathParametersInto(pathTemplateMatcher, inputBuilder)
              inputBuilder.setField(subField, subInputBuilder.build())
              updateRequest(req, inputBuilder.build)
            })
      }
    }
  }

  // Question: Do we need to handle conversion from JSON names?
  private[this] final def lookupFieldByName(desc: Descriptor, selector: String): FieldDescriptor =
    desc.findFieldByName(selector) // TODO potentially start supporting path-like selectors with maximum nesting level?

  private[this] final def parseRequestParametersInto(
      query: Map[String, List[String]],
      inputBuilder: DynamicMessage.Builder): Unit = {
    def lookupFieldByPath(desc: Descriptor, selector: String): FieldDescriptor =
      Names.splitNext(selector) match {
        case ("", "")        => null
        case (fieldName, "") => lookupFieldByName(desc, fieldName)
        case (fieldName, next) =>
          val field = lookupFieldByName(desc, fieldName)
          if (field == null) null
          else if (field.getMessageType == null) null
          else lookupFieldByPath(field.getMessageType, next)
      }

    query.foreach {
      case (selector, values) =>
        if (values.nonEmpty) {
          lookupFieldByPath(methDesc.getInputType, selector) match {
            case null => requestError("Query parameter [$selector] refers to non-existant field")
            case field if field.getJavaType == FieldDescriptor.JavaType.MESSAGE =>
              requestError(
                "Query parameter [$selector] refers to a message type"
              ) // FIXME validate assumption that this is prohibited
            case field if !field.isRepeated && values.size > 1 =>
              requestError("Multiple values sent for non-repeated field by query parameter [$selector]")
            case field => // FIXME verify that we can set nested fields from the inputBuilder type
              val x = FieldMsgDecodeFunction(field, requestError)
              if (field.isRepeated) {
                values.foreach { v =>
                  inputBuilder.addRepeatedField(
                    field,
                    x(v).getOrElse(requestError("Malformed Query parameter [$selector]")))
                }
              } else
                inputBuilder.setField(
                  field,
                  x(values.head).getOrElse(requestError("Malformed Query parameter [$selector]")))
          }
        } // Ignore empty values
    }
  }

  private[this] final def parsePathParametersInto(matcher: Matcher, inputBuilder: DynamicMessage.Builder): Unit =
    pathExtractor(
      matcher,
      (field, value) =>
        inputBuilder.setField(field, value.getOrElse(requestError("Path contains value of wrong type!"))))

  private[this] final def updateRequest(req: HttpRequest, message: DynamicMessage): HttpRequest = {
    HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(path = Path / methDesc.getService.getFullName / methDesc.getName),
      headers = req.headers :+ new `Message-Accept-Encoding`("identity"),
      entity = HttpEntity.Chunked(
        ContentTypes.`application/grpc+proto`,
        Source.single(
          GrpcProtocolNative
            .newWriter(Identity)
            .encodeFrame(GrpcProtocol.DataFrame(ByteString.fromArrayUnsafe(message.toByteArray))))),
      protocol = HttpProtocols.`HTTP/2.0`)
  }

  final def transformResponse(
      grpcRequest: HttpRequest,
      futureResponse: Future[(List[HttpHeader], Source[ProtobufAny, NotUsed])]): Future[HttpResponse] = {
    def extractContentTypeFromHttpBody(entityMessage: MessageOrBuilder): ContentType =
      Option(entityMessage.getField(entityMessage.getDescriptorForType.findFieldByName("content_type"))) match {
        case None | Some("") =>
          ContentTypes.NoContentType
        case Some(string: String) =>
          ContentType
            .parse(string)
            .fold(
              list =>
                throw new IllegalResponseException(
                  list.headOption.getOrElse(ErrorInfo.fromCompoundString("Unknown error"))),
              identity)
        case _ =>
          throw new IllegalResponseException(ErrorInfo.fromCompoundString("Unknown error"))
      }

    def extractDataFromHttpBody(entityMessage: MessageOrBuilder): ByteString =
      ByteString.fromArrayUnsafe(
        entityMessage
          .getField(entityMessage.getDescriptorForType.findFieldByName("data"))
          .asInstanceOf[com.google.protobuf.ByteString]
          .toByteArray)

    def parseResponseBody(pbAny: ProtobufAny): MessageOrBuilder = {
      val bytes = ReplySerializer.serialize(pbAny)
      val message = DynamicMessage.parseFrom(methDesc.getOutputType, bytes.iterator.asInputStream)
      responseBodyDescriptor.fold(message: MessageOrBuilder) { field =>
        message.getField(field) match {
          case m: MessageOrBuilder if !field.isRepeated =>
            m // No need to wrap this
          case value =>
            responseBody(field.getJavaType, value, field.isRepeated)
        }
      }
    }

    def responseBody(jType: JavaType, value: AnyRef, repeated: Boolean): com.google.protobuf.Value = {
      val result =
        if (repeated) {
          Value.newBuilder.setListValue(
            ListValue.newBuilder.addAllValues(
              value.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map(v => responseBody(jType, v, false)).asJava))
        } else {
          val b = Value.newBuilder
          jType match {
            case JavaType.BOOLEAN =>
              b.setBoolValue(value.asInstanceOf[JBoolean])
            case JavaType.BYTE_STRING =>
              b.setStringValueBytes(value.asInstanceOf[com.google.protobuf.ByteString])
            case JavaType.DOUBLE =>
              b.setNumberValue(value.asInstanceOf[JDouble])
            case JavaType.ENUM =>
              b.setStringValue(
                value.asInstanceOf[EnumValueDescriptor].getName
              ) // Switch to getNumber if enabling printingEnumsAsInts in the JSON Printer
            case JavaType.FLOAT =>
              b.setNumberValue(value.asInstanceOf[JFloat].toDouble)
            case JavaType.INT =>
              b.setNumberValue(value.asInstanceOf[JInteger].toDouble)
            case JavaType.LONG =>
              b.setNumberValue(value.asInstanceOf[JLong].toDouble)
            case JavaType.MESSAGE =>
              val sb = Struct.newBuilder
              value
                .asInstanceOf[MessageOrBuilder]
                .getAllFields
                .forEach((k, v) =>
                  sb.putFields(
                    k.getJsonName,
                    responseBody(k.getJavaType, v, k.isRepeated)
                  ) //Switch to getName if enabling preservingProtoFieldNames in the JSON Printer
                )
              b.setStructValue(sb)
            case JavaType.STRING =>
              b.setStringValue(value.asInstanceOf[String])
          }
        }
      result.build()
    }

    val jsonPrinter = JsonFormat.printer
      .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder.add(methDesc.getOutputType).build())
      .includingDefaultValueFields()
      .omittingInsignificantWhitespace()

    if (methDesc.isServerStreaming) {
      val sseAccepted =
        grpcRequest
          .header[Accept]
          .exists(_.mediaRanges.exists(_.value.startsWith(MediaTypes.`text/event-stream`.toString)))

      futureResponse.flatMap {
        case (headers, data) =>
          if (sseAccepted) {
            import EventStreamMarshalling._
            Marshal(data.map(parseResponseBody).map { em =>
              ServerSentEvent(jsonPrinter.print(em))
            }).to[HttpResponse].map(response => response.withHeaders(headers))
          } else {
            Future.successful(
              HttpResponse(
                entity = HttpEntity.Chunked(
                  ContentTypes.`application/json`,
                  data
                    .map(parseResponseBody)
                    .map(em => HttpEntity.Chunk(ByteString(jsonPrinter.print(em)) ++ NEWLINE_BYTES))),
                headers = headers))
          }
      }
    } else {
      for {
        (headers, data) <- futureResponse
        protobuf <- data.runWith(Sink.head)
      } yield {
        val entityMessage = parseResponseBody(protobuf)
        HttpResponse(
          entity = if (responseBodyDescriptor.nonEmpty) {
            val contentType = extractContentTypeFromHttpBody(entityMessage)
            val body = extractDataFromHttpBody(entityMessage)
            HttpEntity(contentType, body)
          } else {
            HttpEntity(ContentTypes.`application/json`, ByteString(jsonPrinter.print(entityMessage)))
          },
          headers = headers)
      }
    }
  }
}

object HttpHandler {

  private final val NEWLINE_BYTES = ByteString('\n')

  // This is used to support the "*" custom pattern
  private final val ANY_METHOD = HttpMethod.custom(
    name = "ANY",
    safe = false,
    idempotent = false,
    requestEntityAcceptance = RequestEntityAcceptance.Tolerated)

  private final object ReplySerializer extends ProtobufSerializer[ProtobufAny] {
    override final def serialize(reply: ProtobufAny): ByteString =
      if (reply.value.isEmpty) ByteString.empty
      else ByteString.fromArrayUnsafe(reply.value.toByteArray)

    override final def deserialize(bytes: ByteString): ProtobufAny =
      throw new UnsupportedOperationException("operation not supported")
  }

  private object Names {
    final def splitPrev(name: String): (String, String) = {
      val dot = name.lastIndexOf('.')
      if (dot >= 0) {
        (name.substring(0, dot), name.substring(dot + 1))
      } else {
        ("", name)
      }
    }

    final def splitNext(name: String): (String, String) = {
      val dot = name.indexOf('.')
      if (dot >= 0) {
        (name.substring(0, dot), name.substring(dot + 1))
      } else {
        (name, "")
      }
    }
  }

  sealed trait ParserError extends Function[String, Nothing]

  private final object configError extends ParserError {
    def apply(msg: String) = throw new ConfigurationException("HTTP API Config: " + msg)
  }

  private final object requestError extends ParserError {
    def apply(msg: String) = throw IllegalRequestException(
      ClientError(StatusCodes.BadRequest.intValue)(msg, StatusCodes.BadRequest.defaultMessage))
  }

  sealed trait FieldMsgDecodeFunction extends Function[String, Option[Any]]

  object IntDecodeFunction extends FieldMsgDecodeFunction {
    override def apply(s: String): Option[Any] =
      try Option(s.toInt)
      catch {
        case _: Throwable => None
      }
  }

  object LongDecodeFunction extends FieldMsgDecodeFunction {
    override def apply(v1: String): Option[Any] =
      try Option(v1.toLong)
      catch {
        case _: Throwable => None
      }
  }

  object FloatDecodeFunction extends FieldMsgDecodeFunction {
    override def apply(v1: String): Option[Any] =
      try Option(v1.toFloat)
      catch {
        case _: Throwable => None
      }
  }

  object DoubleDecodeFunction extends FieldMsgDecodeFunction {
    override def apply(v1: String): Option[Any] =
      try Option(v1.toDouble)
      catch {
        case _: Throwable => None
      }
  }

  object StringDecodeFunction extends FieldMsgDecodeFunction {
    override def apply(v1: String): Option[Any] = Option(v1)
  }

  object BooleanDecodeFunction extends FieldMsgDecodeFunction {
    override def apply(v1: String): Option[Any] =
      try Option(v1.toBoolean)
      catch {
        case _: Throwable => None
      }
  }

  object ByteStringDecodeFunction extends FieldMsgDecodeFunction {
    override def apply(v1: String): Option[Any] =
      Some(
        com.google.protobuf.ByteString.copyFrom(Base64.rfc2045.decode(v1))
      ) // Make cheaper? Protobuf has a Base64 decoder?
  }

  private final object FieldMsgDecodeFunction {
    def apply(field: FieldDescriptor, illegalCallback: ParserError): FieldMsgDecodeFunction = {
      field.getJavaType match {
        case JavaType.BOOLEAN     => BooleanDecodeFunction
        case JavaType.BYTE_STRING => ByteStringDecodeFunction
        case JavaType.DOUBLE      => DoubleDecodeFunction
        case JavaType.ENUM        => illegalCallback("Enum path parameters not supported!")
        case JavaType.FLOAT       => FloatDecodeFunction
        case JavaType.INT         => IntDecodeFunction
        case JavaType.LONG        => LongDecodeFunction
        case JavaType.MESSAGE     => illegalCallback("Message path parameters not supported!")
        case JavaType.STRING      => StringDecodeFunction
      }
    }
  }

}
