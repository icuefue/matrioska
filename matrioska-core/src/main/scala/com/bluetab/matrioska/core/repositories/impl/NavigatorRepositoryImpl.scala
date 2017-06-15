package com.bluetab.matrioska.core.repositories.impl

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout.durationToTimeout
import com.bluetab.matrioska.core.beans.Metainfo
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreContext}
import com.bluetab.matrioska.core.repositories.NavigatorRepository
import com.bluetab.matrioska.core.repositories.impl.GovernanceRepositoryProtocol.MetainfoDao
import org.json4s.{DefaultFormats, Formats}
import spray.can.Http
import spray.can.Http.HostConnectorSetup
import spray.client.pipelining._
import spray.http._
import spray.httpx.Json4sSupport
import spray.util.pimpFuture

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}


object GovernanceRepositoryProtocol extends Json4sSupport {

  override implicit def json4sFormats: Formats = DefaultFormats

  case class MetainfoDao(identity: String, originalName: String, originalDescription: String)

}

class NavigatorRepositoryImpl extends NavigatorRepository {

  def findMetainfoObject(desObject: String, sourceType: String, parentPath: String): Option[Metainfo] = {

    implicit val system = ActorSystem()
    implicit val sslContext = CoreContext.sslContext
    implicit val clientSSLEngineProvider = CoreContext.clientSSLEngineProvider
    IO(Http) ! HostConnectorSetup(host = CoreConfig.navigator.host,
      port = CoreConfig.navigator.port, sslEncryption = true)
    val pipeline: HttpRequest => Future[List[MetainfoDao]] = (
      addHeader("Navigator", "navigator")
      ~> addCredentials(BasicHttpCredentials(CoreConfig.navigator.username, CoreConfig.navigator.password))
      ~> sendReceive
      ~> unmarshal[List[MetainfoDao]])

    val responseFuture: Future[List[MetainfoDao]] = pipeline {
      val params = Map("query" -> s"((originalName:$desObject)AND(sourceType:$sourceType)AND(deleted:FALSE)AND(parentPath:$parentPath))",
        "limit" -> "1000000", "offset" -> "0")
      Get(Uri(CoreConfig.navigator.uri).withQuery(params))
    }
    val metainfoList = Await.result(responseFuture, 3.seconds)
    val metainfoDao = metainfoList.head

    IO(Http).ask(Http.CloseAll)(3.second).await
    system.shutdown()

    Some(Metainfo(metainfoDao.identity, metainfoDao.originalName, metainfoDao.originalDescription))

  }

  override def putMetainfoObject(objectId: String, values: scala.collection.immutable.Map[String, String]): Boolean = {

    if(values.nonEmpty) {
      // Importamos el dispatcher
      implicit val system = ActorSystem()

      // Configuramos la conexión a navigator
      implicit val sslContext = CoreContext.sslContext
      implicit val clientSSLEngineProvider = CoreContext.clientSSLEngineProvider
      IO(Http) ! HostConnectorSetup(host = CoreConfig.navigator.host, port = CoreConfig.navigator.port, sslEncryption = true)
      val pipeline: HttpRequest => Future[MetainfoDao] = (
        addHeader("Navigator", "navigator")
          ~> addCredentials(BasicHttpCredentials(CoreConfig.navigator.username, CoreConfig.navigator.password))
          ~> sendReceive
          ~> unmarshal[MetainfoDao]) // Esperamos como respuesta un MetainfoDao

      var data = "{"
      values.foreach(value =>
        data = data.concat("\"" + value._1 + "\":\"" + value._2 + "\",")
      )
      data = data.substring(0, data.length - 1).concat("}")
      CoreContext.logger.info(s"data: $data")

      val responseFuture: Future[MetainfoDao] = pipeline{
        Put(Uri(s"${CoreConfig.navigator.uri}$objectId")).withEntity(HttpEntity(ContentTypes.`application/json`, data))}

      val response = Await.result(responseFuture, 3.seconds)
      CoreContext.logger.info(s"identity: ${response.identity}, " +
        s"originalName: ${response.originalName}, " +
        s"originalDescription: ${response.originalDescription}")

      IO(Http).ask(Http.CloseAll)(3.second).await
      system.shutdown()
      true
    }
    else {
      false
    }

  }
}
