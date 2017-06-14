package com.bluetab.matrioska.ingest.apps

import java.nio.charset.Charset

import com.bluetab.matrioska.core.conf.{CoreContext, CoreRepositories, CoreServices}
import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreServices}

class MetainfoApp extends LinxApp {

  override def run(args: Seq[String]) {

//    val map = scala.collection.immutable.Map ("name" -> s"nombre para la prueba ${System.currentTimeMillis}", "description" -> s"descripcion para la prueba ${System.currentTimeMillis}")
//    CoreServices.governmentService.putMetainfoObject("c0a4f05244e46f2c9aed14cb744e99f9", map)
    CoreServices.governmentService.recreateSolrDictionaryIndex(CoreConfig.solr.dictionaryCollection)
//    val charset = CoreRepositories.fsRepository.detectFileCharset("/bigdata/workspace/xe54068/ESKYGUENSP_NETCASH_20170123_001.dat_PROCESADO")
//
//    charset match {
//      case Some(charset) => CoreContext.logger.info(s"extraído: ${charset.toString}")
//      case None => CoreContext.logger.info(s"No soportado")
//    }
  }
}
