package com.bluetab.matrioska.core.repositories.impl

import com.bluetab.matrioska.core.conf.{CoreConfig, CoreContext, CoreRepositories}
import com.bluetab.matrioska.core.constants.CoreConstants
import com.bluetab.matrioska.core.repositories.SolrRepository

import scala.sys.process._

/**
  * Created by xe54068 on 23/01/2017.
  */
class SolrRepositoryImpl extends SolrRepository {

  override def deleteCollection(name: String): Unit = {
    val result = s"solrctl --solr http://lpbig501:8983/solr/ collection --delete $name" !
  }

  override def createCollection(name: String): Unit = {
    val result = s"solrctl --solr http://lpbig501:8983/solr/ collection --create $name" !
  }

  override def loadDictionaryToCollection(name: String): Unit = {
    val dicctionary = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentSelectAllDictionary)

    dicctionary match{
      case Some(rdd) => {
        CoreContext.logger.info(s"Size: ${rdd.collect.length}")
        var sb = new StringBuffer()
        sb.append("\"id\",\"cod_platdic\",\"fec_alconcbd\",\"xti_origen\",\"des_nomcfun\",\"des_confunc\",\"des_corfunc\",\"des_fuenteo\",\"des_calculos\",\"fec_uact\",\"des_frecact\",\"cod_proyto\",\"cod_dofun\",\"cod_sdofun\",\"cod_vigente\",\"aud_usuario\",\"aud_tim\",\"des_ambitobd\"\n")
        rdd.collect.foreach( row => {
          row.toSeq.foreach(entry =>
            if(entry != null)
              sb.append("\"" + entry.toString.replace("\"", "").replace("\n", "") + "\",")
            else
              sb.append("\"NULL\",")

          )
          sb.setLength(sb.length - 1)
          sb.append("\n")
        }
        )
        val diccionarioFilepath = s"${CoreConfig.localfilesystempath.dat}/diccionario_${System.currentTimeMillis}.dat"
        CoreRepositories.fsRepository.createFileWithContent(diccionarioFilepath, sb.toString.toCharArray)

        var result = s"java -Durl=http://lpbig501.igrupobbva:8983/solr/${name}_shard1_replica1/update/csv -jar /usr/local/pr/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/jars/post.jar $diccionarioFilepath" !

        CoreRepositories.fsRepository.deleteFile(diccionarioFilepath)
      }
      case None =>
        CoreContext.logger.info(s"Diccionario vacío")
    }
  }
}
