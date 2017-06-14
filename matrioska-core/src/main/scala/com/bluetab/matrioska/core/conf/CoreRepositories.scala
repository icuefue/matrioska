package com.bluetab.matrioska.core.conf

import com.bluetab.matrioska.core.repositories._
import com.bluetab.matrioska.core.repositories.impl._






/**
 * Repositorios
 */
object CoreRepositories {

  val hiveRepository: HiveRepository = new HiveRepositoryImpl
  val dfsRepository: DFSRepository = new DFSRepositoryImpl
  val hBaseRepository: HBaseRepository = new HBaseRepositoryImpl
  val impalaRepository: ImpalaRepository = new ImpalaRepositoryImpl
  val jsonRepository: JsonRepository = new JsonRepositoryImpl
  val fsRepository: FSRepository = new FSRepositoryImpl
  val rdbmsRepository: RdbmsRepository = new RdbmsRepositoryImpl
  val navigatorRepository: NavigatorRepository = new NavigatorRepositoryImpl
  val mailRepository: MailRepository = new MailRepositoryImpl
  val solrRepository: SolrRepository = new SolrRepositoryImpl

}
