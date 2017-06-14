package com.bbva.ebdm.linx.core.conf

import com.bbva.ebdm.linx.core.repositories.DFSRepository
import com.bbva.ebdm.linx.core.repositories.FSRepository
import com.bbva.ebdm.linx.core.repositories.HBaseRepository
import com.bbva.ebdm.linx.core.repositories.HiveRepository
import com.bbva.ebdm.linx.core.repositories.ImpalaRepository
import com.bbva.ebdm.linx.core.repositories.JsonRepository
import com.bbva.ebdm.linx.core.repositories.NavigatorRepository
import com.bbva.ebdm.linx.core.repositories.RdbmsRepository
import com.bbva.ebdm.linx.core.repositories.MailRepository
import com.bbva.ebdm.linx.core.repositories.SolrRepository
import com.bbva.ebdm.linx.core.repositories.impl.DFSRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.FSRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.HBaseRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.HiveRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.ImpalaRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.JsonRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.NavigatorRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.RdbmsRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.MailRepositoryImpl
import com.bbva.ebdm.linx.core.repositories.impl.SolrRepositoryImpl






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
