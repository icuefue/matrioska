package com.bluetab.matrioska.core.conf

import com.bluetab.matrioska.core.services.impl.{AuditoryServiceImpl, CommonServiceImpl, GovernmentServiceImpl}
import com.bluetab.matrioska.core.services.{AuditoryService, CommonService, GovernmentService}

/**
 * Servicios disponibles
 */
object CoreServices {

  val governmentService: GovernmentService = new GovernmentServiceImpl
  val auditoryService: AuditoryService = new AuditoryServiceImpl
  val commonService: CommonService = new CommonServiceImpl

}
