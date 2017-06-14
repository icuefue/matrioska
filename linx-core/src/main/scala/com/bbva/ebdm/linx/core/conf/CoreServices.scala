package com.bbva.ebdm.linx.core.conf

import com.bbva.ebdm.linx.core.services.GovernmentService
import com.bbva.ebdm.linx.core.services.AuditoryService
import com.bbva.ebdm.linx.core.services.impl.GovernmentServiceImpl
import com.bbva.ebdm.linx.core.services.impl.AuditoryServiceImpl
import com.bbva.ebdm.linx.core.services.CommonService
import com.bbva.ebdm.linx.core.services.impl.CommonServiceImpl

/**
 * Servicios disponibles
 */
object CoreServices {

  val governmentService: GovernmentService = new GovernmentServiceImpl
  val auditoryService: AuditoryService = new AuditoryServiceImpl
  val commonService: CommonService = new CommonServiceImpl

}
