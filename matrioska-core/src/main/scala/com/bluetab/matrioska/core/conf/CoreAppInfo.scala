package com.bluetab.matrioska.core.conf

import com.bluetab.matrioska.core.LinxAppArgs
import com.bluetab.matrioska.core.beans.AppInfo
import org.slf4j.MDC

object CoreAppInfo {

  var malla = "Default Malla"
  var capa = "Default Capa"
  var job = "Default Job"
  var name = "Default Name"
  var useCase = "Default UseCase"
  var uuid = CoreContext.sc.applicationId
  var planDate = LinxAppArgs.planDate
  var notFound = true

  if (!LinxAppArgs.force) {
    val appInfo = CoreServices.governmentService.findAppInfo(LinxAppArgs.appName)
    appInfo match {
      case Some(appInfo:AppInfo) =>
        malla = appInfo.malla
        capa = appInfo.capa
        job = appInfo.job
        name = appInfo.name
        useCase = appInfo.useCase
        notFound = false
      case None =>
        notFound = true
    }
  } else {
    notFound = false
  }

  MDC.put("malla", malla)
  MDC.put("job", job)
  MDC.put("capa", capa)
  MDC.put("name", name)
  MDC.put("uuid", uuid)
  MDC.put("plandate", planDate)

}
