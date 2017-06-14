package com.bbva.ebdm.linx.ingest.services.impl

import com.bbva.ebdm.linx.ingest.constants.IngestDfsConstants
import com.bbva.ebdm.linx.ingest.services.InformacionalService
import com.bbva.ebdm.linx.core.exceptions.FatalException
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import org.apache.spark.sql.DataFrame
import com.bbva.ebdm.linx.ingest.structs.InformacionalStructs
import com.bbva.ebdm.linx.core.conf.CoreServices
import org.apache.spark.sql.DataFrame
import com.bbva.ebdm.linx.ingest.constants.IngestConstants

class InformacionalServiceImpl extends InformacionalService {

  def loadTimeInfo: DataFrame = {
    val maxDate = CoreServices.commonService.getMaxPartition("rd_informacional", "tenydfec")
    var dates: Option[DataFrame] = None
    maxDate match {
      case Some(maxDate) =>
        dates = CoreRepositories.hiveRepository.sql(IngestConstants.HqlInformacionalSelectAllLastPartition, maxDate.substring(0, 4), maxDate.substring(4, 6), maxDate.substring(6, 8))
      case None => throw new FatalException("No tenemos MaxDate para la tabla Tiempo")
    }
    if (dates == None)
      throw new FatalException("No se recuperan Fechas")
    dates.get.registerTempTable("datesInfo")
    return dates.get

  }

}