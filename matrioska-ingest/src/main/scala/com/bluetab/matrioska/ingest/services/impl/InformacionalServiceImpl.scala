package com.bluetab.matrioska.ingest.services.impl

import com.bluetab.matrioska.ingest.constants.IngestDfsConstants
import org.apache.spark.sql.DataFrame
import com.bluetab.matrioska.core.conf.CoreServices
import org.apache.spark.sql.DataFrame
import com.bluetab.matrioska.core.conf.{CoreRepositories, CoreServices}
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.ingest.constants.IngestConstants
import com.bluetab.matrioska.ingest.services.InformacionalService
import com.bluetab.matrioska.ingest.structs.InformacionalStructs

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