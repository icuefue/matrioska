package com.bbva.ebdm.linx.ingest.services

import org.apache.spark.sql.DataFrame

/**Interfaz con los servicios comunes del proyecto Informacional
 * @author xe58268
 *
 */
trait InformacionalService {
  
 /**Recupera la tabla completa(Ultima partición) con la información de la tablas Tiempos(tenydfec)
 * @return DataFrame con la información de la tabla Tiempos(tenydfec)
 */
def loadTimeInfo: DataFrame
}