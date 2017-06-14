package com.bbva.ebdm.linx.core.repositories.impl


import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Table
import com.bbva.ebdm.linx.core.repositories.HBaseRepository
import com.bbva.ebdm.linx.core.conf.CoreContext
import com.bbva.ebdm.linx.core.conf.CoreConfig

class HBaseRepositoryImpl extends HBaseRepository {

  def write(schema: String, table: String, value: (String, Map[String, Map[String, String]])) = {

    var tableHBase : Option[Table] = None
    val tableName = CoreConfig.hbase.schemas.get(schema) + ":" + table
    try {

      tableHBase = Some(CoreContext.hBaseConnection.getTable(TableName.valueOf(tableName)))
      CoreContext.logger.debug("HBASE: inserción en: " + tableHBase.toString)
      val p = new Put(Bytes.toBytes(value._1))

      for ((columnFamily, fields) <- value._2) {
        for ((column, fieldValue) <- fields) {
          p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(fieldValue))
        }
      }
      CoreContext.logger.debug("HBASE: inserción de: " + p.toString())
      tableHBase.get.put(p)
      CoreContext.logger.debug("HBASE: realizada correctamente")
    } catch {
      case t: Throwable => throw t
    } finally {
      tableHBase match {
        case Some(table:Table) => table.close()
        case None => CoreContext.logger.info("tableHBase -> null")
      }
    }
  }

}
