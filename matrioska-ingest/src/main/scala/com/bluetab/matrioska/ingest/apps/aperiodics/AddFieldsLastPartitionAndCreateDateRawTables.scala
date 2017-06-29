package com.bluetab.matrioska.ingest.apps.aperiodics

import java.text.SimpleDateFormat
import java.util.Date

import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreRepositories, CoreServices}
import org.apache.spark.sql.{Row, SaveMode}

class AddFieldsLastPartitionAndCreateDateRawTables extends LinxApp {

  override def run(args: Seq[String]) {
    val tables1 = Seq(
      (Array("rd_netcash", "t_administracion_usuarios_tmp"), Array("rd_netcash", "t_administracion_usuarios")),
      (Array("rd_netcash", "t_uso_servicios_tmp"), Array("rd_netcash", "t_uso_servicios")),
      (Array("rd_netcash", "t_perfil_cliente_tmp"), Array("rd_netcash", "t_perfil_cliente")),
      (Array("rd_gm_calculated_data","tgriskr_apilog_tmp"),Array("rd_gm_calculated_data","tgriskr_apilog")),
      (Array("rd_gm_calculated_data","tgriskr_cr01_tmp"),Array("rd_gm_calculated_data","tgriskr_cr01")),
      (Array("rd_gm_calculated_data","tgriskr_deltaeq_tmp"),Array("rd_gm_calculated_data","tgriskr_deltaeq")),
      (Array("rd_gm_calculated_data","tgriskr_deltafx_tmp"),Array("rd_gm_calculated_data","tgriskr_deltafx")),
      (Array("rd_gm_calculated_data","tgriskr_deltainfl_tmp"),Array("rd_gm_calculated_data","tgriskr_deltainfl")),
      (Array("rd_gm_calculated_data","tgriskr_dv01nb_tmp"),Array("rd_gm_calculated_data","tgriskr_dv01nb")),
      (Array("rd_gm_calculated_data","tgriskr_expfx_tmp"),Array("rd_gm_calculated_data","tgriskr_expfx")),
      (Array("rd_gm_calculated_data","tgriskr_pl_tmp"),Array("rd_gm_calculated_data","tgriskr_pl")),
      (Array("rd_gm_calculated_data","tgriskr_rmargin_tmp"),Array("rd_gm_calculated_data","tgriskr_rmargin")),
      (Array("rd_gm_calculated_data","tgriskr_sdiv_tmp"),Array("rd_gm_calculated_data","tgriskr_sdiv")),
      (Array("rd_gm_calculated_data","tgriskr_vegaeq_tmp"),Array("rd_gm_calculated_data","tgriskr_vegaeq")),
      (Array("rd_gm_calculated_data","tgriskr_vegafx_tmp"),Array("rd_gm_calculated_data","tgriskr_vegafx")),
      (Array("rd_gm_calculated_data","tgriskr_vegainfl_tmp"),Array("rd_gm_calculated_data","tgriskr_vegainfl")),
      (Array("rd_gm_calculated_data","tgriskr_vegair_tmp"),Array("rd_gm_calculated_data","tgriskr_vegair"))
    )
    val tables2=Seq(
      (Array("rd_informacional", "tendsrfq_tmp"), Array("rd_informacional", "tendsrfq"))
    )

    val tables=tables2
    tables.foreach { table =>
      println("################# " + table._1(0) + "." + table._1(1) + " ###################")
      val oldDF = CoreRepositories.hiveRepository.table(table._1(0), table._1(1))
//      val oldFiles = oldDF.select(input_file_name()).distinct().collect()
      val createdDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
      val sourceName = "Carga Inicial"
      val sourceOffset = 0.toLong
      
      val newContentRDD = oldDF.rdd.map { x =>
        // Para tablas de version anterior de Linx
        //val line = x.toSeq
        //Row.fromSeq((line.take(line.size - 3) :+ createdDate) ++ (line.takeRight(3) :+ 1.toShort))
        // Para Informacional
        val fechaproceso=x.getAs[String]("fechaproceso")
        val year=fechaproceso.substring(0, 4).toInt
        val month=fechaproceso.substring(4, 6).toInt
        val day=fechaproceso.substring(6, 8).toInt
        val lastversion=1.toShort
        val line = x.toSeq
        Row.fromSeq(
             Seq(sourceName, sourceOffset ) 
               ++ (line.take(line.size - 1) :+ createdDate) 
               ++ Seq(year, month, day, lastversion))
      }
      val tableStruct = CoreServices.governmentService.getTableDefinition(table._2(0), table._2(1))
      val newContentDF = CoreRepositories.hiveRepository.createDataFrame(newContentRDD, tableStruct)
      //Borro todos los ficheros antiguos 
      newContentDF.write.mode(SaveMode.Append)
        .partitionBy("year", "month", "day", "last_version")
        .saveAsTable(CoreConfig.hive.schemas.get(table._2(0)) + "." + table._2(1))
//      newContentDF.take(5).foreach(x => println(x.toSeq))
//      oldFiles.foreach { x =>
//        val inputFileName = x.getString(0)
//        CoreRepositories.dfsRepository.deleteAbsolutePath(inputFileName)
//      }

    }

  }
}