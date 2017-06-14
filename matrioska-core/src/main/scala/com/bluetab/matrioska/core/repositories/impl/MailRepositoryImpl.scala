package com.bluetab.matrioska.core.repositories.impl

import com.bluetab.matrioska.core.beans.MailMessage
import com.bluetab.matrioska.core.beans.MailRecipientTypes.{Cc, Cco, To}
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreContext, CoreRepositories}
import com.bluetab.matrioska.core.repositories.MailRepository

import scala.collection.immutable.Seq
import scala.sys.process._

class MailRepositoryImpl extends MailRepository {

  def sendMail(mailMessage: MailMessage) = {

    val script = CoreConfig.mail.script
    var toList = Seq[String]()
    var ccList = Seq[String]()
    var ccoList = Seq[String]()
    for (recipient <- mailMessage.getRecipients) {
      recipient._1 match {
        case Some(To) => toList = toList :+ recipient._2
        case Some(Cc) => ccList = ccList :+ recipient._2
        case Some(Cco) => ccoList = ccoList :+ recipient._2
        case None =>
      }
    }
    val subject = mailMessage.subject
    val text = mailMessage.text
    var filepaths = Seq[String]()
    mailMessage.getAttachments.foreach{ attachment =>
      CoreRepositories.fsRepository.createFileWithContent(attachment.name, attachment.content.toCharArray)
      filepaths = filepaths :+ attachment.name
    }
    val filePathsSeparated = filepaths.mkString(",")
    val to = toList.mkString(",")
    val cc = ccList.mkString(",")
     
    var cmd = Seq(script, "--subject", subject, "--body", text)
    
    if (to.length > 0){
      cmd = cmd :+ "--to" :+ to
    }
    if (cc.length > 0){
      cmd = cmd :+ "--cc" :+ cc
    }
    if (filePathsSeparated.length > 0){
      cmd = cmd :+ "--attach" :+ filePathsSeparated
    }
    
    CoreContext.logger.info("Envío de email: " + cmd.mkString(" "))
    val result = cmd !

    // Borramos los ficheros generados en el attachment
    mailMessage.getAttachments.foreach{ attachment =>
      CoreRepositories.fsRepository.deleteFile(attachment.name)
    }

    CoreContext.logger.info("Resultado del envío de email:" + result.toString)
  }

}
