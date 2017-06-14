package com.bbva.ebdm.linx.core.beans

import com.bbva.ebdm.linx.core.beans.MailRecipientTypes.MailRecipientType

object MailRecipientTypes {
  sealed trait MailRecipientType
  case object To extends MailRecipientType
  case object Cc extends MailRecipientType
  case object Cco extends MailRecipientType

  def getObject(mailRecipientType: String): Option[MailRecipientType] = {
    mailRecipientType match {
      case "To" => Some(To)
      case "Cc" => Some(Cc)
      case "Cco" => Some(Cco)
      case _ => None
    }
  }
  val mailRecipientTypes = Seq(To, Cc, Cco)
}

class MailMessage {
  var subject = ""
  var text = ""
  var from = ""
  private var recipients: Seq[(Option[MailRecipientType], String)] = Seq()
  private var attachments: Seq[Attachment] = Seq()

  def addRecipient(mailRecipientType: MailRecipientType, recipient: String) = {
    recipients = recipients :+ (Some(mailRecipientType), recipient)
  }

  def addAttachment(attachment: Attachment) = {
    attachments = attachments :+ attachment
  }

  def getAttachments = attachments

  def getRecipients = recipients

}
