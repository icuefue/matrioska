package com.bbva.ebdm.linx.core.repositories

import com.bbva.ebdm.linx.core.beans.MailMessage


/**
 * Acciones para enviar emails desde la plataforma de BigData
 *
 * @author xe58268
 */
trait MailRepository {

  /**
   * Método para el envío de mails en la plataforma BigData. Los parámetros de envío se definen en el 
   * objeto de tipo MailMessage
   * 
 * @param mailMessage: información para el envío del mensaje como los destinatarios, asunto, cuerpo, etc
 */
def sendMail(mailMessage: MailMessage)
}
