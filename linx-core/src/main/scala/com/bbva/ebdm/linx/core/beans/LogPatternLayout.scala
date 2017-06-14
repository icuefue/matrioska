package com.bbva.ebdm.linx.core.beans

import org.apache.log4j.PatternLayout
import org.apache.log4j.helpers.PatternConverter
import org.apache.log4j.helpers.PatternParser
import org.apache.log4j.spi.LoggingEvent

class LogPatternLayout extends PatternLayout {

  override def createPatternParser(pattern: String): PatternParser = {
    new LogPatternParser(pattern);
  }
}

class LogPatternParser(pattern: String) extends PatternParser(pattern) {

  override def finalizeConverter(c: Char) = {
    c match {
      case 'm' =>
        currentLiteral.setLength(0);
        addConverter(new LogPatternConverter());
      case _ => super.finalizeConverter(c);
    }

  }

  class LogPatternConverter extends PatternConverter {

    override def convert(evt: LoggingEvent): String = {
      // For simplicity, assume this information is retrieved from somewhere.
      return evt.getMessage.toString().replaceAll("[\n\t]", " ");
    }
  }

} 
