package com.wargaming.dwh

import java.io.{ByteArrayOutputStream, File, OutputStream, PrintStream}

/**
 * Created by d_balyka on 15.05.14.
 */
object LoggingUtils {

  def fixLoggingToErrStream(logFilename: String = "") = {
    if (logFilename.isEmpty) {
      System.setErr(new PrintStream(new BlackHoleOutputStream()))
    } else {
      try {
        val file = new File(logFilename)
        file.getParentFile.mkdirs()
        if (!file.exists()) {
          file.createNewFile()
        }
        System.setErr(new PrintStream(logFilename))
      } catch {
        case t: Throwable => {
          System.setErr(new PrintStream(new BlackHoleOutputStream()))
        }
      }
    }
  }
}

class BlackHoleOutputStream extends OutputStream {
  override def write(b: Int): Unit = {
    /*do nothing*/
  }
}