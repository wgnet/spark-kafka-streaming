package com.wargaming.dwh.avro

import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.avro.generic.{GenericRecord, GenericContainer}
import scala.collection.JavaConversions._
import org.apache.avro.Schema

/**
 * Created by d_balyka on 07.05.14.
 */
object AvroDatumUtils {

  def serialize[A <: GenericContainer](data: Seq[A]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val datumWriter = new SpecificDatumWriter[A](data.head.getSchema)
    val dataFileWriter = new DataFileWriter[A](datumWriter)
    dataFileWriter.create(data.head.getSchema, baos)
    data.foreach(d => {
      dataFileWriter.append(d)
    })
    dataFileWriter.close()
    baos.toByteArray
  }

  /**
   *
   * @param data
   * @return (schema, sequence of json strings)
   */
  def avroRecords(data: Array[Byte]): (Schema, Seq[GenericRecord]) = {
    val datumReader = new SpecificDatumReader[GenericRecord]()
    val dataFileStream = new DataFileStream[GenericRecord](new ByteArrayInputStream(data), datumReader)
    val schema = dataFileStream.getSchema
    (schema, dataFileStream.iterator().toSeq)
  }

}
