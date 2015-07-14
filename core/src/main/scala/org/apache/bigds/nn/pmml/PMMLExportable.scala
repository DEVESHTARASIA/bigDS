package org.apache.spark.mllib.bigds.pmml

import java.io.{StringWriter, OutputStream, File}
import javax.xml.transform.stream.StreamResult

import org.apache.spark.SparkContext
import org.jpmml.model.JAXBUtil

/**
 * Created by clin3 on 2015/3/18.
 */
trait PMMLExportable {

  /**
   * Export the model to the stream result in PMML format
   */
  private def toPMML(streamResult: StreamResult): Unit = {
    val modelExporter = ModelExporterFactory.createModelExporter(this)
    JAXBUtil.marshalPMML(modelExporter.pmml, streamResult)
  }

  /**
   * Export the model to a local File in PMML format
   */
  def toPMML(localPath: String): Unit = {
    toPMML(new StreamResult(new File(localPath)))
  }

  /**
   * Export the model to a distributed file in PMML format
   */
  def toPMML(sc: SparkContext, path: String): Unit = {
    val pmml = toPMML()
    sc.parallelize(Array(pmml),1).saveAsTextFile(path)
  }

  /**
   * Export the model to the Outputtream in PMML format
   */
  def toPMML(outputStream: OutputStream): Unit = {
    toPMML(new StreamResult(outputStream))
  }

  /**
   * Export the model to a String in PMML format
   */
  def toPMML(): String = {
    var writer = new StringWriter();
    toPMML(new StreamResult(writer))
    return writer.toString();
  }
}
