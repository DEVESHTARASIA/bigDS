package org.apache.spark.mllib.bigds.pmml

import java.text.SimpleDateFormat
import java.util.Date
import org.dmg.pmml.{Timestamp, Application, PMML, Header}

/**
 * Created by clin3 on 2015/3/15.
 */
private[bigds]trait ModelExporter {
  val pmml: PMML = new PMML()

  setHeader(pmml)

  private def setHeader(pmml: PMML): Unit = {
    val version = getClass.getPackage.getImplementationVersion
    val app = new Application()
      .withName("bigDS")
      .withVersion(version)
    val timestamp = new Timestamp()
      .withContent(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      .format(new Date()))
    val header = new Header()
      .withApplication(app)
      .withTimestamp(timestamp)
    pmml.setHeader(header)
  }

}
