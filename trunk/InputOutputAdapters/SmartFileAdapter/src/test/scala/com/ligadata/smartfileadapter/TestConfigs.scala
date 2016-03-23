package com.ligadata.smartfileadapter

import org.scalatest._
import com.ligadata.AdaptersConfiguration.SmartFileAdapterConfiguration
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration
/**
  * Created by Yasser on 3/10/2016.
  */
class TestConfigs extends FunSpec with BeforeAndAfter with ShouldMatchers with BeforeAndAfterAll with GivenWhenThen {

  val adapterSpecificCfgJson =
    """
      |{
      |  "Type": "Hdfs",
      |  "ConnectionConfig": {
      |    "HostLists": "10.20.30.40:2000,10.20.30.40:2001",
      |    "UserId": "uid",
      |    "Password": "pwd"
      |  },
      |  "MonitoringConfig": {
      |  "Locations": "/data/input,/tmp/input",
      |    "MaxTimeWait": "5000"
      |  }
      |}
    """.stripMargin

  val adapterSpecificCfgJsonKerberos =
  """{
    |    "Type": "Hdfs",
    |    "ConnectionConfig": {
    |      "HostLists": "10.20.30.40:2000,10.20.30.40:2001",
    |      "Authentication": "kerberos",
    |	     "Principal": "user@domain.com",
    |	     "Keytab": "/tmp/user.keytab"
    |    },
    |    "MonitoringConfig": {
    |	  "Locations": "/data/input,/tmp/input",
    |      "MaxTimeWait": "5000"
    |    }
    |}""".stripMargin

  describe("Unit Test for smart file adapter adapterSpecificCfg json parsing") {

    it("Make sure json parsing of adapter specific config is correct") {

      val inputConfig = new AdapterConfiguration()
      inputConfig.Name = "TestInput_2"
      inputConfig.className = "com.ligadata.InputAdapters.SamrtFileInputAdapter$"
      inputConfig.jarName = "smartfileinputoutputadapters_2.10-1.0.jar"
      //inputConfig.dependencyJars = new Set()
      inputConfig.adapterSpecificCfg = adapterSpecificCfgJson

      val conf = SmartFileAdapterConfiguration.getAdapterConfig(inputConfig)

      conf._type shouldEqual  "Hdfs"
      conf.connectionConfig.userId shouldEqual "uid"
      conf.connectionConfig.password shouldEqual "pwd"
      conf.connectionConfig.hostsList.mkString(",") shouldEqual "10.20.30.40:2000,10.20.30.40:2001"
      conf.monitoringConfig.locations.mkString(",") shouldEqual "/data/input,/tmp/input"
      conf.monitoringConfig.waitingTimeMS shouldEqual 5000
    }
  }


  describe("Unit Test for smart file adapter adapterSpecificCfg json parsing with kerberos auth") {

    it("Make sure json parsing of adapter specific config is correct") {

      val inputConfig = new AdapterConfiguration()
      inputConfig.Name = "TestInput_2"
      inputConfig.className = "com.ligadata.InputAdapters.SamrtFileInputAdapter$"
      inputConfig.jarName = "smartfileinputoutputadapters_2.10-1.0.jar"
      //inputConfig.dependencyJars = new Set()
      inputConfig.adapterSpecificCfg = adapterSpecificCfgJsonKerberos

      val conf = SmartFileAdapterConfiguration.getAdapterConfig(inputConfig)

      conf._type shouldEqual  "Hdfs"
      conf.connectionConfig.authentication shouldEqual "kerberos"
      conf.connectionConfig.principal shouldEqual "user@domain.com"
      conf.connectionConfig.keytab shouldEqual "/tmp/user.keytab"
      conf.connectionConfig.hostsList.mkString(",") shouldEqual "10.20.30.40:2000,10.20.30.40:2001"
      conf.monitoringConfig.locations.mkString(",") shouldEqual "/data/input,/tmp/input"
      conf.monitoringConfig.waitingTimeMS shouldEqual 5000
    }
  }
}
