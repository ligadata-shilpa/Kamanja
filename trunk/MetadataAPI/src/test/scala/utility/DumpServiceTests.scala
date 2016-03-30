package com.ligadata.MetadataAPI.utility.test
import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.DumpService
import com.ligadata.MetadataAPI.Utility.ConfigService
/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class DumpServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "Dump metadata" should "display the metadata" in {
      DumpService.dumpMetadata should include regex "Metadata dumped"
  }

  "Dump all nodes" should "dump all the nodes in the metadata" in {
    val clusterCfg = getClass.getResource("/Metadata/config/ClusterConfig.json").getPath
    ConfigService.uploadClusterConfig(clusterCfg)
    DumpService.dumpAllNodes should include regex "Successfuly fetched all nodes"
  }

  "dumpAllClusters" should "dump all the clusters in the metadata" in {
    val clusterCfg = getClass.getResource("/Metadata/config/ClusterConfig.json").getPath
   ConfigService.uploadClusterConfig(clusterCfg)
    DumpService.dumpAllClusters should include regex "Successfuly fetched all clusters"
  }

  "dumpAllClusterCfgs" should "dump all the cluster cfgs in the metadata" in {
    val clusterCfg = getClass.getResource("/Metadata/config/ClusterConfig.json").getPath
    ConfigService.uploadClusterConfig(clusterCfg)
    DumpService.dumpAllClusterCfgs should include regex "Successfully fetched all Cluster Configs"
  }

  "dumpAllAdapters" should "dump all the adapters in the metadata" in {
    val clusterCfg = getClass.getResource("/Metadata/config/ClusterConfig.json").getPath
    ConfigService.uploadClusterConfig(clusterCfg)
    DumpService.dumpAllAdapters should include regex "Successfully fetched all adpaters"
  }

}

