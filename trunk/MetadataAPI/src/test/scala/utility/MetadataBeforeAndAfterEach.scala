package com.ligadata.MetadataAPI.utility.test

/**
 * Created by dhavalkolapkar on 3/23/16.
 */
import java.io.File
import java.io._
import org.scalatest._
import com.ligadata.MetadataAPI.MetadataAPIImpl

trait MetadataBeforeAndAfterEach extends BeforeAndAfterEach { this: Suite =>
  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
      file.delete
  }

  def cleanDir(location: String): Unit ={
    val temp=new File(location)
    if(temp.exists()){
      delete(temp)
    }
    new File(location).mkdir()
    println("Directory purged")

}
  override def beforeEach() {
    val loc=getClass.getResource("/storage").getPath
    val mdMan: MetadataManager = new MetadataManager()
    val metaProp=new MetadataAPIProperties
    print("Location: "+loc )
    cleanDir(loc)
    mdMan.initMetadataCfg(metaProp)

    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach() {
    try super.afterEach() // To be stackable, must call super.afterEach
    finally
      MetadataAPIImpl.shutdown
    //builder.clear()
  }

}
