package com.ligadata.jtm.eval

import com.ligadata.jtm.nodes.Root

/**
  * Created by joerg on 2/3/16.
  */
object Types {


  def upsert(key: String, value: Set[String]) (implicit map: scala.collection.mutable.Map[String, Set[String]])= {
    val c = map.get(key)
    if(c.isDefined) {
      val s = c.get
      val r = s ++ value
      map += (key -> r)
    } else {
      map += (key -> value)
    }
  }

  /** Map with all messages used and their aliases
    *
    * @param root
    * @return
    */
  def CollectMessages(root: Root): Map[String, Set[String]] = {
    val givenAlias = root.aliases
    implicit var result = scala.collection.mutable.Map.empty[String, Set[String]]

    // Go through all transformations
    root.transformations.foreach( t => {
      // Dependency sets test
      t._2.dependsOn.foreach( d => {
        d.map( c => {
          val resolved = givenAlias.getOrElse(c, c)
          upsert(resolved, Set(c))
        })
      })
      t._2.outputs.foreach( o => {
        val c = o._1 // Name of the output
        val resolved = givenAlias.getOrElse(c, c)
         upsert(resolved, Set(c))
      })
    })
    result.toMap
  }

  /** Map with all types and the path to it
    *
    * @param root
    * @return
    */
  def CollectTypes(root: Root): Map[String, Set[String]] = {
    var result = scala.collection.mutable.Map.empty[String, Set[String]]

    result.toMap
  }
}
