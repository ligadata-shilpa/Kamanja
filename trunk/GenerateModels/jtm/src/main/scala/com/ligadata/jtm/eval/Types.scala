package com.ligadata.jtm.eval

import com.ligadata.jtm.nodes.Root

/**
  * Created by joerg on 2/3/16.
  */
object Types {

  /** Map with all messages used and their aliases
    *
    * @param root
    */
  def CollectMessages(root: Root): Map[String, Set[String]] = {
    val givenAlias = root.aliases
    var result = scala.collection.Map.empty[String, Set[String]]
    // Go through all transformations
    root.transformations.foreach( t => {
      // Dependency sets test
      t._2.dependsOn.foreach( d => {
        d.map( c => {
          val resolved = givenAlias.getOrElse(c, c)
          val f = result.get(resolved)
          if(f.isDefined) {
            result.updated(resolved, f.get ++ Set(c))
          } else {
            result += (resolved -> Set(c))
          }
        })
      })
      t._2.outputs.foreach( o => {
        val c = o._1 // Name of the output
        val resolved = givenAlias.getOrElse(c, c)
        val f = result.get(resolved)
        if(f.isDefined) {
          result.updated(resolved, f.get ++ Set(c))
        } else {
          result += (resolved -> Set(c))
        }
      })
    })
    result
  }
}
