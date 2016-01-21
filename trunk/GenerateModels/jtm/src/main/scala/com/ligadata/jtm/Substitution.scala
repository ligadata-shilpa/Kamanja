package com.ligadata.jtm

/**
  * Created by joerg on 1/20/16.
  */
import scala.collection.mutable.Map

// Note this can create an endless recursion Run( "{Hello}" with "Hello"-> "{World}", World->"{Hello}"
class Substitution {

  def Add(name: String, value: String) = {
    subts += (name -> value)
  }

  def Run(value: String): String = {

      val r = Replace(value)
      if(r!=value)
        Run(r)
      else
        r
  }

  def Replace(value: String): String = {
    val r = subts.foldLeft(value)((s:String, x:(String,String)) => ( "#\\{" + x._1 + "\\}" ).r.replaceAllIn( s, x._2 ))
    r
  }

  var subts = Map.empty[String, String]
}
