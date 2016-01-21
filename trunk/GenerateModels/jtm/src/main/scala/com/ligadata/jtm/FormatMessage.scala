package com.ligadata.jtm

/**
  * Created by joerg on 1/20/16.
  */
object FormatMessage {

  // FormatMessage.Format("Hello {1}, World {0}", "World", "Hello" )
  def Format(fmt : String, args: String*) : String = {
    val subst = new Substitution
    for((arg, count) <- args.zipWithIndex) {
      subst.Add(count.toString, arg)
    }
    subst.Replace(fmt)
  }
}
