package com.ligadata.tool.generatemessage

/**
  * Created by Yousef on 5/12/2016.
  */
class DataTypeUtility {

  def isInteger(field: String): Boolean={
   try{
     val dataType = Integer.parseInt(field)
     return true
   } catch{
     case e: Exception =>     return false
   }
  }

  def isDouble(field: String): Boolean={
    try{
      val firstPart = field.substring(0,field.indexOf('.'))
      val secondPart = field.substring(field.indexOf('.'), field.length)
      var dataType = Integer.parseInt(firstPart)
      dataType = Integer.parseInt(secondPart)
      return true
    } catch{
      case e: Exception =>  return false
    }
  }
}
