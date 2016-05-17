package com.ligadata.tool.generatemessage

/**
  * Created by Yousef on 5/12/2016.
  */
class DataTypeUtility { // This class created to check the value if it double or int

  def isInteger(field: String): Boolean={ //This method used to check if the value is Integer (return true if integer and false otherwise)
   try{
     val dataType = Integer.parseInt(field)
     return true
   } catch{
     case e: Exception => return false
   }
  }

  def isDouble(field: String): Boolean={ //This method used to check if the value is Double (return true if double and false otherwise)
    try{
      if(field.contains('.')) {
        val firstPart = field.substring(0, field.indexOf('.'))
        val secondPart = field.substring(field.indexOf('.')+1, field.length)
        var dataType = Integer.parseInt(firstPart)
        dataType = Integer.parseInt(secondPart)
        return true
      } else
        return false
    } catch{
      case e: Exception => return false
    }
  }

  def isBoolean(field: String): Boolean={ //This method used to check if the value is boolean or not (return true if boolean and false otherwise)
    if(field.equalsIgnoreCase("true")|| field.equalsIgnoreCase("false"))
      return true
    else
      return false
  }

  def isAllDigits(field: String): Boolean={ //This method used to check if all character in string digits or not (return true if all are digits and false otherwise)
    return field forall Character.isDigit
  }

  def FindFeildType(feild: String): String={
    if(isDouble(feild)){
      return "Double"
    } else if(isInteger(feild)){
      return "Int"
    } else if(isBoolean(feild)){
      return "Boolean"
    } else {
      return "String"
    }
  }
}
