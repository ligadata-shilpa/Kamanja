package com.ligadata.tool.generatemessage

/**
  * Created by Yousef on 5/12/2016.
  */

import java.lang._

class DataTypeUtility extends LogTrait{ // This class created to check the value if it double or int

  def isInteger(field: String): Boolean={ //This method used to check if the value is Integer (return true if integer and false otherwise)
   try{
     val dataType = Integer.parseInt(field)
     if(dataType <= Integer.MAX_VALUE && dataType >= Integer.MIN_VALUE) {
       return true
     } else return false
   } catch{
     case e: Exception => return false
   }
  }

  def isDouble(field: String): Boolean={ //This method used to check if the value is Double (return true if double and false otherwise)
    try{
      if(field.contains('.')) {
        val dataType = Double.parseDouble(field)
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
    if(isFloat(feild)){
      return "Float"
    } else if(isDouble(feild)){
      return "Double"
    } else if(isInteger(feild)){
      return "Int"
    } else if(isLong(feild)) {
      return "Long"
    } else if(isBoolean(feild)){
      return "Boolean"
    } else {
      return "String"
    }
  }

  def isLong(field: String): Boolean={ //This method used to check if the value is Integer (return true if integer and false otherwise)
    try{
      val dataType = Long.parseLong(field)
      return true
    } catch{
      case e: Exception => return false
    }
  }

  def isFloat(field: String): Boolean={ //This method used to check if the value is Double (return true if double and false otherwise)
    try{
      if(field.contains('.')) {
        val dataType = Float.parseFloat(field)
        if(dataType >= Float.MIN_VALUE && dataType <= Float.MAX_VALUE) {
          return true
        } else return false
      } else
        return false
    } catch{
      case e: Exception => return false
    }
  }

  def FindFinalType(fileSize: Int, itemIndex: Int, inputFile: String, delimiter: String): String = {
    val fileBean: FileUtility = new FileUtility()
    var previousType = ""
    for (size <- 2 to 4) {
      if (fileSize >= size) {
        val fieldLines = fileBean.ReadHeaderFile(inputFile, size - 1)
        val linesfeild = fileBean.SplitFile(fieldLines, delimiter)
        val currentType = FindFeildType(linesfeild(itemIndex))
        if (previousType.equalsIgnoreCase("string") || (previousType.equalsIgnoreCase("boolean") && !currentType.equalsIgnoreCase("boolean"))
          || (!previousType.equalsIgnoreCase("boolean") && !previousType.equalsIgnoreCase("") && (currentType.equalsIgnoreCase("string") || currentType.equalsIgnoreCase("boolean")))
          || (previousType.equalsIgnoreCase("Int") && currentType.equalsIgnoreCase("boolean"))) {
          previousType = "String"
        } else if (previousType.equalsIgnoreCase("boolean") && currentType.equalsIgnoreCase("boolean")) {
          previousType = "Boolean"
        } else if ((previousType.equalsIgnoreCase("double") && (!currentType.equalsIgnoreCase("string") && !currentType.equalsIgnoreCase("boolean")))
          || (previousType.equalsIgnoreCase("long") && (currentType.equalsIgnoreCase("double") || currentType.equalsIgnoreCase("float")))
          || (previousType.equalsIgnoreCase("float") && (currentType.equalsIgnoreCase("long") || currentType.equalsIgnoreCase("double")))) {
          previousType = "Double"
        } else if (previousType.equalsIgnoreCase("long") && (currentType.equalsIgnoreCase("long") || currentType.equalsIgnoreCase("int"))) {
          previousType = "Long"
        } else if (previousType.equalsIgnoreCase("float") && (currentType.equalsIgnoreCase("float") || currentType.equalsIgnoreCase("int"))) {
          previousType = "Float"
        } else if (previousType.equalsIgnoreCase("") || (previousType.equalsIgnoreCase("int") && !currentType.equalsIgnoreCase("boolean"))) {
          previousType = currentType
        } else if (previousType.equalsIgnoreCase("int") && currentType.equalsIgnoreCase("boolean")) {
          previousType = "Boolean"
        }
      }
    }
    return previousType
  }

  def splitToArray(value: String): Array[String]={//this method used to split string to array for PartitionKey,PrimaryKey,TimePartitionInfo
    return value.split(",")
  }

  def CheckKeys(messagefields: Array[String], keys: String): Array[String] ={
    val keysArray = splitToArray(keys)
    for (key <- keysArray){
      if(!messagefields.contains(key)){
        logger.error("%s key from partitioKey/PrimaryKey/TimePartitionInfo does not in message fields. choose another key please".format(key))
        sys.exit(1)
      }
    }
    return keysArray
  }
}
