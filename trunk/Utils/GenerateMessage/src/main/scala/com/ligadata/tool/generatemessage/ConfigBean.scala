package com.ligadata.tool.generatemessage

import scala.collection.immutable.Map

/**
  * Created by Yousef on 5/15/2016.
  */
class ConfigBean {

  private var _delimiter = ","
  private var _outputPath = ""
  private var _saveMessage = false
  private var _messageType = "fixed"
  private var _nameSpace = "com.message"
  private var _partitionKey = false
  private var _primaryKey = false
  private var _timePartition = false
  private var _feilds = Map[String, String]()

  // Getter
  def delimiter = _delimiter
  def outputPath = _outputPath
  def saveMessage= _saveMessage
  def nameSpace= _nameSpace
  def partitionKey = _partitionKey
  def primaryKey = _primaryKey
  def timePartition = _timePartition
  def feilds = _feilds

  // Setter
  def delimiter_= (value:String):Unit = _delimiter = value
  def outputPath_= (value:String):Unit = _outputPath = value
  def saveMessage_= (value:Boolean):Unit = _saveMessage = value
  def messageType_= (value:String):Unit = _messageType = value
  def nameSpace_= (value:String):Unit = _nameSpace = value
  def partitionKey_= (value:Boolean):Unit = _partitionKey = value
  def primaryKey_= (value:Boolean):Unit = _primaryKey = value
  def timePartition_= (value:Boolean):Unit = _timePartition = value
  def feilds_= (value:Map[String,String]):Unit = _feilds = value
}
