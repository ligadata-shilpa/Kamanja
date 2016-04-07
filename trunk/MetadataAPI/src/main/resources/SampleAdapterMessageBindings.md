****************************
**Adapter Message Bindings**

Adapter message bindings are independently cataloged metadata objects that associate three other metadata objects together:

- An adapter (input, output, or storage)
- A message that this adapter either deserializes from its source or serializes to send to its sink.
- The serializer to use that understands how to serialize and deserialize the message.

There are currently three builtin serializers provided in the Kamanja distribution that can be used: a JSON serializer, a CSV serializer and a KBinary serializer.  The KBinary is used principally by the Kamanja platform to manage data in its stores and caches.

The adapter message bindings can be ingested one at a time (see the [input adapter example below]).  If there are multiple messages that are managed by an adapter that all share the same serializer, a compact representation is possible (see the [output adapter example below]).  It is also possible to organize one or more adapter message binding specifications in a file and have kamanja consume that.

************************************
**Adapter Message Binding Examples**

What follows is a number of annotated examples that illustrate the bindings and how one might use them to configure a cluster.  The local kamanja command is used here.  See [The wiki page for the MetadataAPI service] to see the service rendition of commands like these.


**Input Adapter Message Binding**

{
  "AdapterName": "kafka_adapter1",
  "MessageName": "com.botanical.csv.ordermsg",
  "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser",
  "Options": {
    "lineDelimiter": "\r\n",
    "fieldDelimiter": ",",
    "produceHeader": true,
    "alwaysQuoteFields": false
  }
}

_cmd (pwd is installdir):_
	bin/kamanja debug config/MetadataAPIConfig.properties add adaptermessagebinding '<json> {"AdapterName": "kafka_adapter1", "MessageName": "com.botanical.csv.ordermsg", "Options": {"alwaysQuoteFields": false, "fieldDelimiter": ",", "lineDelimiter": "\r\n", "produceHeader": true }, "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser"} </json>'

_The keys in the JSON specification are case sensitive.  For example, use "AdapterName", not "ADAPTERname" or "adaptername"._

Note that the adapter message binding sent to the kamanja script is just a flattened version of the json map structure presented above wrapped with the <json>..</json> tokens.  These tokens allow the kamanja command processor to recognize the beginning and end of the json specified configuration for the adapter message binding.  Note that the _space_ following <json> and preceding the closing </json> tokens are currently _required_.  

The serializer used in this case is the name of the builtin CSV deserializer.  Note too that it has an options map that is used to configure it.  See the [description of CSV serializer] for what the options mean.


**Output Adapter Message Binding**
{
  "AdapterName": "kafka_adapter2",
  "MessageNames": ["com.botanical.csv.emailmsg", "com.botanical.csv.shippingmsg"],
  "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser",
  "Options": {
    "lineDelimiter": "\r\n",
    "fieldDelimiter": ",",
    "produceHeader": true,
    "alwaysQuoteFields": false
  }
}

_cmd (pwd is installdir):_
	bin/kamanja config/MetadataAPIConfig.properties add adaptermessagebinding '<json> {"AdapterName": "kafka_adapter2", "MessageNames": ["com.botanical.csv.emailmsg", "com.botanical.csv.shippingmsg"], "Options": {"alwaysQuoteFields": false, "fieldDelimiter": ",", "lineDelimiter": "\r\n", "produceHeader": true }, "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser"} </json>'

This output adapter also uses the CSV builtin adapter, but notice too that there are two messages mentioned in the MessageNames key value.  What actually happens is that binding metadata is built for each adapter/message/serializer combination.  Think of this as a short hand.

If the key for the message is "MessageName", then only message name is given.  If "MessageNames" (plural), an array of names is expected.

**Storage Adapter Message Binding**
{
  "AdapterName": "hbase1",
  "MessageNames": ["com.botanical.csv.audit.ordermsg", "com.botanical.json.audit.shippingmsg"],
  "Serializer": " org.kamanja.serdeser.JsonSerDeser"
}

_cmd (pwd is installdir):_
	bin/kamanja config/MetadataAPIConfig.properties add adaptermessagebinding '<json> {"AdapterName": "hbase1", "MessageNames": ["com.botanical.csv.audit.ordermsg", "com.botanical.json.audit.shippingmsg"], "Serializer": " org.kamanja.serdeser.JsonSerDeser"} </json>'

In this storage adapter binding, the use of the builtin JSON adapter is illustrated.  Again there are multiple messages specified.  The JSON serializer doesn't currently have any options, so it can be omitted.

**Storage Adapter Message Binding Specifications in a File**

A file can have multiple binding specifications in it.  For example,

[
	{
	  "AdapterName": "kafka_adapter1",
	  "MessageName": "com.botanical.csv.ordermsg",
	  "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser",
	  "Options": {
		"lineDelimiter": "\r\n",
		"fieldDelimiter": ",",
		"produceHeader": true,
		"alwaysQuoteFields": false
	  }
	},
	{
	  "AdapterName": "kafka_adapter2",
	  "MessageNames": ["com.botanical.csv.emailmsg", "com.botanical.csv.shippingmsg"],
	  "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser",
	  "Options": {
		"lineDelimiter": "\r\n",
		"fieldDelimiter": ",",
		"produceHeader": true,
		"alwaysQuoteFields": false
	  }
	},
	{
	  "AdapterName": "hbase1",
	  "MessageNames": ["com.botanical.csv.audit.ordermsg", "com.botanical.json.audit.shippingmsg"],
	  "Serializer": " org.kamanja.serdeser.JSON"
	}
]


Using a file to ingest all (or at least the key adapter bindings) for a new cluster has its attraction.  A binding is prepared for each map and, as can be seen in the kafka_adapter2 and hbase1 adapters, the multiple message shorthand is used that will cause a binding for each unique triple (adapter, message, serializer).

If the above file was called ~/HalcyonClusterAdapterBindings.json, a Kamanja command can directly ingest it:

_cmd (pwd is installdir):_
	bin/kamanja debug config/MetadataAPIConfig.properties add adaptermessagebinding ~/HalcyonClusterAdapterBindings.json

Like the other examples, you can also push this directly on the command line too (it is useful to have an editor that can pretty print/flatten the JSON text to do these more complicated structures for the command line):

_cmd (pwd is installdir):_
	bin/kamanja config/MetadataAPIConfig.properties add adaptermessagebinding '<json> [{"AdapterName": "kafka_adapter1", "MessageName": "com.botanical.csv.ordermsg", "Options": {"alwaysQuoteFields": false, "fieldDelimiter": ",", "lineDelimiter": "\r\n", "produceHeader": true }, "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser"}, {"AdapterName": "kafka_adapter2", "MessageNames": ["com.botanical.csv.emailmsg", "com.botanical.csv.shippingmsg"], "Options": {"alwaysQuoteFields": false, "fieldDelimiter": ",", "lineDelimiter": "\r\n", "produceHeader": true }, "Serializer": " org.kamanja.serdeser.csv.CsvSerDeser"}, {"AdapterName": "hbase1", "MessageNames": ["com.botanical.csv.audit.ordermsg", "com.botanical.json.audit.shippingmsg"], "Serializer": " org.kamanja.serdeser.JSON"} ] </json>'








