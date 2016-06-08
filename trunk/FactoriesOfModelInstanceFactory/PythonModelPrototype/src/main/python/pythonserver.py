#!/usr/bin/python

# server.py 
import socket                                         
import time
import argparse
import sys
#import subprocess
#import imp
import hashlib
import struct

#sys.path.insert(0, "/home/rich/github1/dev/r1.5.0/kamanja/trunk/FactoriesOfModelInstanceFactory/PythonModelPrototype/src/main/python") 
#print "sys.modules = " + str(sys.modules)

parser = argparse.ArgumentParser(description='Kamanja PyServer')
parser.add_argument('--host', help='host name (default=localhost) ', default="localhost", type=str)
parser.add_argument('--port', help='port binding (default=9999)', default="9999",type=int)
parser.add_argument('--pythonpath', required=True, type=str)
args = vars(parser.parse_args())

print 'starting pythonserver ...\nhost = ' + args['host'] + '\nport = ' + str(args['port']) + '\npythonpath = ' + args['pythonpath']

# set the sys.path s.t. the first path searched is our path 
sys.path.insert(0, args['pythonpath']) 

# create a socket object
serversocket = socket.socket(
	        socket.AF_INET, socket.SOCK_STREAM) 

# get local machine name
host = socket.gethostname()     
port = int(args['port'])

# bind to the port
print('Connection parms: ' + host + ", " + str(args['port']))
serversocket.bind((host, port))    

def importPackageByName(moduleName, name):
	"""
	Import the class 'name' found in package 'moduleName'.  The moduleName
	may be a sub-package (e.g., the module is found in the 'commands' 
	sub-package in the use below).
	"""
	try:
		print "load moduleName = " + moduleName 
		module = __import__(moduleName, globals(), locals(), [name])
	except ImportError:
		return None
	return getattr(module, name)


# command dictionary used to manage server loaded below
cmdDict = {
    #"addModel": addModel,  .
    #"stopModel": stopModel,
    }

# model instance dictionary.
modelDict = {
    }

# Modify command dict with the python server commands supporte.
# These are loaded on the server from the 'commands' sub directory specified 
# of a directory on the PYTHONPATH...e.g.,
#
#	export PYTHONPATH=$HOME/python/pyserver
#
# Using the PYTHONPATH makes sense at least for testing.  It might be fruitful
# for production as well... 
#
# A similar approach is used for the model dispatch dictionary.  It is dynamically
# created from content, however, that has been sent to the server on an open
# connection to the server.

# import the base model classes that are in the common subdirectory
#from abc import ABCMeta

importPackageByName("common.ModelBase", "ModelBase")
importPackageByName("common.ModelInstance", "ModelInstance")
# Add the system level command to the dispatcher dict
for extname in 'addModel', 'removeModel', 'serverStatus', 'executeModel', 'stopServer':
	HandlerClass = importPackageByName("commands." + extname, "Handler")
	handler = HandlerClass()
	cmdDict[extname] = handler


def unPack(rawmsg):
	"""
	This method a) verifies the message ingtegrity by examining
	the md5 markers and computing the md5 digest on the enclosed
	json data enclosed in the raw message to certify the message
	is legit. 
	From the valid json data, a dictionary is resurrected and the
	value of key "cmd" is found.  This value in turn is used to 
	locate the appropriate command handler for processing.
	If there is no "cmd" value or the md5 checks fail, an error 
	message is logged and returned.
	A tuple is returned containing the command key, the reconstituted
	dictionary resurrected from the json string and an error message field
	that will be empty ("") if all is well else the error description.	
	WARNING: supplied message is modified.
	"""
	lenMd5Digest = 16
	lenInt = 4
	rmsgLen = len(rawmsg)
	reasonable = rmsgLen > lenMd5Digest * 2 + lenInt

	cmd = ""
	cmdmap = []
	errmsg = ""
	if reasonable == True:
		startHash = rawmsg[0:lenMd5Digest]
		del rawmsg[0:lenMd5Digest]
		payloadLenBytes = rawmsg[0:lenInt]
		(payloadLen,) = unPack('>I', bytearray(payloadLenBytes)) #big endian
		del rawmsg[0:lenInt]
		payloadBytes = rawmsg[0:payloadLen]
		del rawmsg[0:payloadLen]
		endHash = rawmsg[0:lenMd5Digest]
		del rawmsg[0:lenMd5Digest]

		hashesMatch = startHash == endHash
		if hashesMatch == True:
			# calculate the md5 for ourselves and compare 
			h = hashlib.new('md5') # md5 digest used for marker based on msg
			h.update(payloadBytes)
			md5hash = h.digest()	
			validPayload = md5hash == endHash
			if validPayload == True:
		 		payload = str(payloadBytes)
		 		cmdmap = json.loads(payload)
		 		if "cmd" in cmdParameters and cmdParameters["cmd"] in cmdDict:
		 			cmd = cmdmap["cmd"]
		 		else:
		 			cmdStr = "none supplied"
		 			if "cmd" in cmdParameters:
		 				cmdStr = cmdParameters["cmd"]
		 			errmsg = "There is no command parameter or the command supplied is one that is not known... cmd = {}...legitimate server commands must be one of the following: {}".format(cmdStr, str(cmdDict.keys()))
			else:
				errmsg = "while the begin and end digests match, they do not reflect the md5 digest of the supplied message."
		else:
			errmsg = "the beginning and ending digests do not match... the input msg is bogus"
	else:
		errmsg = "Insufficient bytes supplied to be a reasonable message"
	return (cmd, cmdmap, errmsg)

def formatWireMsg(msg):
	"""
	Format the supplied message for transmission to client.  Format is
	described in the dispatcher function.
	"""
	msgBytes = bytearray(msg)
	msgBytesLen = len(msgBytes)
	msgBytesLenArray = struct.pack('>I', msgBytesLen) # big endian
	h = hashlib.new('md5') # md5 digest used for marker based on msgBytes
	h.update(msgBytes)
 	md5hash = h.digest()	
 	msgparts = []
 	msgparts.append(md5hash)
 	msgparts.append(msgBytesLenArray)
 	msgparts.append(msgBytes)
 	msgparts.append(md5hash)
 	rawmsg = b"".join(msgparts)
	return rawmsg

def goingDownMsg():
	"""
	A stop server command has been received.  Format a standard message and
	return it
	"""
	svrdownMsg = json.dumps({'Server' : host, 'Port' : str(port), 'Message' : "python server stopped by user command"})	
 	rawmsg = formatWireMsg(svrdownMsg)
	return rawmsg

def bogusResultsMsg(errmsg):
	"""
	Some junk was sent by client to us.  Issue formatted message with the supplied text
	"""
	bogusMsg = json.dumps({'Server' : host, 'Port' : str(port), 'Message' : errmsg})	
 	rawmsg = formatWireMsg(bogusMsg)
	return rawmsg

def dispatcher(rawmsg):
	"""
	a. The message byte array passed to and fro has these segments: 
	<StartMarker>, <DataLen>, <JsonData>, <EndMarker>
	b.  The markers are currently md5 hashes based upon the <JsonData> string (i.e., sans <DataLen>).
	c.  The <DataLen> is a 4 byte representation of the length of the
	<JsonData> string.
	d.  The <StartMarker> and <EndMarker> should have the same value.  The
	python server will compute its own md5 hash and compare with the markers.  If they do not match an error message is both logged and returned as the result.
	e.  The <JsonData> itself is a map.  In addition to the message integrity checks, to be successfully processed, a key must be found in the map == 'cmd' whose value is the command that will interpret the remaining key/value pairs in the dictionary.  Should the 'cmd' key value not refer to a
	legitimate command,  error is logged and returned.
	""" 
	results = ""
	(cmdKey, cmdParameters, errMsg) = unPack(rawmsg)
	if len(cmdKey) > 0:
		print "processing cmd = " + cmdKey + "...argument dict = " + str(cmdParameters)
		cmd = cmdDict[cmdKey]
		results = cmd.handler(modelDict, host, port, cmdParameters)
	else:
		results = bogusResultsMsg(errMsg)
	return results

# queue up to 5 requests... could be another value... as written 
# the conn are serialized... we mitigate by having a pythonserver for each
# partition the engine chooses to distribute the input with.
preserveNewLines=True
serversocket.listen(5)
result = ''
while True:                                         
    # establish a connection
	conn, addr = serversocket.accept()
	print('Connected by', addr)
	while True:
		# get a complete msg 1k at a time... we may want to adjust it up?
	    msgparts = []
	    remaining = 1024
	    while remaining > 0:
	        data = s.recv(remaining)     # Get available data
	        msgparts.append(data)        # Add it to list of msg parts
	        if not data: remaining = 0   # no more data...  
	    rawmsg = b"".join(msgparts)      # Stitch together the complete message

	    result = dispatcher(rawmsg) # string answer ...
	    if result != 'kill-9': # stop command will return 'kill-9' as value
	    	wireMsg = formatWireMsg(result)
	    	conn.sendall(wireMsg) 
	    else: 
	    	wireMsg =  goingDownMsg()
	    	conn.sendall(wireMsg)
	    conn.close()
    	break
	
	if result == 'kill-9': # stop command stops listener tears down server ...
		break

print 'server stopped by admin command'
