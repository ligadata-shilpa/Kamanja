#!/usr/bin/python

# server.py 
import socket                                         
import time
import argparse
import sys
import subprocess
import imp

 
sys.path.append("/home/rich/python/models") 

parser = argparse.ArgumentParser(description='Kamanja PyServer')
parser.add_argument('--port', help='port binding', required=True)
parser.add_argument('--host', help='host name (default=localhost) ', default="localhost")
args = vars(parser.parse_args())
#print args

# create a socket object
serversocket = socket.socket(
	        socket.AF_INET, socket.SOCK_STREAM) 

# get local machine name
host = socket.gethostname()     
port = int(args['port'])

# bind to the port
print('Connection parms: ' + host + ", " + args['port'])
serversocket.bind((host, port))    

def importName(moduleName, name):
	"""
	Import a named object from a module in the context of this function 
	"""
	try:
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

# Add the system level command to the dispatcher dict
for extname in 'addModel', 'removeModel', 'serverStatus', 'executeModel', 'stopServer':
	HandlerClass = importName("commands." + extname, "Handler")
	handler = HandlerClass()
	cmdDict[extname] = handler

# The dispatcher invokes the proper command based upon the first line of the received
# data.  It dispatches the appropriate handler for it, giving the command access to the
# model dictionary, host, port, and remaining args from 
# supplying the remaining arguments/lines from the command as its arg.
def dispatcher(cmdkey, args):
	cmd = cmdDict[cmdKey]
	results = cmd.handler(modelDict, host, port, args)
	return results

# queue up to 5 requests... could be another value... as written 
# the conn are serialized... we mitigate by having a pythonserver for each
# partition the engine chooses to distribute the input with.
preserveNewLines=True
serversocket.listen(5)
result = ''
while True:                                         
	conn, addr = serversocket.accept()
	print('Connected by', addr)
	while True:
	    # establish a connection
	    data = conn.recv(1024)
	    if not data: break # no more data... 
	    # split the data on the comma, clean up each element, use first as command and rest as arguments
	    # ... will change to "magic", xid, cmdKey, rest of args.  Multi line args managed with  str.splitlines([keepends])
	    # ... perhaps multi line commands would then pop the first line that has the xid and command in it
	    dataList = data.splitlines(preserveNewLines)
	    cmdRaw = dataList.pop(0) # obtain the command from first arg
	    # ... both cmd and dataList changed by pop
	    cmd = cmdRaw.strip()
	    result = dispatcher(cmd,dataList)
	    if (result != 'kill-9') # stop command will return 'kill-9' as value
	    	conn.sendall(result) 
	    conn.close()
    	break
	
	if (result == 'kill-9') # stop command stops listener tears down server ...
		break

print 'server is exiting due to stop command'
