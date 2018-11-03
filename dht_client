#!/usr/bin/env python
import sys
import signal
import socket
import sqlite3 as sql
import hashlib
import os
from time import sleep
from copy import copy,deepcopy
from struct import *

TCP_MAX_SIZE=1500

D = { 
	'root_name':'sombrero.ccs.neu.edu', 
	'root_port':15151,
	'mode':0,
	'my_name':'captain.ccs.neu.edu',
	'my_port':15151 
}

def h(fname):
	return hashlib.sha1(fname).digest()

def toUni(hash_digest):
	return u''.join([u'{:02x}'.format(ord(c)) for c in hash_digest])

# def toBytes(uHash):
# 	return uHash.decode('hex')

def storeFile():
	fname=raw_input('file>')

	fnameHash=h(fname)
	print "fhash:",toUni(fnameHash)

	D['rootconn'].send('\x03'+fnameHash)
	try:
		f=open(fname,'r')
	except IOError as err:
		print "Could not open file (%s)"%(err)
		return
	else:
		contents=f.read()
		f.close()
	print "file contents:",contents
	msg=''

	try:
		msg=D['rootconn'].recv(TCP_MAX_SIZE)
	except socket.error as err:
		print "error when contacting root server (%s)"%(err)
	else: 
		if msg=='\x06': #can send file right to root server
			try:
				D['rootconn'].send('\x03'+contents)
				print 'OK to send, sending file"','\x03'+contents,'"'
				msg=''
				msg=D['rootconn'].recv(TCP_MAX_SIZE)
			except socket.error as err:
				print "error after sending file (%s)"%(err)
			else:
				if msg=='\x06':
					print "File stored successfully"
				else: 
					print "File was not stored"
		elif len(msg)>1 and msg[0]=='\x00': #root won;t take file, its sending back the right address
			pport,paddr = unpack('!i%ds'%(len(msg)-5),msg[1:])
			print "Root says to ask ",paddr,':',pport 
			temp=None
			try:
				temp=socket.create_connection( (paddr,pport), 10 )
				temp.sendall('\x03'+contents)
				temp.settimeout(5.0)
				msg=temp.recv(TCP_MAX_SIZE)
			except socket.error as err:
				print "Could not exchange with peer server (%s)"%(err)
				if temp is not None:
					temp.close()
			else:
				if msg=='\x06':
					print "File stored successfully"
				else: 
					print "File was not stored"

		else:
			print "Invalid response from server",msg
			D['rootconn'].close()
			exit(0)


def recursiveLookup():
	fname=raw_input('file>')

	fnameHash=h(fname)
	print "fhash:",toUni(fnameHash)

	D['rootconn'].send('\x04\x00'+fnameHash)

	msg=''
	try:
		msg=D['rootconn'].recv(TCP_MAX_SIZE)
	except socket.error as err:
		print "error when contacting root server (%s)"%(err)
	else: 
		if len(msg)>1 and msg[0]=='\x04': #root server had the file
			contents=msg[1:]
			print "Received file:",contents
			f=open(fname+str(os.getpid()),'w')
			f.write(contents)
			f.close()			
		elif msg == '\x08\x00\x00':	#file is not in the ring
			print "File is not in the ring"
		elif len(msg)>1 and msg[0]=='\x00': #root won;t take file, its sending back the right address
			pport,paddr = unpack('!i%ds'%(len(msg)-5),msg[1:])
			print "Root says to ask ",paddr,':',pport 
			temp=None
			try:
				temp=socket.create_connection( (paddr,pport), 10 )
				temp.settimeout(5.0)
				msg=temp.recv(TCP_MAX_SIZE)
			except socket.error as err:
				print "Could not exchange with peer server (%s)"%(err)
				if temp is not None:
					temp.close()
			else: 
				if len(msg)>1 and msg[0]=='\x04': #root server had the file
					contents=msg[1:]
					print "Received file:",contents
					f=open(fname+str(os.getpid()),'w')
					f.write(contents)
					f.close()
				elif msg == '\x08\x00\x00':
					print "File is not in the ring"
				else:
					print "Invalid response from peer server"
				temp.close()
		else:
			print "Invalid response from server"
			D['rootconn'].close()
			exit(0)

def iterativeLookup():
	fname=raw_input('file>')

	fnameHash=h(fname)
	print "fhash:",toUni(fnameHash)

	D['rootconn'].send('\x04\x01'+fnameHash)

	msg=''
	try:
		msg=D['rootconn'].recv(TCP_MAX_SIZE)
	except socket.error as err:
		print "error when contacting root server (%s)"%(err)
	else: 
		if len(msg)>1 and msg[0]=='\x04': #root server had the file
			contents=msg[1:]
			print "Received file:",contents
			f=open(fname+str(os.getpid()),'w')
			f.write(contents)
			f.close()			
		elif msg == '\x08\x00\x00':	#file is not in the ring
			print "File is not in the ring"
		elif len(msg)>1 and msg[0]=='\x00': #root won;t take file, its sending back the right address
			while True:
				pport,paddr = unpack('!i%ds'%(len(msg)-5),msg[1:])
				print "Last guy says to ask ",paddr,':',pport 
				temp=None
				try:
					temp=socket.create_connection( (paddr,pport), 10 )
					temp.settimeout(5.0)
					temp.sendall('\x04\x01'+fnameHash)
					msg=temp.recv(TCP_MAX_SIZE)
				except socket.error as err:
					print "Could not exchange with peer server (%s)"%(err)
					if temp is not None:
						temp.close()
				else: 
					if len(msg)>1 and msg[0]=='\x04': #root server had the file
						contents=msg[1:]
						print "Received file:",contents
						f=open(fname+str(os.getpid()),'w')
						f.write(contents)
						f.close()
						break
					elif msg == '\x08\x00\x00':
						print "File is not in the ring"
						break
					elif len(msg)>1 and msg[0]=='\x00':
						continue
					else:
						print "Invalid response from peer server"
						break
					temp.close()
		else:
			print "Invalid response from server"
			D['rootconn'].close()
			exit(0)

def printRing():
	D['rootconn'].send('\x05')

def exitClient():
	D['rootconn'].close()
	print 'Shuting down client'
	exit(0)

def killRing():
	D['rootconn'].send('\x09')
	print 'Sent kill command to root, exiting...'
	exitClient()

CMD={
	's':storeFile,
	'r':recursiveLookup,
	'i':iterativeLookup,
	'p':printRing,
	'k':killRing,
	'e':exitClient
}

for i in deepcopy(CMD).keys(): CMD[i.upper()]=CMD[i]

def mainLoop():
	D['problem']=0

	while not D['problem']:
		cmd = raw_input('(s)tore, (r)ecursive search, (i)terative search, (p)rint ring, (k)ill ring, (e)xit\n>')
		if len(cmd)>0 and cmd[0] in CMD:
			CMD[cmd[0]]()
		else:
			print "Could not recognize input: %s"%(cmd)

def main():
	args = sys.argv
	i=1

	# print "D:%s"%(D)

	if len(args)%2==0:
		print "Invalid number of arguments"
		exit(0)

	while i<len(args):
		if args[i] == '-p':
			D['my_port']=int(args[i+1])
		elif args[i] == '-h':
			D['my_name']=args[i+1]
		elif args[i] == '-R':
			D['root_port']=int(args[i+1])
		elif args[i] == '-r':
			D['root_name']=args[i+1]
		else: 
			print "error: invalid arguments"
			exit(0)
		i+=2

	# print "D:%s"%(D)

	#connect to root server
	try:
		D['rootconn']=socket.create_connection( (D['root_name'],D['root_port']), 10, (D['my_name'],D['my_port']) )
	except socket.error as err:
		print "Could not connect to server (%s)"%(err)
		exit(0)
		

	#send '\x01\x01' // client conn req
	D['rootconn'].send('\x01\x01')

	D['rootconn'].settimeout(10.0)
	msg=''

	try:
		msg=D['rootconn'].recv(TCP_MAX_SIZE)
	except socket.error as err:
		print "error on recv 6 from server (%s)"%(err)
	else:
		if msg == '\x06':
			print "Successfully conencted to server\nEntering main loop"
			mainLoop()
		else:
			print "No response from server\nTerminating"

	D['rootconn'].close()

if __name__ == '__main__':
	main()