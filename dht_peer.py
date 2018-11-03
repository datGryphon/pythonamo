#!/usr/bin/env python
import sys
import signal
import socket
import select
import sqlite3 as sql
import hashlib
import os
from time import sleep
from random import randint
from struct import *

TCP_MAX_SIZE=1500

D = { 
	'root_name':'sombrero.ccs.neu.edu', 
	'root_port':15151,
	'mode':0,
	'my_name':'pilotka.ccs.neu.edu',
	'my_port':15151,
	'lower_bound':'\x00'*20,
	'upper_bound':'\x01'+ ('\x00'*20),
	'parIsRoot':0
}

def shutdown(signum=None,frame=None):
	print "\nShutting down %s"%('root' if D['mode'] else 'peer')

	D['db'].close()

	if 'child' in D:
		sock = D['child']
		try:
			sock.sendall('\x09')
			sock.shutdown(1)
			sock.close()
		except:
			pass
	if 'par' in D:
		sock = D['par']
		try:
			sock.sendall('\x09')
			sock.shutdown(1)
			sock.close()
		except:
			pass
	if 'servsock' in D:
		sock = D['servsock']
		sock.shutdown(1)
		sock.close()
	if 'inbound' in D:
		sock = D['inbound']
		sock.shutdown(1)
		sock.close()	
	if 'unidentifiedConns' in D:
		for sock in D['unidentifiedConns']:
			try:
				sock.sendall('\x09')
			except:
				pass
			sock.shutdown(1)
			sock.close()
	if 'clients' in D:
		for sock in D['clients']:
			try:
				sock.sendall('\x09')
				sock.shutdown(1)
				sock.close()
			except:
				pass
	exit(0)

signal.signal(signal.SIGINT,shutdown)

def connectDB():

	conn = sql.connect( ''.join([D['fileDir'],'indexDB_',str(os.getpid()),'.db']) )

	c = conn.cursor()

	c.execute('''DROP TABLE IF EXISTS storage''')

	c.execute('''CREATE TABLE storage (
			hash text primary key,
			file blob
		)''')

	conn.commit()

	D['db']=conn

def h(fname):
	return hashlib.sha1(fname).digest()

def toUni(hash_digest):
	return u''.join([u'{:02x}'.format(ord(c)) for c in hash_digest])

#compares two digests as numbers
def ltHash(h1,h2): #return true if h1 < h2
	if len(h1) > len(h2):
		return False
	elif len(h1) < len(h2):
		return True
	else:
		isGreater=0
		for c,d in zip(h1,h2):
			if d>c:
				break
			elif d<c:
				isGreater=1
				break
		return not isGreater

def storeFile(hash_digest,file):
	# print "Storing file: " + hash_digest
	c=D['db'].cursor()

	uHash = toUni(hash_digest)

	try:
		c.execute('''INSERT INTO storage VALUES (?,?)''',(uHash,sql.Binary(file)))
	except sql.IntegrityError as err:
		#already exists an item with that hash
		c.execute('''UPDATE storage SET file=? WHERE hash=?''',(sql.Binary(file),uHash))
	finally:
		D['db'].commit()

def getFile(hash_digest):
	# print "Getting file: " + hash_digest
	c=D['db'].cursor()
	uHash=toUni(hash_digest)
	c.execute('''SELECT * FROM storage WHERE hash=?''',(uHash,))
	r=c.fetchone()
	return '%s'%(r[1]) if r is not None else None

def allFiles():
	# print "Getting File list"
	c=D['db'].cursor()
	c.execute('''SELECT * FROM storage''')

	return [ (row[0].decode('hex'),'%s'%(row[1])) for row in c.fetchall() ]

def printRing():
	print "This node covers\n",toUni(D['lower_bound']),'__to__',toUni(D['upper_bound'])

	print "Ring Family{"
	if 'gchil' in D:
		print "GCHILD:", D['gchil']

	if 'child' in D:
		sock = D['child']
		print sock
		print "CHILD:",socket.gethostbyaddr(sock.getpeername()[0])[0],sock.getpeername()[1] 
		sock.sendall('\x05')

	if 'par' in D:
		sock = D['par']
		print sock
		print "PAR:",socket.gethostbyaddr(sock.getpeername()[0])[0],sock.getpeername()[1]
	if 'gpar' in D:
		print "GPAR:",D['gpar']

	print "}end Family"

def repairChild():
	#try to connect to grandchild with timeout 2
	D['child'].shutdown(1)
	D['child'].close()
	if 'gchil' in D:
		try:
			print "connecting to GCHIL"
			sock = socket.create_connection(D['gchil'],20)
		except socket.error as err:
			shutdown()
		else:
			D['upper_bound']=h(D['gchil'][0])
			D['child'] = sock
			if 'par' in D:
				addrs = pack('!i%ds'%(len(socket.gethostbyaddr(D['par'].getpeername()[0])[0])),D['my_port']+1,socket.gethostbyaddr(D['par'].getpeername()[0])[0])
			else:
				addrs=''
			D['child'].sendall('\x02\x01'+addrs)
			msg = D['child'].recv(TCP_MAX_SIZE)
			if len(msg)>1 and msg[:2]=='\x02\x03':
				if len(msg)>2:
					D['gchil']=unpack('!i%ds'%(len(msg)-6),msg[2:])[::-1]
			else:
				print 'invalid message from grandchild'
				shutdown()
	else:
		print "tail of the ring fell off, taking over for dropped node"
		D.pop('child')
		D['upper_bound']='\x01'+ ('\x00'*20)

def repairParent():
	#select on inbound port to see if there's a message from your new parent
	if 'gpar' in D:
		print "waiting for GPAR"
		D['par'].shutdown(1)
		D['par'].close()
		select.select([D['inbound']],[],[],0)
		canRead,canWrite,canErr = select.select([D['inbound']],[],[],20)

		if len(canRead)==1:
			print "CanRead from inbound!"
			D['par']=D['inbound'].accept()[0]
			msg = D['par'].recv(TCP_MAX_SIZE)
			print "msg from gpar:", msg
			if len(msg)>1 and msg[:2]=='\x02\x01':
				print "got valid msg from gpar"
				if len(msg)>2:
					D['gpar']=unpack('!i%ds'%(len(msg)-6),msg[2:])[::-1]
					print "new GPAR:", D['gpar']
				else:
					D.pop('gpar')
				if 'child' in D:
					addrs = pack('!i%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0])[0])),D['my_port']+1,socket.gethostbyaddr(D['child'].getpeername()[0])[0])
					print "sending new par his new gchil: ",addrs
				else:
					addrs=''
				D['par'].sendall( '\x02\x03' + addrs)
			else:
				print 'invalid message from grandparnet'
				shutdown()
		else:
			print "grandparent did not contact!"
			shutdown()
	else: 
		print "no grandparent to connect to! RING BROKEN!"
		shutdown()

def rootLoop():
	try:
		D['servsock'].bind((D['root_name'],D['root_port']))
		D['servsock'].listen(5)
	except socket.error as err:
		print "Could not bind/listen on socket for root (%s)"%(err)
		exit(0)

	D['problem']=0
	D['unidentifiedConns']=[]
	D['clients']=[]
	canRead=[]
	canWrite=[]
	canErr=[]
	rnd=1
	print 'Server (',D['root_name'],':',str(D['root_port']),') is ready'
	
	while not (D['problem']):

		sock=None

		#need to check for message from child incase there's a newgchild notification
		if 'child' in D:			
			canRead,canWrite,canErr = select.select([D['child']],[],[],0)

			if len(canRead)==1:
				sock=D['child']
				try:
					msg=sock.recv(TCP_MAX_SIZE)
				except socket.error as err:
					repairChild()
				else:
					print 'MSG from child:',toUni(msg[:2])
					if len(msg)==0:
						print "Child has closed connection (%s)"
						repairChild()
					elif len(msg)>2 and msg[:2]=='\x07\x04': #new gcchild notification
						D['gchil'] = unpack('!i%ds'%(len(msg)-6),msg[2:])[::-1]
						print "new GCHILD", D['gchil']
		
		#else, no emergencies look for new peer/client/req
		canRead,canWrite,canErr = select.select([D['servsock']],[],[],0)

		if len(canRead) == 1:
			tup = D['servsock'].accept()
			# sleep(15)
			print "New connection from " + tup[0].getpeername()[0] + ':' + str(tup[0].getpeername()[1]) + "\nadding to unidentified pool"
			D['unidentifiedConns'].append(tup[0])

		#check for new message from unidentified conns  
		canRead,canWrite,canErr = select.select(D['unidentifiedConns'],[],[],0)
		
		if len(canRead)>0:
			sock=canRead[randint(0,len(canRead)-1)]
			msg = sock.recv(TCP_MAX_SIZE)
			print "new message from unIDed peer",sock,"(%d): "%(len(msg)),"".join("{:02x}".format(ord(c)) for c in msg)
			if msg == '\x01\x01':	#found new client
				print "connection type requested: client at address "   + sock.getpeername()[0]
				D['unidentifiedConns'] = [ x for x in D['unidentifiedConns'][:] if x is not sock]
				D['clients'] = [sock] + [x for x in D['clients'][:]]
				sock.sendall('\x06')
				sock.settimeout(10.0)
			elif msg == '\x01\x02\x01':	#found new peer
				D['unidentifiedConns'] = [ x for x in D['unidentifiedConns'][:] if x is not sock]
				pname = h(socket.gethostbyaddr(sock.getpeername()[0])[0])
				print "connection type requested: peer at address "   + socket.gethostbyaddr(sock.getpeername()[0])[0] + "\nPeerID =>  " + "".join("{:02x}".format(ord(c)) for c in pname)
				if ltHash(D['lower_bound'],pname) and ltHash(pname,D['upper_bound']):
					print "this peer is my new child"
					sock.settimeout(10.0)
					msg=''
					try:
						sock.sendall('\x01\x02\x01') #im your parent
						msg=sock.recv(TCP_MAX_SIZE)
						if msg=='\x04':
							#need to send files to new child
							print "sending files to new child"

							#...need to start file-upload 
							#get all current files
							rows = allFiles()
							#find sublist that is greater than peer id
							toSend = [ row for row in rows if ltHash(pname,row[0]) and ltHash(row[0],D['upper_bound'])]
							print 'Need to send these files:\n',toSend

							#send each one to new child
							p=0
							for record in toSend:
								msg=pack('20s%ds'%(len(record[1])),record[0],record[1])
								sock.sendall('\x03'+msg)
								msg=sock.recv(TCP_MAX_SIZE)
								if msg!='\x06':
									p=1
									break;

							if p:
								print 'Could not transfer files\nGiving up on new child'
								sock.shutdown(1)
								sock.close()
							else:
								sock.sendall('\x06') #done sending files
								print "all files sent"
									
								#let new child know how many kids it needs
								n=chr((1 if 'gchil' in D else 0)+(1 if 'child' in D else 0))
								addrs=''
								
								paddr=socket.gethostbyaddr(sock.getpeername()[0])[0]
								pport=sock.getpeername()[1]

								if 'child' in D:
									#tell child he's being demoted
									D['child'].sendall('\x07\x01'+pack('!i%ds'%(len(paddr)),pport+1,paddr))
									#pack parent addr for new child
									addrs=pack('!i%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0])[0])), 
										D['child'].getpeername()[1]+1 , socket.gethostbyaddr(D['child'].getpeername()[0])[0] )
								#this part is not right.....
								#need to figure out whats going on!
								if n=='\x02':
									print "adding gpar to the end of addrs..."
									addrs= addrs + '\xff'*4 + pack('!i%ds'%(len(D['gchil'][0])), D['gchil'][1]+1 , D['gchil'][0] )

								print n+addrs

								sock.sendall(n+addrs)

								#wait for child to respond with all-clear or error
								msg=''
								msg=sock.recv(TCP_MAX_SIZE)
								if msg=='\x06':	#child successfully connected to new fam
									D['upper_bound']=pname
									if 'child' in D:
										D['gchil']=D['child']
										gcaddr=(socket.gethostbyaddr(D['gchil'].getpeername()[0])[0],D['gchil'].getpeername()[1])
										D['gchil'].shutdown(1)
										D['gchil'].close()	
										D['gchil']=gcaddr #save his address for later
										print 'new gchil address!',D['gchil']
									D['child']=sock
									printRing()
									# sock.sendall('\x05') #tell child to print ring
								else:
									print "child ring join failed"
									sock.shutdown(1)
									sock.close()
									if 'child' in D:
										D['child'].sendall('\x06') #just kidding
						else:
							print "Invalid msg from new child"
							sock.shutdown(1)
							sock.close()
					except socket.error as err:
						print "new child failed to respond (%s)"%(err)
				else: 
					#send message to child (need parent for <IP:PORT>)
					print "This is not my child because...",ltHash(D['lower_bound'],pname),ltHash(pname,D['upper_bound'])
					
					r=''
					while True:
							try:
								D['child'].sendall('\x10'+pname)	#Send whatever you have to child
								r=D['child'].recv(TCP_MAX_SIZE)
								if len(r) > 1 and r[0]=='\x00': 	#got a valid resp
									print "Child responded '", r,"'"
									break
							except socket.error as err:
								print "Could not contact child!!\nNeed to repair Family"
								repairChild()
					#handle response from child / send addr to new peer
					try:
						sock.sendall(r)
					except socket.error as err:
						print "couldnot send results to new peer"
						sock.shutdown(1)					
						sock.close()
			else:
				print "unIDed peer message invalid"
				D['unidentifiedConns'] = [ x for x in D['unidentifiedConns'][:] if x is not sock]
				sock.shutdown(1)
				sock.close()				

		#check for new client req
		canRead,canWrite,canErr = select.select(D['clients'],[],[],0.5)

		if len(canRead)>0:
			sock=canRead[randint(0,len(canRead)-1)]
			msg = sock.recv(TCP_MAX_SIZE)
			print "new message from client (%d): "%(len(msg)),"".join("{:02x}".format(ord(c)) for c in msg)
			if len(msg) == 0:
				print "Client closed connection"
				D['clients'] = [ x for x in D['clients'][:] if x is not sock]
				sock.shutdown(1)
				sock.close()
			elif len(msg) > 0:
				#Store file
				if len(msg)==21 and msg[0] == '\x03':
					fhash=msg[1:]
					print "Storing  file ",toUni(fhash)
					if ltHash(D['lower_bound'],fhash) and ltHash(fhash,D['upper_bound']):
						print "Root will store this file"
						try:
							sock.sendall('\x06')
							msg=''
							msg=sock.recv(TCP_MAX_SIZE)
						except socket.error as err:
							print 'Client didn\'t respond with file (%s)'%(err)
							D['clients'] = [ x for x in D['clients'][:] if x is not sock]
							sock.shutdown(1)
							sock.close()
						else: 
							print len(msg),'"',msg,'"'
							if len(msg) > 1 and msg[0]=='\x03':
								file=msg[1:]
								storeFile(fhash,file)
								sock.sendall('\x06')
							else: 
								print "Invalid message from client"
								D['clients'] = [ x for x in D['clients'][:] if x is not sock]
								sock.shutdown(1)
								sock.close()
					else:
						print "Need to find correct node to store file"
						if 'child' in D:
							r=''
							while True:
									try:
										D['child'].sendall(msg)	#Send whatever you have to child
										r=D['child'].recv(TCP_MAX_SIZE)
										if len(r) > 1 and r[0]=='\x00': #got a valid resp
											print "Child responded"
											break
									except socket.error as err:
										repairChild()
							#handle response from child
							try:
								sock.sendall(r)
							except socket.error as err:
								print "couldnot send results to client"
								D['clients'] = [ x for x in D['clients'][:] if x is not sock]			
								sock.shutdown(1)					
								sock.close()
				elif len(msg)==22 and msg[0] == '\x04':
					IorR=msg[1]
					fhash=msg[2:]
					print "Retreive file ",toUni(fhash)
					if ltHash(D['lower_bound'],fhash) and ltHash(fhash,D['upper_bound']):
						print "Root should have this file"
						contents =  getFile(fhash)
						if contents is not None:
							print "The file is in my DB!"
							try:
								sock.sendall('\x04'+contents)
							except socket.error as err:
								print "Could not send file to client" 
								D['clients'] = [ x for x in D['clients'][:] if x is not sock]
								sock.shutdown(1)
								sock.close()
							else: 
								print "File sent"
						else:
							print "Oh no! I don't have the file."
							try:
								sock.sendall('\x08\x00\x00')
							except socket.error as err:
								print "Could not send response to client" 
								D['clients'] = [ x for x in D['clients'][:] if x is not sock]
								sock.shutdown(1)
								sock.close()
					else:
						if IorR == '\x00':
							print "Need to find correct node to store file"
							r=''
							while True:
									try:
										D['child'].sendall(msg)	#Send whatever you have to child
										r=D['child'].recv(TCP_MAX_SIZE)
										if len(r) > 1 and (r[0]=='\x00' or r[0]=='\x08'): #got a valid resp
											print "Child responded '", r,"'"
											break
									except socket.error as err:
										repairChild()
							#handle response from child
							try:
								sock.sendall(r)
							except socket.error as err:
								print "couldnot send results to client"
								D['clients'] = [ x for x in D['clients'][:] if x is not sock]					
								sock.shutdown(1)			
								sock.close()
						else:
							print "don't care who has it, just ask my neighbor"
							try:
								sock.sendall('\x00'+pack('!i%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0])[0])),D['my_port']+1,socket.gethostbyaddr(D['child'].getpeername()[0])[0]))
							except socket.error as err:
								print "couldnot send results to client"
								D['clients'] = [ x for x in D['clients'][:] if x is not sock]					
								sock.shutdown(1)			
								sock.close()
				elif msg == '\x05':
					print "Print family..."
					if 'child' in D:
						printRing()
					else:
						print "I have no family :'("
				elif msg == '\x09':
					print "Shutdown command received"
					shutdown()
				else:
					print "Client message invalid"
					D['clients'] = [ x for x in D['clients'][:] if x is not sock]
					sock.shutdown(1)
					sock.close()					
		# print "ServerSock:",D['servsock']
		# print "Clients:%s\nUnknown:%s"%(D['clients'],D['unidentifiedConns'])
		# printRing()
		print '-'*25,"END-ROUND",rnd,'-'*30
		rnd+=1

def peerLoop():
	D['problem']=0
	canRead=[]
	canWrite=[]
	canErr=[]
	rnd=1
	print 'Peer (',D['my_name'],':',str(D['my_port']),') is ready:'
	
	while not D['problem']:

		sock=None

		if 'child' in D:			
			canRead,canWrite,canErr = select.select([D['child']],[],[],0)

			if len(canRead)==1:
				sock=D['child']
				try:
					msg=sock.recv(TCP_MAX_SIZE)
				except socket.error as err:
					repairChild()
				else:
					print 'MSG from child:',toUni(msg[:2])
					if len(msg)==0:
						repairChild()
					#send everything else back up stream
					elif msg[0]=='\x00' or msg[0]=='\x08':
						while True:
							try:
								D['par'].sendall(msg)
								break
							except socket.error as err:
								print "Cannot contact parnet!"
								repairParent()
					elif len(msg)>2 and msg[:2]=='\x07\x04': #new gcchild notification
						D['gchil'] = unpack('!i%ds'%(len(msg)-6),msg[2:])[::-1]
						print "new GCHILD", D['gchil']
		
		if 'par' in D:			
			canRead,canWrite,canErr = select.select([D['par']],[],[],0.5)

			if len(canRead)==1:
				sock=D['par']
				try:
					msg=sock.recv(TCP_MAX_SIZE)
				except socket.error as err:
					print "par has closed connection (%s)"%(err)
					repairParent()
				else:
					print 'MSG from par:',msg
					if msg == '':
						print "lost connection to parnet!!!"
						repairParent()
					elif msg == '\x09':
						print "received termination command from parent"
						shutdown()
					elif msg == '\x05':
						printRing()
					elif len(msg) > 1:
						if len(msg)==21 and msg[0] == '\x03':
							print "Can I store a file with hash ", msg[1:],'?'
							fhash = msg[1:]
							if ltHash(D['lower_bound'],fhash) and ltHash(fhash,D['upper_bound']):
								print "yes!!! sending my address back toward root"
								while True:
									try:
										D['par'].sendall('\x00'+pack('!i%ds'%(len(D['my_name'])),D['my_port']+1,D['my_name']))
										break
									except socket.error as err:
										repairParent()
								print "Waiting for client connection"
								#wait for client to connect
								canRead,canWrite,canErr = select.select([D['inbound']],[],[],5.0)
								client=None
								if len(canRead)==1:
									print "Client inbound!"
									try:
										client,caddr=D['inbound'].accept()
										client.settimeout(10.0)
										msg=client.recv(TCP_MAX_SIZE)
									except socket.error as err:
										print "Could not get file from client (%s)"%(err)
										if client is not None:
											client.shutdown(1)
											client.close()
									else:
										if len(msg)>1 and msg[0]=='\x03':
											contents=msg[1:]
											print "received file contents: ",contents
											storeFile(fhash,contents)
											try:
												client.sendall('\x06')
												sleep(0.1)
												client.shutdown(1)
												client.close()
											except socket.error as err:
												print "Error when reporting results to client (%s)"%(err)
										else:
											print "invalid client message"
											client.shutdown(1)
											client.close()
							else: 
								print "No!! I won't take it"
								if 'child' in D:
									while True:
										try:
											D['child'].sendall(msg)	#Send whatever you have to child
											break
										except socket.error as err:
											repairChild()

								#Handle response from child above
						elif len(msg)==22 and msg[0] == '\x04':
							print "Root is looking for file ",msg[2:]
							fhash = msg[2:]
							if ltHash(D['lower_bound'],fhash) and ltHash(fhash,D['upper_bound']):
								print "I should have it..."
								contents=getFile(fhash)
								while True:
									try:
										if contents is not None:
											D['par'].sendall('\x00'+pack('!i%ds'%(len(D['my_name'])),D['my_port']+1,D['my_name']))
										else: 
											print "But its not. =>",'\x08\x00\x00'
											D['par'].sendall('\x08\x00\x00') #Its not in the ring
										break
									except socket.error as err:
										repairParent()
								if contents is not None:
									print "sent address to root, waiting for client"
									#wait for client to connect
									canRead,canWrite,canErr = select.select([D['inbound']],[],[],5.0)
									client=None
									if len(canRead)==1:
										print "Client inbound!"
										try:
											client,caddr=D['inbound'].accept()
											client.sendall('\x04'+contents)
										except socket.error as err:
											print "Could send file to client (%s)"%(err)
											if client is not None:
												client.shutdown(1)
												client.close()
										else:
											sleep(0.1)
											client.shutdown(1)
											client.close()
							else: # need to forward message to child
								if 'child' in D:
									while True:
										try:
											D['child'].sendall(msg)
											break
										except socket.error as err:
											repairChild()
						elif msg[:2]=='\x07\x01': #new parent notification
							newPar = unpack('!i%ds'%(len(msg)-6),msg[2:])[::-1]
							print "new parent at ",newPar
							try: 
								newGpar = (socket.gethostbyaddr(D['par'].getpeername()[0])[0], D['par'].getpeername()[1])
								newSock = socket.create_connection(newPar,4) #connect to new parent
								D['par'].shutdown(1)
								D['par'].close()
								D['par'] = newSock
								D['gpar'] = newGpar
								if 'child' in D:
									while True:
										try:
											D['child'].sendall('\x07\x02'+ pack('!i%ds'%(len(newPar[0])), newPar[1], newPar[0]))
											break
										except socket.error as err:
											repairChild()
							except socket.error as err:
								print "Could not connect to new parnent (%s)"%(err)
							else: 
								print "new conn established, switched to new parent, child notified"
						elif msg[:2]=='\x07\x02': #new grandparent notification
							D['gpar'] = unpack('!i%ds'%(len(msg)-6),msg[2:])[::-1]
							print "new GPAR", D['gpar']
						elif msg[0] == '\x10':
							print "new child trying to join the ring at index", toUni(msg[1:])
							pname=msg[1:]
							#if hash is in my bounds
							if ltHash(D['lower_bound'],pname) and ltHash(pname,D['upper_bound']):
								#send back \x00 with my address
								while True:
									try:
										sock.send('\x00'+pack('!i%ds'%(len(D['my_name'])), D['my_port']+1, D['my_name']))
										break
									except socket.error as err:
										repairParent()

								#listen for new connection from peer
								canRead,canWrite,canErr = select.select([D['inbound']],[],[],5)

								if len(canRead)==1:
									newSock = D['inbound'].accept()[0]
									rows = allFiles()
									toSend = [ row for row in rows if ltHash(pname,row[0]) and ltHash(row[0],D['upper_bound'])]
									print 'Need to send these files:\n',toSend

									p=0
									for record in toSend:
										msg=pack('20s%ds'%(len(record[1])),record[0],record[1])
										newSock.sendall('\x03'+msg)
										msg=newSock.recv(TCP_MAX_SIZE)
										if msg!='\x06':
											p=1
											break;

									if p:
										print 'Could not transfer files\nGiving up on new child'
										newSock.shutdown(1)
										newSock.close()
									else:
										newSock.sendall('\x06') #done sending files
										print "all files sent"

									msg=newSock.recv(TCP_MAX_SIZE)
									if msg == '\x06':
										paddr=socket.gethostbyaddr(newSock.getpeername()[0])[0]
										pport=newSock.getpeername()[1]-1
								
										n=chr((1 if 'child' in D else 0)+(1 if 'gchil' in D else 0)+(1 if 'par' in D else 0))

										addrs=''
										if n=='\x00':
											pass
										elif n=='\x01':
											if 'child' in D:
												#need to tell child he has a new parent
												D['child'].sendall('\x07\x01'+pack('!i%ds'%(len(paddr)),pport,paddr))
												addrs='c' + pack('!i%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0])[0])),
													D['child'].getpeername()[1],socket.gethostbyaddr(D['child'].getpeername()[0])[0])
											else:
												#need to tell parent he has a new gchild
												D['par'].sendall('\x07\x04'+pack('!i%ds'%(len(paddr)),pport,paddr))
												addrs='p' + pack('!i%ds'%(len(socket.gethostbyaddr(D['par'].getpeername()[0])[0])),
													D['par'].getpeername()[1],socket.gethostbyaddr(D['par'].getpeername()[0])[0])
										elif n=='\x02':
											if 'gchil' in D:
												#need to tell child he has a new parent  #he will tell his child he has new gp
												D['child'].sendall('\x07\x01'+pack('!i%ds'%(len(paddr)),pport,paddr))
												addrs='c' + pack('!i%ds4si%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0],)[0]), len(D['gchil'][0])),
													D['child'].getpeername()[1],socket.gethostbyaddr(D['child'].getpeername()[0])[0],
													'\xff'*4 , D['gchil'][1] , D['gchil'][0])
											else:
												#need to tell child he has new parent and tell parent he has new gchil
												D['child'].sendall('\x07\x01'+pack('!i%ds'%(len(paddr)),pport,paddr))
												D['par'].sendall('\x07\x04'+pack('!i%ds'%(len(paddr)),pport,paddr))
												addrs='p' + pack('!i%ds4si%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0],)[0]), len(socket.gethostbyaddr(D['par'].getpeername()[0])[0])),
													D['child'].getpeername()[1],socket.gethostbyaddr(D['child'].getpeername()[0])[0],
													'\xff'*4 ,D['par'].getpeername()[1],socket.gethostbyaddr(D['par'].getpeername()[0])[0])
										else:
											#neet to tell child he has new parent, he will tell his child about new gp
											#need to tell parent he has new gc
											D['child'].sendall('\x07\x01'+pack('!i%ds'%(len(paddr)),pport,paddr))
											D['par'].sendall('\x07\x04'+pack('!i%ds'%(len(paddr)),pport,paddr))
											addrs = pack('!i%ds4si%ds4si%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0],)[0]), len(D['gchil'][0]),len(socket.gethostbyaddr(D['par'].getpeername()[0])[0]) ),
												D['child'].getpeername()[1],socket.gethostbyaddr(D['child'].getpeername()[0])[0], '\xff'*4,
												D['gchil'][1] , D['gchil'][0], '\xff'*4,
												D['par'].getpeername()[1],socket.gethostbyaddr(D['par'].getpeername()[0])[0])

										#send family address to child
										newSock.sendall(n+addrs)

										#wait for all clear from child //new gchil connected to new child
										msg=newSock.recv(TCP_MAX_SIZE)
										if msg=='\x06':	#child successfully connected to new fam
											D['upper_bound']=pname
											if 'child' in D:
												D['gchil']=D['child']
												gcaddr=(socket.gethostbyaddr(D['gchil'].getpeername()[0])[0],D['gchil'].getpeername()[1])
												D['gchil'].shutdown(1)
												D['gchil'].close()	
												D['gchil']=gcaddr #save his address for later
												print 'new gchil address!',D['gchil']
											D['child']=newSock
											printRing()
										else:
											print "child ring join failed"
											newSock.shutdown(1)
											newSock.close()
											if 'child' in D:
												D['child'].sendall('\x06') #just kidding
												D['par'].sendall('\x07\x04' + pack('!i%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0])[0])),
													D['child'].getpeername()[1],socket.gethostbyaddr(D['child'].getpeername()[0])[0]))
									else:
										newSock.shutdown(1)
										newSock.close()
										print "child ring join failed"
							else:
								print "This is not my child because...",ltHash(D['lower_bound'],pname),ltHash(pname,D['upper_bound'])
								while True:
										try:
											D['child'].sendall('\x10'+pname)	#Send whatever you have to child
											break
										except socket.error as err:
											repairChild()
					else:
						print "Par message invalid syntax"
						D['par'].shutdown(1)
						D['par'].close()
						repairParent()

		canRead,canWrite,canErr = select.select([D['inbound']],[],[],0)

		if len(canRead) == 1:
			print "inbound client connection!"
			client = D['inbound'].accept()[0]
			client.settimeout(10)
			try:
				msg=client.recv(TCP_MAX_SIZE)
			except socket.error as err:
				print "Error when recving from client (%s)"%(err)
				client.shutdown(1)
				client.close()
			else: 
				if len(msg) >1 and msg[0]=='\x04':
					if ltHash(D['lower_bound'],fhash) and ltHash(fhash,D['upper_bound']):
						print "I should have it..."
						contents=getFile(fhash)
						if contents is not None:
							client.sendall('\x04'+ contents)
						else:
							client.sendall('\x08\x00\x00')
					else:
						client.sendall('\x00'+pack('!i%ds'%(len(socket.gethostbyaddr(D['child'].getpeername()[0])[0])),
							D['my_port']+1,socket.gethostbyaddr(D['child'].getpeername()[0])[0]))
					client.shutdown(1)
					client.close()


		print '-'*25,"END-ROUND",rnd,'-'*30
		rnd+=1

def bootstrap():
	haveParent=0
	D['par'].settimeout(10.0)
	sock=D['par']
	sock.sendall('\x01\x02\x01')#are you my parent?
	D['parIsRoot']=0
	msg=''
	try:
		msg=sock.recv(TCP_MAX_SIZE)
	except socket.error as err:
		print "error on recv from server (%s)"%(err)
		sock.shutdown(1)
		sock.close()
		exit(0)
	else:
		if msg == '\x01\x02\x01':	
			haveParent=1

			if socket.gethostbyaddr(sock.getpeername()[0])[0] == D['root_name']:
				print 'Parent is root!!!'
				D['parIsRoot']=1

			print "found parent!"
			msg=''
			sock.sendall('\x04')#give me your files

			inbound = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			inbound.bind((D['my_name'],D['my_port']+1))
			inbound.settimeout(10.0)#
			inbound.listen(5)
			print "bound listening sock to port ",D['my_port']+1

			p=0
			while not p:
				msg=sock.recv(TCP_MAX_SIZE)
				if msg=='\x06':
					print 'All files received\n',allFiles()
					p=1
				elif len(msg)>21 and msg[:1]=='\x03':
					print 'new file'
					#unpack "hash,file"
					fhash,contents = unpack('20s%ds'%(len(msg)-21),msg[1:] )
					if ltHash(D['lower_bound'],fhash) and ltHash(fhash,D['upper_bound']):
						storeFile(fhash,contents)
						sock.sendall('\x06')
					else:
						print 'this file is outside of my bounds!'
					#store hash,file
					# sock.send('\x06')
				else:
					print "invalid msg"
					p=-1
		
			if p==-1:
				sock.shutdown(1)
				sock.close()
				exit(0)
				
			msg=''
			msg=sock.recv(TCP_MAX_SIZE)

			print "Server sent family info", msg
			D['inbound']=inbound

			#listen for new family members
			if len(msg)>0 and ord(msg[0])<3:
				
				if msg[0]=='\x01':
					#only one addr to unpack
					caddr = unpack('!i%ds'%(len(msg)-5),msg[1:] )[::-1]
					print "new child ", caddr

				if msg[0]=='\x02':
					p1 = msg[1:msg.rfind('\xff'*4)]
					caddr = unpack('!i%ds'%(len(p1)-4),p1 )[::-1]
					p2 = msg[msg.rfind('\xff'*4)+4:]
					gcaddr = unpack('!i%ds'%(len(p2)-4),p2 )[::-1]
					print "new child", caddr, '\nnew grandchild', gcaddr
					D['gchil'] = gcaddr

				if ord(msg[0])>0:
					#get connection from new child
					canRead,canWrite,canErr = select.select([D['inbound']],[],[],4.0)

					if len(canRead) ==1:
						D['child']=D['inbound'].accept()[0]
						D['upper_bound']=h(socket.gethostbyaddr(caddr[0])[0])
						print "connect to child with ID => ",toUni(D['upper_bound'])
					else: 
						print "child didn't connect!! abort!"
						shutdown()

				sock.sendall('\x06')
				print "Successfully conencted to ring\nEntering main loop"
				peerLoop()

			else:
				print "Invalid message"
				sock.shutdown(1)
				sock.close()
				shutdown()
		elif len(msg) >1 and msg[:1]=='\x00':
			print "This is not my parent, next I'll try ",unpack('!i%ds'%(len(msg)-5), msg[1:])[::-1]

			#close parent connection
			sock.shutdown(1)
			sock.close()

			#create connection to new parent
			D['par'] = socket.create_connection(unpack('!i%ds'%(len(msg)-5), msg[1:])[::-1], 5, (D['my_name'],D['my_port']+2))	
			D['par'].settimeout(10.0)	
			D['par'].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			print "found parent!"
			sock = D['par']

			#get files from new parent
			msg=''

			inbound = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			inbound.bind((D['my_name'],D['my_port']+1))
			inbound.settimeout(10.0)#
			inbound.listen(5)
			print "bound listening sock to port ",D['my_port']+1

			p=0
			while not p:
				msg=sock.recv(TCP_MAX_SIZE)
				print "msg:",msg
				if msg=='\x06':
					print 'All files received\n',allFiles()
					p=1
				elif len(msg)>21 and msg[:1]=='\x03':
					print 'new file'
					#unpack "hash,file"
					fhash,contents = unpack('20s%ds'%(len(msg)-21),msg[1:] )
					if ltHash(D['lower_bound'],fhash) and ltHash(fhash,D['upper_bound']):
						storeFile(fhash,contents)
						sock.sendall('\x06')
					else:
						print 'this file is outside of my bounds!'
				else:
					print "invalid msg"
					p=-1
		
			if p==-1:
				sock.shutdown(1)
				sock.close()
				exit(0)
				
			sock.sendall('\x06')
			msg=''
			msg=sock.recv(TCP_MAX_SIZE)

			print "Server sent family info", msg
			D['inbound']=inbound

			if len(msg)>0 and ord(msg[0])<4:
				
				caddr=None
				gpaddr=None
				gcaddr=None

				#only one addr to unpack
				if msg[0]=='\x01':
					#check if it is a child address or a grandparent address
					if msg[1] == 'c':
						caddr = unpack('!i%ds'%(len(msg)-6),msg[2:] )[::-1]
						print "new child ", caddr
					else:
						gpaddr = unpack('!i%ds'%(len(msg)-6),msg[2:] )[::-1]
						print "new grandparent ", gpaddr

				if msg[0]=='\x02':
					p1 = msg[2:msg.rfind('\xff'*4)]
					caddr = unpack('!i%ds'%(len(p1)-4),p1 )[::-1]
					p2 = msg[msg.rfind('\xff'*4)+4:]
					print "new child", caddr
					if msg[1] == 'c':
						gcaddr = unpack('!i%ds'%(len(p2)-4),p2 )[::-1]
						print 'new grandchild', gcaddr
						D['gchil'] = gcaddr
					else:
						gpaddr = unpack('!i%ds'%(len(p2)-4),p2 )[::-1]
						print 'new grandparent', gcaddr
						D['gpar'] = gpaddr

				if msg[0]=='\x03':
					p1 = msg[2:msg.find('\xff'*4)]	#before first ffffffff
					print "new child", caddr, 
					caddr = unpack('!i%ds'%(len(p1)-4),p1 )[::-1]
					p2 = msg[msg.find('\xff'*4)+4:msg.rfind('\xff'*4)] #between two ffffffff
					print 'new grandchild', gcaddr
					D['gchil']=gcaddr = unpack('!i%ds'%(len(p2)-4),p2 )[::-1]
					p3 = msg[msg.rfind('\xff'*4)+4:]	#after second ffffffff
					print p3
					D['gpar']=gpaddr = unpack('!i%ds'%(len(p3)-4),p3 )[::-1]
					print 'new grandparent', gcaddr

				if caddr is not None:
					#get connection from new child
					canRead,canWrite,canErr = select.select([D['inbound']],[],[],4.0)

					if len(canRead) ==1:
						D['child']=D['inbound'].accept()[0]
						D['upper_bound']=h(socket.gethostbyaddr(D['child'].getpeername()[0])[0])
						print "connect to child with ID => ",toUni(D['upper_bound'])
					else: 
						print "child didn't connect!! abort!"
						shutdown()

				if gcaddr is not None:
					D['gchil'] = gcaddr

				if gpaddr is not None:
					D['gpar'] = gpaddr

				sock.sendall('\x06')
				print "Successfully conencted to ring\nEntering main loop"
				peerLoop()

			else:
				print "Invalid message"
				sock.shutdown(1)
				sock.close()
				shutdown()
		else:
			print "Invalid message"
	
def main():
	args = sys.argv
	i=1

	# print "D:%s"%(D)

	if len(args)%2==0:
		print "Invalid number of arguments"
		exit(0)

	while i<len(args):
		if args[i] == '-m':
			D['mode']=int(args[i+1])
		elif args[i] == '-p':
			D['my_port']=int(args[i+1])
		elif args[i] == '-h':
			D['my_name']=args[i+1]
		elif args[i] == '-R':
			D['root_port']=int(args[i+1])
		elif args[i] == '-r':
			D['root_name']=args[i+1]
		i+=2

	# print "D:%s"%(D)

	if D['mode']:
		D['fileDir']= ''.join(['./rootFiles',str(os.getpid()),'/'])
		os.makedirs(D['fileDir'])

		# print "made root folder"

		D['root_port']=D['my_port']
		D['root_name']=D['my_name']
		connectDB()

		# print "DB connected"
		try:
			D['servsock'] = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		except socket.error as err:
			print "Root failed to create socket (%s)" %(err)
			exit(0)
		finally:
			# print"Socket created, entering main loop..."
			rootLoop()
	else:
		print 'Peer IP:Port => ',D['my_name'],':',D['my_port']
		# print ''.join([D['my_name'],str(D['my_port'])])
		D['lower_bound']=h(D['my_name'])
		print 'Peer ID => ', toUni(D['lower_bound'])
		D['fileDir']= ''.join(['./peerFiles',str(os.getpid()),'/'])
		os.makedirs(D['fileDir'])

		connectDB()

		try:
			D['par']=socket.create_connection( (D['root_name'],D['root_port']), 10, (D['my_name'],D['my_port']) )
			D['par'].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		except socket.error as err:
			print "Peer failed to create connection to root server (%s)" %(err)
			exit(0)
		
		bootstrap()
		
if __name__ == '__main__':
	main()