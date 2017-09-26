#!/usr/bin/python


####################################################################
#                                                                  #
#  SMAPBot: A blacklisted SMAP granule checker.                    #
#  Maintainer: Mike Laxer                                	   #
#  Date: 2017/07/10                                                #
#                                                                  #
#                                                                  #
#                                                                  #
#                                                                  #
#                                                                  #
#                                                                  #
#                                                                  #
#                                                                  #
#                                                                  #
#                                                                  #
####################################################################



import sys
import csv
import os
import io
import ConfigParser
import psycopg2
import psycopg2.extras
import smtplib
from datetime import datetime
import pprint
import subprocess


###------Start config file section------###
config = ConfigParser.RawConfigParser()
config.read('smapbot.cfg')

# Set database config variables
db = config.get('SQL','database')
user = config.get('SQL','user')
host = config.get('SQL','host')
port = config.get('SQL','port')
path = config.get('Paths','local')

# Set mail config variables
fromaddr = config.get('Mail', 'from')
toaddr = config.get('Mail', 'to')

# Set file i/o config variables
datalist = config.get('I/O','csv')
granuleids = config.get('I/O','granidfile') 
geoids = config.get('I/O','geoidfile') 
whitelist = config.get('I/O', 'whitelist')

# Set curl config variables
url = config.get('Urls','url')

###------End config file section------###


# Output file formatting
FORMAT = '%Y%m%d%H%M%S'
granfile = '%s.%s' % (granuleids, datetime.now().strftime(FORMAT))
geofile = '%s.%s' % (geoids, datetime.now().strftime(FORMAT))

# Input/Output file handling
f = open(datalist, 'rb+')
w = open(granfile, 'wb+')
g = open(geofile, 'wb+')
wlist = open(whitelist, 'rb+')

class smapbot():

	def csvGet(self):
		# retrieve csv file from SDS
		os.system("sh curlcsv.sh")

		
	def csvRead(self):
		self.orbits = []
		with open(datalist) as csvfile:
			self.readCSV = csv.DictReader(csvfile)
			self.csvData = []
			for row in self.readCSV:
				self.csvRow = {}
				self.csvRow = row
				if self.csvRow['DATA STREAM'] == 'Radar':
					pass
				else:
					self.csvData.append(self.csvRow)
		
		for orbit in self.csvData:
			# Deal with ranges of orbits...  >:/	
			self.hOrbit = orbit['HALF ORBIT']
			
			# Split ranges by dash '-'
			self.hOrbits = self.hOrbit.split("-")
			orbit['ORBIT1'] = self.hOrbits[0]

			# Check if we're working with a range:
			if len(self.hOrbits) > 1:
				orbit['ORBIT2'] = self.hOrbits[1]
				o1 = orbit['ORBIT1']
				o2 = orbit['ORBIT2']
				# Get orbit integers	
				orbit1 = int(o1[:-1])
				orbit2 = int(o2[:-1])
				# Get all orbits in the range, inclusive
				self.rangeOrbits_D = range(orbit1,orbit2+1)
				# Logic for half orbit ranges
				if o1[-1] == 'A' and o2[-1] == 'A':			
					self.rangeOrbits_A = self.rangeOrbits_D
					self.rangeOrbits_D = self.rangeOrbits_D[:-1]
				elif o1[-1] == 'A' and o2[-1] == 'D':
					self.rangeOrbits_A = self.rangeOrbits_D
				elif o1[-1] == 'D' and o2[-1] == 'A':
					self.rangeOrbits_A = self.rangeOrbits_D[1:]
					self.rangeOrbits_D = self.rangeOrbits_D[:-1]
				elif o1[-1] == 'D' and o2[-1] == 'D':
					self.rangeOrbits_A = self.rangeOrbits_D[1:]

				# Add half orbit value to each list element
				self.rangeOrbits_D = [str(d_orbit) + 'D' for d_orbit in self.rangeOrbits_D]
				self.rangeOrbits_A = [str(a_orbit) + 'A' for a_orbit in self.rangeOrbits_A]
				
				# Combine orbit lists into a single list
				self.rangeOrbits = self.rangeOrbits_A + self.rangeOrbits_D
				
				# Create new orbit dictionary for each new half orbit and add it to the master list
				for hOrbit in self.rangeOrbits:
					newOrbit = {}
					newOrbit['CRID'] = orbit['CRID']
					newOrbit['HALF ORBIT'] = hOrbit
					newOrbit['DATA STREAM'] = orbit['DATA STREAM']
					newOrbit['TYPE'] = orbit['TYPE']
					newOrbit['DATES AFFECTED'] = orbit['DATES AFFECTED']
					newOrbit['DESCRIPTION'] = orbit['DESCRIPTION']
					self.csvData.append(newOrbit)
						
			else:
				pass


			# format orbit numbers so they contain 5 digits:
			if len(orbit['HALF ORBIT']) == 4:
				orbit['HALF ORBIT'] = '00'+str(orbit['HALF ORBIT'])
			elif len(orbit['HALF ORBIT']) == 5:
				orbit['HALF ORBIT'] = '0'+str(orbit['HALF ORBIT'])
			else:
				pass

			# Add underscore between orbit number and orbit node:		
			orbit['HALF ORBIT'] = orbit['HALF ORBIT'].replace("A", "_A")
			orbit['HALF ORBIT'] = orbit['HALF ORBIT'].replace("D", "_D")
		
			# ignore ranges of orbits since we've taken care of them above:
			if len(orbit['HALF ORBIT']) > 7:
				pass
			else:
				# add orbits to list 'self.orbits'
				self.orbits.append(orbit)

	
	
	def psql(self):
		self.i = 1
		self.con = None
		self.dtype = "('SPL1BTB','SPL1CTB','SPL2SMP')"
		self.dtype_e = "('SPL1BTB_E','SPL1CTB_E','SPL2SMP_E')"
		self.deletes = []

		# Set up half orbit whitelist
		self.whitelist = []
		self.whitelist = wlist.readlines()
		
		# Remove trailing newline
		self.whitelist = [w.rstrip() for w in self.whitelist]

		for o in self.orbits:
			self.horbit = '%'+str(o['HALF ORBIT'])+'%'
			
			if len(o['CRID']) < 6:
				self.crid = '%'
				print '\n\nLooking for %s %s granules with orbit %s: \n' % (o['TYPE'], o['DATA STREAM'], o['HALF ORBIT'])
			else: 
				self.crid = '%'+str(o['CRID'])+'%'
				print '\n\nLooking for %s %s granules with CRID %s and orbit %s: \n' % (o['TYPE'], o['DATA STREAM'], o['CRID'], o['HALF ORBIT'])
			if o['DATA STREAM'] == 'Radiometer':
				self.collections = self.dtype
			else:
				self.collections = self.dtype_e
			
			# Check if half orbit is in the whitelist
			if o['HALF ORBIT'] in self.whitelist:
				pass
			else:
				try:
					self.start = 0
					self.con = psycopg2.connect(database=db, user=user, host=host, port=port)
					self.query = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)
					self.query.execute("""
						SELECT localgranuleid
						FROM aim.amgranule 
						WHERE amgranule.localgranuleid LIKE '%s' 	
						AND amgranule.localgranuleid LIKE '%s'
						AND amgranule.isorderonly is null
						AND amgranule.deleteeffectivedate is null
						AND shortname in %s""" % (self.crid, self.horbit, self.collections)
					)
					self.qout = self.query.fetchall()
					if len(self.qout) >= 1:
						for q in self.qout:
							q = q[0]
							print q
							self.deletes.append(q)
					else:
						print 'No granules found\n'
				except psycopg2.DatabaseError, e:
					print 'Error %s' % e
					sys.exit(1)
				finally:
					if self.con:
						self.con.close()

	
	def out(self):
		self.granids = []
		self.geoids = []
		self.count = len(self.deletes)
		for d in self.deletes:
			try:
				self.start = 0
				self.con = psycopg2.connect(database=db, user=user, host=host, port=port)
                                self.query = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)
                                self.query.execute("""
						SELECT granuleid from aim.amgranule
						WHERE amgranule.localgranuleid = '%s'
						""" % d)
				self.dout = self.query.fetchall()
				for e in self.dout:
					e = e[0]
					self.granids.append(e)
					w.write("%s\n" % e)	
			except psycopg2.DatabaseError, e:
                                print 'Error %s' % e
                                sys.exit(1)
			try: 
				self.start = 0
				self.con = psycopg2.connect(database=db, user=user, host=host, port=port)
                                self.query = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)
                                self.query.execute("""
                                                SELECT 'SC:'||shortname||'.00'||versionid||':'||granuleid from aim.amgranule
                                                WHERE amgranule.localgranuleid = '%s'
                                                """ % d)
                                self.dout = self.query.fetchall()
				for f in self.dout:
					f = f[0]
					self.geoids.append(f)
					g.write("%s\n" % f)
			except psycopg2.DatabaseError, e:
                                print 'Error %s' % e
                                sys.exit(1)
			finally:
                                if self.con:
                                        self.con.close()
		print "\n\nFound %s granules to remove\n" % self.count


	def mail(self):
		self.subject = 'SMAPBot: Bad/Missing Granules Report'
		if len(self.deletes) > 1:
			self.msg = ("From: %s\nTo: %s\nSubject: %s\nHello Meatbag,\n\n"
			"I found %s SMAP granules requiring your attention.  These granules "
			"have been flagged by SMAP SDS as bad or missing due to software, hardware, "
			"or environmental issues and should be deleted from our archive.  Verify that "
			"these granules should be indiscriminately DELETED and then run the following:"
			"\n\nOn n5dpl01:"
			"\n--------------\n"
			"/usr/ecs/OPS/CUSTOM/utilities/EcDlUnpublishStart.pl -mode OPS -f %s%s"
			"\n--------------\n\n"
			"On n5oml01:"
			"\n--------------\n/usr/ecs/OPS/CUSTOM/utilities/EcDsBulkDelete.pl -mode OPS -physical -geoidfile %s%s"
			"\n--------------\n\n\n\n"
			"If there are any questions about this report or SMAPBot, contact my organic meatbag maintainer at mike.laxer@nsidc.org") % (
				fromaddr, 
				toaddr,
				self.subject,
				len(self.deletes), 
				path, 
				granfile, 
				path, 
				geofile
			)
		else:
			self.msg = ("From: %s\nTo: %s\nSubject: %s\nHello Meatbag,\n\n"
			"I did not find any granules for you to eliminate today.  No action is necessary.\n\n\n\n"
			"If there are any questions about this report or SMAPBot, contact my organic meatbag maintainer at mike.laxer@nsidc.org") % (
				fromaddr,
			 	toaddr, 
				self.subject
			)

		try:
			smtpObj = smtplib.SMTP('localhost')
			smtpObj.sendmail(fromaddr, toaddr, self.msg)
		except:
			print "Unable to send mail"


def main():
	id=smapbot()
	id.csvGet()
	id.csvRead()
	id.psql()
	id.out()
	id.mail()


if __name__ == "__main__":
	main()
