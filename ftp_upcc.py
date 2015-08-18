#!/usr/bin/env python
# coding=utf-8

import os,time,re,sys,traceback,gzip,string,pickle
import socket
from ftplib import FTP
from optparse import OptionParser

class FtpFile(object):
	def __init__(self,emptyfile="/home/testftp/access.log.gz",uploadhour=2,anapath = "/home/testftp/"):
		self.emptyfile = str(emptyfile)
		self.uploadhour = int(uploadhour)
		self.anapath = str(anapath)
		self.dir_upfile = {}
		self.localday = time.strftime("%Y%m%d",time.localtime(time.time()-uploadhour*60*60))
		self.remoteday = time.strftime("%Y-%m-%d",time.localtime(time.time()-uploadhour*60*60))
		self.localhour = time.strftime("%H",time.localtime(time.time()-uploadhour*60*60))

	def check_arg(self):
		if not os.path.exists(self.anapath):
			return False
		return True

	def correction_time(self):
		if self.localhour == "00":
			self.localday = time.strftime("%Y%m%d",time.localtime(time.time()-(self.uploadhour +1)*60*60))
			self.remoteday = time.strftime("%Y-%m-%d",time.localtime(time.time()-(self.uploadhour +1)*60*60))
			self.localhour = "24"

	def get_updatefile(self):
		# keep clean in dir_upfile
		self.dir_upfile = {}
		# split local path
		str_serpath = self.anapath
		if str_serpath[-1] == ";":
			str_serpath = str_serpath[:-1]
		list_serpath= str_serpath.split(";")
		# correct time if hour is 00
		self.correction_time()
		if not self.check_arg():
			return self.dir_upfile
		# get local path and ftp path
		for file_path in list_serpath:
			file_daypath = os.path.join(file_path,self.localday)
			list_filename = self.get_filename(file_daypath)
			if not list_filename:
				continue 
			for filename in list_filename:
				local_file = os.path.join(file_daypath,filename)
				remote_file = self.get_remotefile(filename)
				if local_file and remote_file:
					self.dir_upfile.update({local_file:remote_file})
		return self.dir_upfile

	def get_filename(self,file_daypath):
		list_filename = []
		str_flag = ".*_%s.gz" %(self.localhour)
		if not os.path.exists(file_daypath):
			return list_filename
		list_daypath = os.listdir(file_daypath)
		for file_name in list_daypath:
			match = re.match(str_flag,file_name)
			if match:
				list_filename.append(file_name)
		return list_filename

	def get_remotefile(self,filename):
		list_pathpart = filename.split("_")
		if len(list_pathpart) != 4:
			return None
		remote_file = os.path.join(list_pathpart[0],self.remoteday,self.localhour,"access.log.gz")
		return remote_file

	def get_failedfile(self):
		# keep clean in dir_upfile
		self.dir_upfile = {}
		data_file = os.path.join(self.anapath,"data.pkl")
		if not os.path.exists(data_file):
			return self.dir_upfile
		with open(data_file,'rb') as datefd:
			self.dir_upfile = pickle.load(datefd)
		os.unlink(data_file)
		return self.dir_upfile

	def get_emptylog(self):
		if not os.path.exists(self.emptyfile):
			if 0 != os.path.getsize(self.emptyfile):
				fd_zip = gzip.open(self.emptyfile, 'wb')
				fd_zip.close()
		else:
			fd_zip = gzip.open(self.emptyfile, 'wb')
			fd_zip.close()
		return self.emptyfile

	def get_remotetime(self):
		dir_remotetime = {}
		if self.remoteday and self.localhour:
			dir_remotetime.setdefault(self.remoteday, [])
			dir_remotetime[self.remoteday].append(self.localhour)
		return dir_remotetime
		

class FtpClient(object):
	
	def __init__(self,user,passwd,serverip,port=21):
		self.serverip = str(serverip)
		self.port = int(port)
		self.user = str(user)
		self.passwd = str(passwd)
		self.ftp = None
		self.FTP_BASEDIR = '/'

	def ftp_connect(self):
		try:
			socket.setdefaulttimeout(60)
			self.ftp = FTP(self.serverip)
			self.ftp.login(self.user,self.passwd)
			return True
		except:
			return False

	def upload_handle(self,dir_upfile,dir_time,emptylog):
		try:
			dir_remotetime = self.get_failedempty(emptylog)
			for nowremoteday,nowremotelist in dir_time.iteritems():
				dir_remotetime[nowremoteday] = list(set(dir_remotetime.get(nowremoteday,[])+nowremotelist)) 
			file_result = self.upload_file(dir_upfile)
			#print "upload file:\n",dir_upfile,file_result
		
			emptylog_result = self.upload_emptylog(dir_remotetime,emptylog)
			#print "upload time:\n",dir_remotetime,emptylog_result
			if not file_result:
				self.save_failedfile(dir_upfile,emptylog)
			if not emptylog_result:
				self.save_failedempty(dir_remotetime,emptylog)
		except:
			self.save_failedfile(dir_upfile,emptylog)
			self.save_failedempty(dir_time,emptylog)
			

	def save_failedfile(self,dir_upfile,emptylog):
		file_path = os.path.dirname(emptylog)
		save_path = os.path.join(file_path,"data.pkl")
		with open(save_path,"wb") as savefd:
			pickle.dump(dir_upfile,savefd)

	def save_failedempty(self,dir_time,emptylog):
		file_path = os.path.dirname(emptylog)
		save_path = os.path.join(file_path,"time.pkl")
		with open(save_path,"wb") as savefd:
			pickle.dump(dir_time,savefd)

	def get_failedempty(self,emptylog):
		dir_time = {}
		file_path = os.path.dirname(emptylog)
		time_file = os.path.join(file_path,"time.pkl")
		if not os.path.exists(time_file):
			return dir_time
		with open(time_file,'rb') as datefd:
			dir_time = pickle.load(datefd)
		os.unlink(time_file)
		return dir_time

	def upload_file(self,dir_upfile):
		try:
			for localfile,remotefile in dir_upfile.iteritems():
				self.ftp.cwd(self.FTP_BASEDIR)
				self.clear_remotefile(remotefile)
				with open(localfile,'rb') as localfd:
					self.ftp.storbinary('STOR access.log.gz',localfd,10240)
			return True
		except:
			return False
	
	def upload_emptylog(self,dir_time,emptylog):
		try:
			for remoteday,remotehourlist in dir_time.iteritems():
				for remotehour in remotehourlist:
					self.ftp.cwd(self.FTP_BASEDIR)
					list_domainpath = self.ftp.nlst()
					for path in list_domainpath:
						self.check_emptylog(path,remoteday)
						self.check_emptylog(remoteday,remotehour)
						result = self.check_emptylog(remotehour,"access.log.gz",1)
						if result:
							emptydir = os.path.join(path,remoteday,remotehour,"access.log.gz")
							#print emptydir
							with open(emptylog,'rb') as localfd:
								self.ftp.storbinary('STOR access.log.gz',localfd,10240)
						self.ftp.cwd(self.FTP_BASEDIR)
			return True
		except:
			return False

	def check_emptylog(self,path,pathname,flag=0):
		self.ftp.cwd(path)
		list_path = self.ftp.nlst()
		if list_path:
			if pathname not in list_path:
				if flag:
					return pathname
				else:
					self.ftp.mkd(pathname)
		else:
			if flag:
				return pathname
			else:
				self.ftp.mkd(pathname)
		return None

	def clear_remotefile(self,remotefile):
		file_path = remotefile.split("/")
		if len(file_path) == 1:
			list_filename = self.ftp.nlst()
			if not list_filename:
				return 
			if remotefile in list_filename:
				self.ftp.delete(remotefile)
				return 
		list_pathname = self.ftp.nlst()
		if file_path[0] not in list_pathname:
			self.ftp.mkd(file_path[0])
		newfile_path = remotefile[(len(file_path[0])+1):]
		self.ftp.cwd(file_path[0])
		self.clear_remotefile(newfile_path)
		return 

	def quit_ftp(self):
		self.ftp.quit()

if __name__ == '__main__':
	# get before hour in argv
	nHourBrfore = 2
	parser = OptionParser()
	parser.add_option("-t","--thour",dest="num",
    	help="Upload FTP file time(hour)", metavar="2")
	(options,args) = parser.parse_args()
	if options.num:
		nHourBrfore = options.num

	# get files that need to upload
	ftpfile = FtpFile(uploadhour = nHourBrfore)
	dir_upfile = ftpfile.get_updatefile()
	dir_failedfile = ftpfile.get_failedfile()
	dir_upfile.update(dir_failedfile)
	dir_remotetime = ftpfile.get_remotetime()
	emptylog = ftpfile.get_emptylog()
	'''
	print "upfile:"
	for k,v in dir_upfile.iteritems():
		print ("\t" + k),v
	print "remotetime:"
	for m,n in dir_remotetime.iteritems():
		print ("\t" + m),n
	print ("emptylog:\n\t" + emptylog)
	'''
	# upload file and enptylog if los is none
	ftpclient = FtpClient("cdn_mzc","cDN_5337cLgM","175.6.15.160")
	if not ftpclient.ftp_connect():
		dir_lasttime = ftpclient.get_failedempty(emptylog)
		for lastremoteday,lastremotelist in dir_lasttime.iteritems():
			dir_remotetime[lastremoteday] = list(set(dir_remotetime.get(lastremoteday,[])+lastremotelist)) 
		ftpclient.save_failedfile(dir_upfile,emptylog)
		ftpclient.save_failedempty(dir_remotetime,emptylog)
		#print "connect failed"
		sys.exit() 
	ftpclient.upload_handle(dir_upfile,dir_remotetime,emptylog)
	ftpclient.quit_ftp()

	
	

