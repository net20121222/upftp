#!/usr/bin/env python
# coding=utf-8
import os,time,re,sys,traceback,gzip
import socket
from ftplib import FTP

EMPTY_FILE = "/home/ftpupcc/access.log.gz"
CUSTID = "/home/logs/00000108/"
FTP_SERVER = "127.0.0.1"
FTP_USER = "user"
FTP_PASSWORD = "password"
FTP_BASEDIR = '/'

def get_localfile():
	# make sure CUSTID has filepath
	if not CUSTID:
		return None

	dir_sendfile = {}
	str_custid = CUSTID
	if str_custid[-1] == ";":
		str_custid = str_custid[:-1]
	list_custid = str_custid.split(";")

	localday = time.strftime("%Y%m%d",time.localtime(time.time()-2*60*60))
	remoteday = time.strftime("%Y-%m-%d",time.localtime(time.time()-2*60*60))
	localhour = time.strftime("%H",time.localtime(time.time()-2*60*60))
	if localhour == "00":
		localday = time.strftime("%Y%m%d",time.localtime(time.time()-3*60*60))
		remoteday = time.strftime("%Y-%m-%d",time.localtime(time.time()-3*60*60))
		localhour = "24"
	for file_path in list_custid:
		checkpath = os.path.join(file_path,localday)
		list_checkfile = get_checkfile(checkpath,localhour)
		if not list_checkfile:
			continue 
		for send_file in list_checkfile:
			local_file = os.path.join(checkpath,send_file)
			remote_file = get_remotefile(send_file,localhour,remoteday)
			if local_file and remote_file:
				dir_sendfile.update({local_file:remote_file})
	return dir_sendfile

def get_checkfile(checkpath,checktime):
	list_OKfile = []
	str_flag = ".*_%s.gz" %(checktime)
	if not os.path.exists(checkpath):
		return None
	list_file = os.listdir(checkpath)
	for file_name in list_file:
		match = re.match(str_flag,file_name)
		if match:
			list_OKfile.append(file_name)
	return list_OKfile

def get_remotefile(send_file,localhour,remoteday):
	list_pathpart = send_file.split("_")
	if len(list_pathpart) != 4:
		return None
	remote_filepath = list_pathpart[0] + "/" + remoteday + "/" + localhour
	remote_file = os.path.join(remote_filepath,"access.log.gz")
	return remote_file

def up_file(dir_upfile):
	if not dir_upfile:
		return 
	socket.setdefaulttimeout(60)
	try:
		ftp = FTP(FTP_SERVER)
	except:
		print("ftp loginx failed to exit")
		sys.exit()
	try:
		ftp.login(FTP_USER,FTP_PASSWORD)
		for localfile,remotefile in dir_upfile.iteritems():
			ftp.cwd(FTP_BASEDIR)
			check_ftppath(remotefile,ftp)
			with open(localfile,'rb') as localfd:
				ftp.storbinary('STOR access.log.gz',localfd,10240)
		up_emptylog(ftp)
	except:
		print("ftp ana failed to exit")
        ftp.quit()
        sys.exit()
	ftp.quit()

def check_ftppath(remotefile,ftp):
	file_path = remotefile.split("/")
	if len(file_path) == 1:
		list_filename = ftp.nlst()
		if not list_filename:
			return 
		if remotefile in list_filename:
			ftp.delete(remotefile)
		return 
	list_pathname = ftp.nlst()
	if file_path[0] not in list_pathname:
		ftp.mkd(file_path[0])
	newfile_path = remotefile[(len(file_path[0])+1):]
	ftp.cwd(file_path[0])
	check_ftppath(newfile_path,ftp)
	return 

def up_emptylog(ftp):
	remoteday = time.strftime("%Y-%m-%d",time.localtime(time.time()-2*60*60))
	localhour = time.strftime("%H",time.localtime(time.time()-2*60*60))
	if localhour == "00":
		remoteday = time.strftime("%Y-%m-%d",time.localtime(time.time()-3*60*60))
		localhour = "24"
	ftp.cwd(FTP_BASEDIR)
	list_domainpath = ftp.nlst()
	for path in list_domainpath:
		check_log(ftp,path,remoteday)
		check_log(ftp,remoteday,localhour)
		result = check_log(ftp,localhour,"access.log.gz",1)
		if result:
			emptydir = os.path.join(path,remoteday,localhour,"access.log.gz")
			print emptydir
			with open(EMPTY_FILE,'rb') as localfd:
				ftp.storbinary('STOR access.log.gz',localfd,10240)
		ftp.cwd(FTP_BASEDIR)
	return 

def check_log(ftp,path,pathname,flag=0):
	ftp.cwd(path)
	list_path = ftp.nlst()
	if list_path:
		if pathname not in list_path:
			if flag:
				return pathname
			else:
				ftp.mkd(pathname)
	else:
		if flag:
			return pathname
		else:
			ftp.mkd(pathname)
	return None

def make_emptyfile():
	if os.path.exists(EMPTY_FILE):
		if 0 != os.path.getsize(EMPTY_FILE):
			fd_zip = gzip.open(EMPTY_FILE, 'wb')
			fd_zip.close()
	else:
		fd_zip = gzip.open(EMPTY_FILE, 'wb')
		fd_zip.close()

if __name__ == '__main__':
	dir_upfile = get_localfile()
	for k,v in dir_upfile.iteritems():
		print k,v
	up_file(dir_upfile)
	

	
