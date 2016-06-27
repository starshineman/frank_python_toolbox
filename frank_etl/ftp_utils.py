#!/usr/bin/python2.6



import sys
import os
import json
from ftplib import FTP

_XFER_FILE = 'FILE'
_XFER_DIR = 'DIR'

def get_argv(args):
    if len(args) == 6:
        return True
    else:
        print "Please input parameters like these, ex: IP, PORT, USERNAME, PASSWORD, SRC_PATH. then re-run the script."
        return False


class Xfer(object):
    '''
    @note: upload local file or dirs recursively to ftp server
    '''
    def __init__(self):
        self.ftp = None
    
    def __del__(self):
        pass
    
    def setFtpParams(self, ip, port, uname, pwd, timeout = 60):        
        self.ip = ip
        self.uname = uname
        self.pwd = pwd
        self.port = port
        self.timeout = timeout
    
    def initEnv(self):
        if self.ftp is None:
            self.ftp = FTP()
            print '### connect ftp server: %s ...'%self.ip
            self.ftp.connect(self.ip, self.port, self.timeout)
            self.ftp.login(self.uname, self.pwd) 
            print self.ftp.getwelcome()
    
    def clearEnv(self):
        if self.ftp:
            self.ftp.close()
            print '### disconnect ftp server: %s!'%self.ip 
            self.ftp = None
    
    def uploadDir(self, localdir='./', remotedir='./'):
        if not os.path.isdir(localdir):  
            return
        self.ftp.cwd(remotedir) 
        for file in os.listdir(localdir):
            src = os.path.join(localdir, file)
            if os.path.isfile(src):
                self.uploadFile(src, file)
            elif os.path.isdir(src):
                try:  
                    self.ftp.mkd(file)  
                except:  
                    sys.stderr.write('the dir is exists %s'%file)
                self.uploadDir(src, file)
        self.ftp.cwd('..')
    
    def uploadFile(self, localpath, remotepath='./'):
        if not os.path.isfile(localpath):  
            return
        print '+++ upload %s to %s:%s'%(localpath, self.ip, remotepath)
        self.ftp.storbinary('STOR ' + remotepath, open(localpath, 'rb'))
    
    def __filetype(self, src):
        if os.path.isfile(src):
            index = src.rfind('\\')
            if index == -1:
                index = src.rfind('/')                
            return _XFER_FILE, src[index+1:]
        elif os.path.isdir(src):
            return _XFER_DIR, ''        
    
    def upload(self, src):
        filetype, filename = self.__filetype(src)
        
        self.initEnv()
        if filetype == _XFER_DIR:
            self.srcDir = src            
            self.uploadDir(self.srcDir)
        elif filetype == _XFER_FILE:
            self.uploadFile(src, filename)
        self.clearEnv() 
               

if __name__ == '__main__':
    if not get_argv(sys.argv):
     	sys.exit(1)
     
    ip = sys.argv[1]
    port = sys.argv[2]
    user = sys.argv[3]
    passwd = sys.argv[4]
    srcPath = sys.argv[5]
    
    xfer = Xfer()
    xfer.setFtpParams(ip, port, user, passwd)
    xfer.upload(srcPath)
