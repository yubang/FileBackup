#coding:UTF-8

"""
把备份文件发送到远程服务器
"""

import os,sqlite3,logging,ftplib
from config import server,base


def executeSql(path,sql,selectSign):
    "执行sql语句"
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute(sql)
    if selectSign :
        r=cursor.fetchall()
    else:
        r=cursor.rowcount
    cursor.close()
    conn.commit()
    conn.close()
    return r
    
    
def getDirPath(name):
    "获取绝对位置"
    dirPath=os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dirPath,name)


def getFilePathFromMd5(md5):
    "根据md5获取文件在本地的位置"
    path = getDirPath("data/fileWarehouse")
    if not os.path.exists(path):
        os.makedirs(path)
    for i in range(0,5):
        path=os.path.join(path,md5[i*8:i*8+8])
    return os.path.dirname(path)   


def sendFileUseFtp(serverHost,serverPort,serverUser,serverPassword,filePath,remotePath):
    "发送文件"
    result = True
    ftp=ftplib.FTP()
    ftp.connect(serverHost,serverPort)
    r=ftp.login(serverUser,serverPassword)
    if r == "230 Login successful.":
        fp = open(filePath,"r")
        #递归在ftp服务器创建文件夹
        remotePaths=remotePath.split("/")
        tempPath="."
        for t in remotePaths:
            tempPath+="/"+t
            try:
                ftp.mkd(tempPath)
            except:
                logging.info(u"在ftp服务器（%s）创建文件夹失败"%(serverHost))
            
        try:
            ftp.cwd(remotePath)
            ftp.storbinary('STOR %s' % os.path.basename(filePath),fp,1024)
        except:
            logging.error(u"在ftp服务器（%s）上传文件失败"%(serverHost))
            result=False
        fp.close()
    else:
        result=False
    ftp.quit()
    return result
    

def handle(filePath,remotePath):
    "处理要上传的文件"
    result=True
    for obj in server.serverLists:
        remotePath=os.path.join(obj['data']['ftpRootPrefix'],remotePath)
        try:
            r=sendFileUseFtp(obj['data']['ftpHost'],obj['data']['ftpPort'],obj['data']['ftpUser'],obj['data']['ftpPassword'],filePath,remotePath)
        except Exception,e:
            r=False
            logging.error(str(e))
            
        if not r:
            result = False
            logging.error(u"文件（%s）上传服务器（%s）失败！"%(filePath,obj['data']['ftpHost']))
        else:
            logging.info(u"文件（%s）上传服务器（%s）成功！"%(filePath,obj['data']['ftpHost']))
    return result        
    
    
def main():
    "主函数"
    
    logging.basicConfig(filename=getDirPath("log/sendFile.log"),level = logging.NOTSET, format = '%(asctime)s - %(levelname)s: %(message)s')
    
    while True:
        objs=executeSql(getDirPath("data/wait.db"),"select * from files where status = 0 limit 10",True)
        
        if len(objs) == 0:
            break
        
        for obj in objs:
            remotePath="./warehouse"
            for i in range(0,4):
                remotePath=os.path.join(remotePath,obj[0][i*8:i*8+8])
        
            remotePath=os.path.dirname(remotePath)
        
            r=handle(getFilePathFromMd5(obj[0]),remotePath)
            if r:
                executeSql(getDirPath("data/wait.db"),"update files set status = 1 where md5 = '%s'"%(obj[0]),False)
            else:
                executeSql(getDirPath("data/wait.db"),"update files set status = -1 where md5 = '%s'"%(obj[0]),False)
        
    #上传备份文件列表
    remotePath="./backup"
    path=getDirPath("data/backup")
    fps = os.listdir(path)
    for fp in fps:
        handle(os.path.join(path,fp),remotePath)
    
            
if __name__ == "__main__":
    main()
      
