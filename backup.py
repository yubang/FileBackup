#coding:UTF-8

"""
备份工具模块
@author:yubang
2015-05-29
"""

import os,time,json,md5,shutil,sqlite3
from config import base

lastCheckTime=0
nowCheckTime=float(time.time())
backupVersion=0
backupPrefix=""

def sqlDeal(value):
    value=value.replace("'","\\'").replace("\"","\\\"")
    return value


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


def getBackupVersion():
    "获取备份的版本号"
    global backupVersion,backupPrefix
    path=getDirPath("data/backup")
    if not os.path.exists(path):
        os.makedirs(path)
    while True:
        filePath=path+"/"+backupPrefix+str(backupVersion)
        if not os.path.exists(filePath):
            break
        backupVersion+=1
      
        
def getFileMd5(filePath):
    "获取文件md5值"
    m = md5.new()
    fp=open(filePath,"r")
    while True:
        d = fp.read(8096)
        if not d:
            break
        m.update(d)
    fp.close()
    return m.hexdigest()


def buildMd5Lists(sourcePath,p="."):
    "生成md5列表"
    global backupVersion,backupPrefix
    path=getDirPath("data/backup")
    path=os.path.join(path,backupPrefix+str(backupVersion))
    if not os.path.exists(sourcePath):
        return None
    if os.path.isdir(sourcePath):
        fps = os.listdir(sourcePath)
        for fp in fps:
            buildMd5Lists(os.path.join(sourcePath,fp),os.path.join(p,fp))
    else:
        md5=getFileMd5(sourcePath)
        r={"md5":md5,"path":p,"realPath":sourcePath}
        fp = open(path,"a")
        fp.write(json.dumps(r))
        fp.write("\n")
        fp.close()
        handleFile(sourcePath,md5)

def handleFile(sourcePath,md5):
    "处理md5文件"
    if not checkFileExistWarehouse(md5):
        loginNewFile(md5)
        copyFileToWarehouse(sourcePath,md5)
        
        
def getFilePathFromMd5(md5):
    "根据md5获取文件在本地的位置"
    path = getDirPath("data/fileWarehouse")
    if not os.path.exists(path):
        os.makedirs(path)
    for i in range(0,5):
        path=os.path.join(path,md5[i*8:i*8+8])
    return os.path.dirname(path)    
    
    
def checkFileExistWarehouse(md5):
    "检查本地文件仓库是否存在该md5文件"
    path=getFilePathFromMd5(md5)
    return os.path.exists(path)
    
    
def copyFileFromWarehouse(targetPath,md5):
    "从本地文件仓库复制文件到指定路径"
    path=getFilePathFromMd5(md5)
    if os.path.exists(path):
        shutil.copy(path,targetPath)
    
    
def copyFileToWarehouse(sourcePath,md5):
    "复制文件到文件仓库"
    path=getFilePathFromMd5(md5)
    if not os.path.exists(path):
        dirpath=os.path.dirname(path)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        shutil.copyfile(sourcePath,path)
    
    
def loginNewFile(md5):
    "记录有新文件放入文件仓库，用于可以远程传输文件"
    dbPath=getDirPath("data/wait.db")
    if not os.path.exists(dbPath):
        executeSql(dbPath,"create table if not exists files (md5 varchar(32),status int(1))",False)
    executeSql(dbPath,"insert into files(md5,status) values('%s',0)"%(md5),False)


def getLastCheckTime():
    "读入上次检查的时间"
    global lastCheckTime
    checkTimePath=getDirPath("data/checkTime.dat")
    if os.path.exists(checkTimePath):
        fp=open(checkTimePath,"r")
        try:
            lastCheckTime=float(fp.read())
        except:
            pass
        fp.close()
        

def saveCheckTime():
    "保存检测时间"
    global nowCheckTime
    checkTimePath=getDirPath("data/checkTime.dat")
    if not os.path.exists(os.path.dirname(checkTimePath)):
        os.path.makedirs(os.path.dirname(checkTimePath))
    fp=open(checkTimePath,"w")
    fp.write(str(nowCheckTime))
    fp.close()
    
        
def main():
    "主函数"
    global backupPrefix
    #读入上次检查的时间
    getLastCheckTime()
    
    for obj in base.backup_paths:
        backupPrefix=obj['name']
        getBackupVersion()
        buildMd5Lists(obj['path'])
    
    #保存检测时间
    saveCheckTime()
    
    
if __name__ == "__main__":
    main()
