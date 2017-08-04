import os
import datetime

def CMD_EXECUTE(cmd):
    	from subprocess import Popen, PIPE
	p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	out, err = p.communicate()
    
	print "Return code: ", p.returncode
	print err

NowDateTime = datetime.datetime.now().strftime(' %Y%m%d_%H_%M_%S').strip()
#Copy From
DATA_SQL_SRC_PATH='Dump\\Last\\'
Ab_Path_DATA_SQL_SRC_PATH=os.getcwd()+'\\'+DATA_SQL_SRC_PATH
#Copy To
BOT_DATA_PATH='\\\\192.168.20.191\\WorkingFolder\\App_Data'
#CMD_DEL_FILE=['del' ,'/s' ,'/q' ,DATA_SQL_SRC_PATH]
CMD_NET_USE=['net', 'use' ,BOT_DATA_PATH, 'JI#94cj/6ru' ,'/USER:Administrator']
CMD_NET_DEL=['net', 'use' ,BOT_DATA_PATH, '/delete']
CMD_XCOPY=['xcopy',Ab_Path_DATA_SQL_SRC_PATH, BOT_DATA_PATH, '/s','/y']
CMD_REMOVE_BOT_DATA=['RD','/S','/Q',BOT_DATA_PATH]
CMD_MD_BOT_DATA=['MD',BOT_DATA_PATH]

EXE_MYSQLDEUMP = 'C:\\AppServ\\MySQL\\bin\\mysqldump.exe'
CANDIDATE_TABLE=['tblmodelname', 'tbluser', 'tblproject']

File_Export_parent_path=os.getcwd()+'\\Dump\\'
File_Export_path =  File_Export_parent_path+ NowDateTime
File_Export_ForSync=File_Export_parent_path+'Last\\'
SourceDB='MySQLDB'
ImportDB='BotData'
CMD_EXECUTE(CMD_NET_USE)
CMD_EXECUTE(CMD_REMOVE_BOT_DATA)
CMD_EXECUTE(CMD_MD_BOT_DATA)
CMD_EXECUTE(CMD_XCOPY)
CMD_EXECUTE(CMD_NET_DEL)

def main(args=None):

    print File_Export_parent_path
    cwd = os.getcwd()
    print cwd
    #List all file from working folder
    files_list = os.listdir(cwd)
    print files_list

    
    for tbl in CANDIDATE_TABLE:
        ExportTableFromMySQL_Ex(tbl)

	import shutil
	if  os.path.exists(File_Export_ForSync):	
		shutil.rmtree(File_Export_ForSync)

	if  os.path.exists(File_Export_path):		
		shutil.copytree(File_Export_path,File_Export_ForSync)

CMD_EXECUTE(CMD_NET_USE)
CMD_EXECUTE(CMD_REMOVE_BOT_DATA)
CMD_EXECUTE(CMD_MD_BOT_DATA)
CMD_EXECUTE(CMD_XCOPY)
CMD_EXECUTE(CMD_NET_DEL)

def ExportTableFromMySQL(table):		
	if not os.path.exists(File_Export_path):
		os.makedirs(File_Export_path)

	File_Name = File_Export_path + '\\' + table + '_' + NowDateTime + '.sql'
	cmd = [EXE_MYSQLDEUMP, '-hlocalhost', '-uroot', '-p123456', '--compatible=mssql', '--no-create-info','--skip-quote-names', '--skip-add-locks',  'GAIA', table,'--result-file='+File_Name]

	from subprocess import Popen, PIPE
	p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	out, err = p.communicate()
    
	print "Return code: ", p.returncode

def ExportTableFromMySQL_Ex(table):



    if not os.path.exists(File_Export_path):
        os.makedirs(File_Export_path)

    File_Name = File_Export_path + '\\' + table + '_' + NowDateTime
    cmd = [EXE_MYSQLDEUMP, '-hlocalhost', '-uroot', '-p123456', '--compatible=mssql', '--no-create-info','--skip-quote-names',
           '--extended-insert=FALSE', '--skip-add-locks', '--extended-insert=FALSE', SourceDB, table]

    from subprocess import Popen, PIPE
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    #err = p.communicate()
    print "Return code: ", p.returncode
	#print "Return err: ", err
    data_record_lst = list() 

    while True:
        _line = p.stdout.readline()
        if _line != '':
        #the real code does filtering here
            data_record_lst.append(_line)
        else:
            break
		

    print len(data_record_lst)
    data_record_chunks = list(data_record_lst[i:i+999] for i in range(0, len(data_record_lst), 999))
    
    count=0
    for chk in data_record_chunks:
        _fh_new = open(File_Name+'['+str(count)+'].sql', 'w')
        _fh_new.writelines('USE ['+ImportDB+'] \n')
        _fh_new.flush()
        for line in chk:
            _fh_new.writelines(line)
            _fh_new.flush()
        _fh_new.close()
        count=count+1






def ExportTableFromMySQLAndRMDoubleQuotes(table):



    if not os.path.exists(File_Export_path):
        os.makedirs(File_Export_path)

    File_Name = File_Export_path + '\\' + table + '_' + NowDateTime + '.sql'
    cmd = [EXE_MYSQLDEUMP, '-hlocalhost', '-uroot', '-p123456', '--compatible=mssql', '--no-create-info',
           '--extended-insert=FALSE', '--skip-add-locks', '--extended-insert=FALSE', SourceDB, table]

    from subprocess import Popen, PIPE
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    
    print "Return code: ", p.returncode
    #print out.rstrip(), err.rstrip()
    _fh_new = open(File_Name, 'w')
    for line in out:

        # replace double quotes
        line = line.replace('"', '')

        _fh_new.writelines(line)
        _fh_new.flush()
        # print line

    _fh_new.close()    

def removeDoubleQuotesFromFile(source, destination):
    _fh_new = open(destination, 'w')
    _fh = open(source)
    for line in _fh:
        # locate table name by "VALUES"
        idx = line.find('VALUES')
        # replace double quotes
        line = line[:idx].replace('"', '') + line[idx:]

        _fh_new.writelines(line)
        _fh_new.flush()
        # print line

    _fh.close()
    _fh_new.close()


if __name__ == "__main__":
    main()
