import os,subprocess,json

def do_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return p.returncode, out, err 

def getProjectURL(tableName):
    return projectURL



def getProject(tableName):
    retProjectName=""
    tableDict={}
    projectURL=getProjectURL(tableName)
    print(projectURL)
    cmd='''curl %s 2> /dev/null''' % (projectURL)
    r=do_cmd(cmd)
    print(r)
    if r[0] == 0:
        js=json.loads(r[1])
        print(js)
        print(js['result'])
        print(len(js['result']))
        i=0
        for item in js['result']:
            print("i:"+str(i))
            i=i+1
            guid=item['guid']
            print(guid)
            if guid.split(".")[0].lower() != "odps":
                continue
            guidProjectName=guid.split(".")[1]
            guidTableName=guid.split(".")[2]
            print("retProjectName:"+retProjectName)
            print("guidProjectName:"+guidProjectName)
            if guidTableName == tableName and (retProjectName == "" or guidProjectName == "tbbi_isolation"):
                retProjectName=guidProjectName
    return retProjectName

infile=open("out.txt","r")
outfile=open("bushuju.txt","w")
for line in infile:
    line=line.strip()
    if line == "":
        continue
    tableName=line
    retProjectName=getProject(tableName)
    print("retProjectName:%s" % (retProjectName))
    outfile.write("%s\t%s\n" % (tableName,retProjectName))       
