needverifyfp=open("./needverifyfile.txt","w")
alreadyverifyfile="./alreadyverifyfile.txt"
alreadydict={}
for line in open(alreadyverifyfile,"r"):
    tablename=line.strip("\n")
    alreadydict[tablename]=""

for line in open("./log/render.log","r"):
    if line.find("begins to verify") >= 0:
        print(line)
        tablename=line.split("table:")[1].split()[0]
        if tablename not in alreadydict:
            print(tablename)
            needverifyfp.write(tablename+" tbbi_isolation_prd5_dst_0\n")
