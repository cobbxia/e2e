def uniqcost():
    costdict={}
    nodelist=[]
    mysql -uroot -pmiddleware_host --database bi  -h10.101.86.14 -N -e "select const,nodeid from e2e.regression_costinfo;"
    for line in open("./cost.txt","r"):
        line=line.strip("\n")
        cost=line.split()[0]
        nodeid=line.split()[1]
        if nodeid not in costdict:
            costdict[nodeid]=cost
        elif cost > costdict[nodeid]:
            costdict[nodeid]=cost
    
    for nodeid in costdict:
        print("%s\t%s" % (nodeid,costdict[nodeid]))
        
