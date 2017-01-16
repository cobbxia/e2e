#!/usr/ali/bin/python
import sys 
import os
import simplejson as json
sys.path.append(sys.path[0] + "/../common/")
import config as Config

verify_tool = Config.verify_tool_dir + 'verify_1table.py'

def GenerateCommandList(input, outputPath, today):
    fi = file(input, "r")
    tables = fi.read().lower().split();
    fi.close();
    print "number of tables:", len(tables);
    commands = {}
    dependencies = {}
    for table_name in tables:
        task_name = table_name;
        commands[task_name] = "python " + verify_tool + " " + table_name + " " + today;
        dependencies[task_name] = []; 
    foCmd = file(outputPath + "/dragon_info.json", "w") 
    json.dump(commands, foCmd, indent = 2)
    foCmd.close();
    foDep = file(outputPath + "/dragon_dep.json", "w")
    json.dump(dependencies, foDep, indent = 2)
    foDep.close();

if __name__ == "__main__":
    if (len(sys.argv) != 4): 
        print "usage: dragon_conf_generater.py inputTableList outputPath bizDate"
        print "       command and dependencies will write to dragon_info.json.new and dragon_dep.json.new in output path";
        exit(1)
    GenerateCommandList(sys.argv[1], sys.argv[2], sys.argv[3]);
