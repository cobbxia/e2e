import sys
g_module=None
def test():
    global g_module
    modulePath="/home/admin/rpyc-3.3.0/server.py"
    if g_module is None:
        g_module=__import__('Test')
    else:
        g_module=reload(g_module)
    Test=getattr(g_module,"Test")
    t=Test()
    print(t)

while True: 
    i=raw_input("input value to continue...")
    test()

#aMod = sys.modules[modulePath]
#print(aMod)
