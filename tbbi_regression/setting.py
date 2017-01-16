import MetaTransfer 
import DesenTransfer 
import DataTransfer 
preproDict={
"render":DataTransfer.insert,
"test":DataTransfer.insert,
}
postproDict={
"render":DataTransfer.modend,
"test":DataTransfer.modend,
}
proDict={
}
processDict={
"updateudf": MetaTransfer.updateUDF,
"transfer": DesenTransfer.TransferPart,
"tag": DesenTransfer.TagPart,
"desen": DesenTransfer.desen,
"drag":DataTransfer.drag,
"verify":DataTransfer.verifyResult,
"delete":DataTransfer.delete,
"size":DataTransfer.tagsize,
"datacopy":DataTransfer.datacopy,
"resource":MetaTransfer.CreateResource,
"table":MetaTransfer.CreateTable,
"odpstable":MetaTransfer.CreateOdpsTable,
"column":MetaTransfer.CreateColumn,
"part":MetaTransfer.CreatePart,
"result":DataTransfer.CreateResultTable,
"copy":DataTransfer.CopyToResult,
"render":DataTransfer.render,
"sleep":DataTransfer.tsleep,
"test":DataTransfer.test,
}

processFlagDict={
"datacopy":"1",
"drag":"1",
"verify":"3",
"delete":"5",
"size":"0",
"tag":"100",
"desen": "0",
"transfer":"3",
"result":"0"
}
