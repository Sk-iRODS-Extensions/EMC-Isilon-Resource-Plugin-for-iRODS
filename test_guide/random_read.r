readAtPosRule {
# Input parameters are:
#   Input path
#   Output path
#   Offset from specified location 
#   Optional location for offset: SEEK_SET, SEEK_CUR, and SEEK_END 
# Output Parameter is:
#   Status of operation
#
# Behavior: read file *Input from *Offset1 and write the data to file *Output1, then
#           read file *Input2 from *Offset2 and write the data to file *Output2
   msiDataObjOpen(*OFlagsInput,*INPUT_FD);
   msiDataObjCreate(*Output1,*OFlagsOutput,*OUTPUT_FD);
   msiDataObjLseek(*INPUT_FD,*Offset1,*Loc1,*Status1);
   msiDataObjRead(*INPUT_FD,*Len1,*R_BUF);
   msiDataObjWrite(*OUTPUT_FD,*R_BUF,*W_LEN);
   msiDataObjClose(*OUTPUT_FD,*Status2);
   msiDataObjCreate(*Output2,*OFlagsOutput,*OUTPUT_FD);
   msiDataObjLseek(*INPUT_FD,*Offset2,*Loc2,*Status3);
   msiDataObjRead(*INPUT_FD,*Len2,*R_BUF);
   msiDataObjWrite(*OUTPUT_FD,*R_BUF,*W_LEN);
   msiDataObjClose(*OUTPUT_FD,*Status4);
   msiDataObjClose(*INPUT_FD,*Status5);
   writeLine("stdout","Open file *Input, create file *Output1, copy *Len1 bytes starting at location *Offset1, create file *Output2, copy *Len2 bytes starting at location *Offset2");
} 
INPUT *Input="/tempZone/home/rods/test/foo1", *OFlagsInput="objPath=/tempZone/home/rods/test/foo1++++rescName=demoResc++++replNum=0++++openFlags=O_RDONLY", *Output1="/tempZone/home/rods/test/foo2", *Output2="/tempZone/home/rods/test/foo3", *OFlagsOutput="destRescName=demoResc++++forceFlag=", *Offset1="10", *Loc1="SEEK_SET", *Len1="100", *Offset2="20", *Loc2="SEEK_SET", *Len2="100"
OUTPUT ruleExecOut
