##########################################################
# 停止数据中心后台服务程序的脚本
##########################################################

killall -9 procctl
killall gzipfiles crtsurfdata deletefiles ftpgetfiles ftpputfiles tcpputfiles tcpgetfiles fileserver obtcodetodb obtmindtodb
killall execsql dminingmysql xmltodb syncupdate syncincrement deletetable migratetable xmltodb_oracle deletetable_oracle 
killall migratetable_oracle syncupdate_oracle syncincrement_oracle

sleep 3

killall -9 gzipfiles crtsurfdata deletefiles ftpgetfiles ftpputfiles tcpputfiles tcpgetfiles fileserver obtcodetodb obtmindtodb
killall -9 execsql dminingmysql xmltodb syncupdate syncincrement deletetable migratetable xmltodb_oracle deletetable_oracle
killall migratetable_oracle syncupdate_oracle syncincrement_oracle