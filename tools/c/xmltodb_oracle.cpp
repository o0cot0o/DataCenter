/*
 *   程序名：xmltodb.cpp 本程序是数据中心的公共功能模块，用于把xml文件入库到Oracle的表中。
 *   作者：wzm
*/

#include "_tools_oracle.h"

// 程序运行的参数的结构体
struct st_arg
{
    char    connstr[101];       // 数据库的连接参数。
    char    charset[51];        // 数据库的字符集。
    char    inifilename[301];   // 数据入库的参数配置文件。
    char    xmlpath[301];       // 待入库xml文件存放的目录。
    char    xmlpathbak[301];    // xml文件入库后的备份目录。
    char    xmlpatherr[301];    // 入库失败的xml文件存放的目录。
    int     timetvl;            // 本程序运行的时间间隔，本程序常驻内存。
    int     timeout;            // 进程心跳的超时时间
    char    pname[51];          // 进程名，建议用"ftpgetfile_后缀"的方式。
} starg;

CLogFile logfile;

connection conn;

// 程序退出和信号2、15的处理函数
void EXIT(int sig);

void _help();

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer);

CPActive PActive;   // 进程心跳

// 业务处理主函数。
int totalcount, inscount, uptcount; // xml文件的总记录数、插入记录数和更新记录数。
bool _xmltodb();

struct st_xmltotable
{
    char    filename[101];      // xml文件的匹配规则，用逗号分隔。
    char    tname[31];          // 待入库的表名。
    int     uptbz;              // 更新标志:1-更新；2-不更新。
    char    execsql[301];       // 处理xml文件之前，执行的SQL语句。
} stxmltotable;

vector<struct st_xmltotable> vxmltotable;      // 数据入库的参数的容器。

// 把数据入库的参数配置文件starg.inifilename加载到vxmltotable容器中。
bool loadxmltotable();
// 从vxmltotable容器中查找xmlfilename的入库参数，存放在stxmltotable结构体中。
bool findxmltotalbe(char *xmlfilename);

// 调用处理文件子函数，返回值：0-成功，其他的都是失败，失败的情况有很多种，暂时不确定。
int _xmltodb(char *fullfilename, char *filename);

// 把xml文件移动到备份目录或错误目录。
bool xmltobakerr(char *fullfilename, char *srcpath, char *dstpath);

 CTABCOLS TABCOLS;  // 获取表的全部字段和主键字段。

char strinsertsql[10241];   // 插入表的sql语句
char strupdatesql[10241];   // 更新表的sql语句

// 拼接生成插入和更新表数据的SQL。
void crtsql();

// prepare插入和更新的sql语句，绑定输入变量。
#define MAXCOLCOUNT 500     // 每个表字段的最大数。
char *strcolvalue[MAXCOLCOUNT];     // 存放从xml每一行中解析出来的值。
sqlstatement stmtins, stmtupt;      // 插入和更新表的sqlstatement对象。
void preparesql();

// 在处理xml文件之前，如果stxmltotable.execsql不为空，就执行它。
bool execsql();

// 解析xml,存放在已绑定的输入变量strcolvalue数组中。
void splitbuffer(char *strBuffer);

int main(int argc, char* argv[])
{
    if(argc != 3) { _help(); return -1; }

    // 关闭全部的信号和输入输出
    CloseIOAndSignal(); 
    // 处理程序的退出信号
    signal(SIGINT, EXIT); signal(SIGTERM, EXIT);

    // 打开日志文件
    if(logfile.Open(argv[1], "a+") == false)
    {
        printf("打开日志文件失败(%s)。\n", argv[1]); return -1;
    }

    // 把xml解析到参数starg结构中
    if(_xmltoarg(argv[2]) == false) return -1;

    PActive.AddPInfo(starg.timeout, starg.pname);       // 设置进程心跳参数。

    // 业务处理主函数。
    _xmltodb();

    return 0;
}

void EXIT(int sig)
{
    printf("程序退出, sig=%d\n\n", sig);
    
    exit(0);
}

void _help()
{
    printf("\n");
    printf("Using:/project/tools1/bin/xmltodb_oracle logfilename xmlbuffer\n");

    printf("Example:/project/tools1/bin/procctl 10 /project/tools1/bin/xmltodb_oracle /log/idc/xmltodb_oracle_vip2.log \"<connstr>qxidc/qxidcpwd@snorcl11g_130</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><inifilename>/project/tools/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip2</xmlpath><xmlpathbak>/idcdata/xmltodb/vip2bak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/vip2err</xmlpatherr><timetvl>5</timetvl><timeout>30</timeout><pname>xmltodb_oracle_vip2</pname>\"\n\n");
 
    printf("本程序是数据中心的公共功能模块，用于把xml文件入库到Oracle的表中。\n");
    printf("logfilename     本程序运行的日志文件。\n");
    printf("xmlbuffer       本程序运行的参数，用xml表示，具体如下：\n\n");
    printf("connstr         数据库的连接参数，格式：ip,username,password,dbname,port。\n");
    printf("charset         数据库的字符集，这个参数要与数据源数据库保持一致，否则会出现中文乱码的情况。\n");
    printf("inifilename     数据入库的参数配置文件。\n");
    printf("xmlpath         待入库xml文件存放的目录。\n");
    printf("xmlpathbak      xml文件入库后的备份目录。\n");
    printf("xmlpatherr      入库失败的xml文件存放的目录。\n");
    printf("timetvl         本程序的时间间隔，单位：秒，视业务需求而定，2-30之间。\n");
    printf("timeout         本程序的超时时间，单位：秒，视xml文件大小而定，建议设置30以上。\n");
    printf("pname           进程名，尽可能采用易懂的、与其他进程不同的名称，方便故障排查。\n\n\n");

}

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer)
{
    memset(&starg, 0, sizeof(struct st_arg));

    GetXMLBuffer(strxmlbuffer, "connstr", starg.connstr, 100);
    if(strlen(starg.connstr) == 0) { logfile.Write("connstr is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer, "charset", starg.charset, 50);
    if(strlen(starg.charset) == 0) { logfile.Write("charset is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer, "inifilename", starg.inifilename, 300);
    if(strlen(starg.inifilename) == 0) { logfile.Write("inifilename is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer, "xmlpath", starg.xmlpath, 300);
    if(strlen(starg.xmlpath) == 0) { logfile.Write("xmlpath is null.\n"); return false; }
    
    GetXMLBuffer(strxmlbuffer, "xmlpathbak", starg.xmlpathbak, 300);
    if(strlen(starg.xmlpathbak) == 0) { logfile.Write("xmlpathbak is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer, "xmlpatherr", starg.xmlpatherr, 300);
    if(strlen(starg.xmlpatherr) == 0) { logfile.Write("xmlpatherr is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer, "timetvl", &starg.timetvl);
    if (starg.timetvl < 2) starg.timetvl = 2;
    if (starg.timetvl > 30) starg.timetvl = 30;

    GetXMLBuffer(strxmlbuffer, "timeout", &starg.timeout);
    if(starg.timeout == 0) { logfile.Write("timeout is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer, "pname", starg.pname, 50);
    if(strlen(starg.pname) == 0) { logfile.Write("pname is null.\n"); return false; }

    return true;
}

// 业务处理主函数。
bool _xmltodb()
{
    int counter = 50;   // 加载入库参数的计数器，初始化为50是为了在第一次进入循环的时候就加载参数。

    CDir Dir;

    while(true)
    {
        if(counter++ > 30)
        {
            counter = 0;
            // 把数据入库的参数配置文件starg.inifilename加载到vxmltotable容器中。
            if(loadxmltotable() == false) return false;
        }

        // 打开starg.xmlpath目录,为了保证先生成的数据入库，打开目录的时候，应该按文件名排序。
        if(Dir.OpenDir(starg.xmlpath, "*.XML", 10000, false, true) == false)
        {
            logfile.Write("Dir.OpenDir(%s) failed.\n", starg.xmlpath); return false;
        }

        while(true)
        {
            // 读取目录，得到一个xml文件。
            if(Dir.ReadDir() == false) break;

            if(conn.m_state == 0)
            {   
                // 连接数据源的数据库。
                if(conn.connecttodb(starg.connstr, starg.charset) != 0)
                {
                    logfile.Write("connect database(%s) failed.\n%s\n", starg.connstr, conn.m_cda.message); return false;
                }
                logfile.Write("connect database(%s) ok.\n", starg.connstr);
            }

            logfile.Write("处理文件%s...", Dir.m_FullFileName);

            // 处理xml文件。
            // 调用处理文件子函数。
            int iret = _xmltodb(Dir.m_FullFileName, Dir.m_FileName);

            PActive.UptATime();

            // 处理xml文件成功，写日志，备份文件。
            if(iret == 0)
            {
                logfile.WriteEx("ok(%s,total=%d,insert=%d,update=%d).\n", stxmltotable.tname, totalcount, inscount, uptcount);
                // 把xml文件移动到starg.xmlpathbak参数指定的目录中,一般不会发送错误，如果真发生了，程序将退出。
                if(xmltobakerr(Dir.m_FullFileName, starg.xmlpath, starg.xmlpathbak) == false) return false;
            }
            // 1-没有配置入库参数；2-待入库的表不存在；5-表的字段数太多。
            if(iret == 1 || iret == 2 || iret == 5)   // iret==1,找不到入库参数。
            {
                if(iret == 1) logfile.WriteEx("failed,没有配置入库参数.\n");
                if(iret == 2) logfile.WriteEx("failed,待入库的表（%s）不存在.\n", stxmltotable.tname);
                if(iret == 5) logfile.WriteEx("failed,待入库的表（%s）字段数太多.\n", stxmltotable.tname);
                // 把xml文件移动到starg.xmlpathbak参数指定的目录中,一般不会发送错误，如果真发生了，程序将退出。
                if(xmltobakerr(Dir.m_FullFileName, starg.xmlpath, starg.xmlpatherr) == false) return false;
            }

            // 打开xml文件错误，这种错误一般不会发生，如果真发生了，程序将退出。
            if(iret == 3)
            {
                logfile.WriteEx("failed, 打开xml文件失败。\n"); return false;
            }

            // 数据库错误，函数返回，程序将退出。
            if(iret == 4)
            {
                logfile.WriteEx("failed, 数据库错误。\n"); return false;
            }
            
            // 在处理xml文件之前，如果stxmltotable.execsql失败，函数返回，程序将退出。
            if(iret == 6)
            {
                logfile.WriteEx("failed, 执行execsql失败。\n"); return false;
            }
        }

        // 如果刚才这次扫描到了有文件，表示不空闲，可能不断的有文件生成，就不sleep了
        if(Dir.m_vFileName.size() == 0) sleep(starg.timetvl);

        PActive.UptATime();
    }

    return true;
}

// 调用处理文件子函数，返回值：0-成功，其他的都是失败，失败的情况有很多种，暂时不确定。
int _xmltodb(char *fullfilename, char *filename)
{
    totalcount = inscount = uptcount = 0;
    
    // 从vxmltotable容器中查找filename的入库参数，存放在stxmltotable结构体中。
    if(findxmltotalbe(filename) == false) return 1;
    
    // 释放上次处理xml文件时为字段分配内存。
    for(int ii = 0; ii < TABCOLS.m_allcount; ii++)
        if(strcolvalue[ii] != 0)    { delete strcolvalue[ii]; strcolvalue[ii] = 0; }

    // 获取表全部的字段和主键信息，如果获取失败，应该是数据库连接已失效。
    // 本程序运行的过程中，如果数据库出现异常，一定会在这里发现。
    if(TABCOLS.allcols(&conn, stxmltotable.tname) == false) return 4;
    if(TABCOLS.pkcols(&conn, stxmltotable.tname) == false) return 4;
    
    // 判断表的字段数不能超过MAXCOLCOUNT
    if (TABCOLS.m_allcount > MAXCOLCOUNT)    return 5;
    // 如果TABCOLS.m_allcount为0，说明表根本不存在，返回2。
    if(TABCOLS.m_allcount == 0) return 2;   // 待入库的表不存在。

    // 为每个字段分配内存
    for(int ii = 0; ii < TABCOLS.m_allcount; ii++)
        strcolvalue[ii] = new char[TABCOLS.m_vallcols[ii].collen+1];

    // 拼接生成插入和更新表数据的SQL。
    crtsql();

    // prepare插入和更新的sql语句，绑定输入变量。
    preparesql();

    // 在处理xml文件之前，如果stxmltotable.execsql不为空，就执行它。
    if(execsql() == false) return 6;

    // 打开xml文件。
    CFile File;

    if(File.Open(fullfilename, "r") == false) { conn.rollback(); return 3; }    // 打开xml文件错误。

    char strBuffer[10241];

    while(true)
    {
        // 从Xml文件中读取一行。
        if(File.FFGETS(strBuffer, 10240, "<endl/>") == false) break;

        totalcount++;

        // 解析xml,存放在已绑定的输入变量中。
        splitbuffer(strBuffer);

        // 执行插入和更新的SQL。
        if(stmtins.execute() != 0)
        {
            if(stmtins.m_cda.rc == 1)
            {
                // 判断入库参数的更新标志。
                if(stxmltotable.uptbz == 1)
                {
                    if(stmtupt.execute() != 0)
                    {
                        // 如果update失败，记录出错的行和错误内容，函数不返回，继续处理数据，也就是，不理这一行。
                        logfile.Write("%s", strBuffer);
                        logfile.Write("stmtupt.execute() failed.\n%s\n%s\n", stmtupt.m_sql, stmtupt.m_cda.message);

                        // 数据库连接已失效，无法继续，只能返回。
                        // 3113-在操作过程中服务器关闭。3114-查询过程中丢失了与Oracle服务器的连接。
                        if(stmtupt.m_cda.rc == 3113 || stmtupt.m_cda.rc == 3114)    return 4;
                    }
                    else uptcount++;
                }
            }
            else
            {
                // 如果insert失败，记录出错的行和错误内容，函数不返回，继续处理数据，也就是，不理这一行。
                logfile.Write("%s", strBuffer);
                logfile.Write("stmtins.execute() failed.\n%s\n%s\n", stmtins.m_sql, stmtins.m_cda.message);

                // 数据库连接已失效，无法继续，只能返回。
                // 3113-在操作过程中服务器关闭。3114-查询过程中丢失了与Oracle服务器的连接。
                if(stmtins.m_cda.rc == 3113 || stmtins.m_cda.rc == 3114)    return 4;
            }
        }
        else inscount++;
    }

    conn.commit();

    return 0;
}

// 把数据入库的参数配置文件starg.inifilename加载到vxmltotable容器中。
bool loadxmltotable()
{
    vxmltotable.clear();

    CFile File;
    if(File.Open(starg.inifilename, "r") == false)
    {
        logfile.Write("File.Open(%s) failed.\n", starg.inifilename); return false;
    }

    char strBuffer[501];
    while(true)
    {
        if(File.FFGETS(strBuffer, 500, "<endl/>") == false) break;

        memset(&stxmltotable, 0, sizeof(struct st_xmltotable));

        GetXMLBuffer(strBuffer, "filename", stxmltotable.filename, 100);    // xml文件的匹配规则，用逗号分隔。
        GetXMLBuffer(strBuffer, "tname", stxmltotable.tname, 30);           // 待入库的表名。
        GetXMLBuffer(strBuffer, "uptbz", &stxmltotable.uptbz);              // 更新标志：1-更新；2-不更新。
        GetXMLBuffer(strBuffer, "execsql", stxmltotable.execsql, 300);      // 处理xml文件之前，执行的SQL语句。

        vxmltotable.push_back(stxmltotable);
    }

    logfile.Write("loadxmltotable(%s) ok.\n", starg.inifilename);

    return true;
}

// 从vxmltotable容器中查找xmlfilename的入库参数，存放在stxmltotable结构体中。
bool findxmltotalbe(char *xmlfilename)
{
    for(int ii = 0; ii < vxmltotable.size(); ii++)
    {
        if(MatchStr(xmlfilename, vxmltotable[ii].filename) == true)
        {
            memcpy(&stxmltotable, &vxmltotable[ii], sizeof(struct st_xmltotable));
            return true;
        }
    }

    return false;
}

// 把xml文件移动到备份目录或错误目录。
bool xmltobakerr(char *fullfilename, char *srcpath, char *dstpath)
{
    char dstfilename[301];  // 目标文件名

    STRCPY(dstfilename, sizeof(dstfilename), fullfilename);

    UpdateStr(dstfilename, srcpath, dstpath, false);    // 小心第四个参数，一定要填false。

    if(RENAME(fullfilename, dstfilename) == false)
    {
        logfile.Write("RENAME(%s,%s) failed.\n", fullfilename, dstfilename); return false;
    }

    return true;
}

// 拼接生成插入和更新表数据的SQL。
void crtsql()
{
    memset(strinsertsql, 0, sizeof(strinsertsql));  // 插入表的sql语句
    memset(strupdatesql, 0, sizeof(strupdatesql));  // 更新表的sql语句

    // 生成插入表的sql语句。 insert into 表名(%s) values(%s)
    char strinsertp1[3001];     // insert语句的字段列表。
    char strinsertp2[3001];     // insert语句values后的内容。

    memset(strinsertp1, 0, sizeof(strinsertp1));
    memset(strinsertp2, 0, sizeof(strinsertp2));

    int colseq = 1;     // values部分字段的序号

    for(int ii = 0; ii < TABCOLS.m_vallcols.size(); ii++)
    {
        // upttime字段不需要处理。
        if(strcmp(TABCOLS.m_vallcols[ii].colname, "upttime") == 0)
            continue;

        // 拼接strinsertp1
        strcat(strinsertp1, TABCOLS.m_vallcols[ii].colname);
        strcat(strinsertp1, ",");

        // 拼接strinsertp2
        char strtemp[101];

        // keyid字段需要特殊处理。
        if(strcmp(TABCOLS.m_vallcols[ii].colname, "keyid") == 0)
        {
            SNPRINTF(strtemp, 100, sizeof(strtemp), "SEQ_%s.nextval", stxmltotable.tname+2);
        }
        else
        {
            if(strcmp(TABCOLS.m_vallcols[ii].datatype, "date") != 0)
                SNPRINTF(strtemp, 100, sizeof(strtemp), ":%d", colseq);
            else
                SNPRINTF(strtemp, 100, sizeof(strtemp), "to_date(:%d, 'yyyymmddhh24miss')", colseq);
            
            colseq++;
        }

        strcat(strinsertp2, strtemp);  strcat(strinsertp2, ",");
    }

    strinsertp1[strlen(strinsertp1)-1] = 0;     // 把最后一个多余的逗号删除
    strinsertp2[strlen(strinsertp2)-1] = 0;     // 把最后一个多余的逗号删除

    SNPRINTF(strinsertsql, 10240, sizeof(strinsertsql), "insert into %s(%s) values(%s)", stxmltotable.tname, strinsertp1, strinsertp2);

    // logfile.Write("strinsertsql=%s=\n", strinsertsql);
    
    // 如果入库参数中指定了表数据不需要更新，就不生成update语句了，函数返回。
    if(stxmltotable.uptbz != 1) return;
    // 生成修改表的sql语句。
    // update T_ZHOBTMIND1 set t=:1,p=:2,u=:3,wd=:4,wf=:5,r=:6,vis=:7,upttime=sysdate, mint=:8,minttime=to_date(:9, 'yyyymmddhh24miss') 
    //   where obtid=:10 and ddatetime=to_date(:11, 'yyyymmddhh24miss')

    // 更新TABCOLS.m_vallcols中的pkseq字段，在拼接update语句时要用到它。
    for(int ii = 0; ii < TABCOLS.m_vpkcols.size(); ii++)
        for(int jj = 0; jj < TABCOLS.m_vallcols.size(); jj++)
            if(strcmp(TABCOLS.m_vpkcols[ii].colname, TABCOLS.m_vallcols[jj].colname) == 0)
            {
                // 更新TABCOLS.m_vallcols中的pkseq字段
                TABCOLS.m_vallcols[jj].pkseq = TABCOLS.m_vpkcols[ii].pkseq;
                break;
            }

    // 先拼接sql语句开始的部分。
    sprintf(strupdatesql, "update %s set ", stxmltotable.tname);

    // 先拼接sql语句set后面的部分。
    colseq = 1;
    for(int ii = 0; ii < TABCOLS.m_vallcols.size(); ii++)
    {
        // keyid字段不需要处理。
        if(strcmp(TABCOLS.m_vallcols[ii].colname, "keyid") == 0)    continue;

        // 如果是主键字段，也不需要拼接在set后面。
        if(TABCOLS.m_vallcols[ii].pkseq != 0)   continue;

        // upttime字段直接等于sysdate,这么做是为了考虑数据库的兼容性。
        if(strcmp(TABCOLS.m_vallcols[ii].colname, "upttime") == 0)
        {
            strcat(strupdatesql, "upttime=sysdate,");
            continue;
        }

        // 其他字段要区分date字段和非date字段。
        char strtemp[101];
        if(strcmp(TABCOLS.m_vallcols[ii].datatype, "date") != 0)
            SNPRINTF(strtemp, 100, sizeof(strtemp), "%s=:%d",TABCOLS.m_vallcols[ii].colname, colseq);
        else
            SNPRINTF(strtemp, 100, sizeof(strtemp), "%s=to_date(:%d, 'yyyymmddhh24miss')",TABCOLS.m_vallcols[ii].colname, colseq);

        strcat(strupdatesql, strtemp);   strcat(strupdatesql, ",");
        
        colseq++;
    }

    strupdatesql[strlen(strupdatesql)-1] = 0;     // 把最后一个多余的逗号删除

    // 然后拼接sql语句where后面的部分。
    strcat(strupdatesql, " where 1=1 ");    // 用1=1是为了后面拼接方便，这是常用的处理方法。

    for(int ii = 0; ii < TABCOLS.m_vallcols.size(); ii++)
    {
        if(TABCOLS.m_vallcols[ii].pkseq == 0)   continue;      // 如果不是主键字段，跳过。

        // 把主键字段拼接到update语句中，要区分date字段和非date字段。
        char strtemp[101];
        if(strcmp(TABCOLS.m_vallcols[ii].datatype, "date") != 0)
            SNPRINTF(strtemp, 100, sizeof(strtemp), " and %s=:%d",TABCOLS.m_vallcols[ii].colname, colseq);
        else
            SNPRINTF(strtemp, 100, sizeof(strtemp), " and %s=to_date(:%d, 'yyyymmddhh24miss')",TABCOLS.m_vallcols[ii].colname, colseq);

        strcat(strupdatesql, strtemp);
        
        colseq++;
    }

    // logfile.Write("strupdatesql=%s=\n", strupdatesql);
}

// prepare插入和更新的sql语句，绑定输入变量。
void preparesql()
{
    // 绑定插入sql语句的输入变量。
    stmtins.connect(&conn);
    stmtins.prepare(strinsertsql);
    int colseq = 1;     // values部分字段的序号。

    for(int ii = 0; ii < TABCOLS.m_vallcols.size(); ii++)
    {
        // upttime和keyid这两个字段不需要处理。
        if(strcmp(TABCOLS.m_vallcols[ii].colname, "upttime") == 0 || strcmp(TABCOLS.m_vallcols[ii].colname, "keyid") == 0)
            continue;
        // 注意，strcolvalue数组的使用不是连续的，是和TABCOLS.m_vallcols的下标是一一对应的。
        stmtins.bindin(colseq, strcolvalue[ii], TABCOLS.m_vallcols[ii].collen);

        colseq++;
    }

    // 绑定更新sql语句的输入变量。
    // 如果入库参数中指定了表数据不需要更新，就不处理update语句了，函数返回。
    if(stxmltotable.uptbz != 1) return;

    stmtupt.connect(&conn);
    stmtupt.prepare(strupdatesql);
    colseq = 1;     // set和where部分字段的序号。
    
    //绑定set部分的输入参数。
    for(int ii = 0; ii < TABCOLS.m_vallcols.size(); ii++)
    {
        // upttime和keyid这两个字段不需要处理。
        if(strcmp(TABCOLS.m_vallcols[ii].colname, "upttime") == 0 || strcmp(TABCOLS.m_vallcols[ii].colname, "keyid") == 0)
            continue;
        
        // 如果是主键字段，也不需要拼接在set的后面。
        if(TABCOLS.m_vallcols[ii].pkseq != 0) continue;

        // 注意，strcolvalue数组的使用不是连续的，是和TABCOLS.m_vallcols的下标是一一对应的。
        stmtupt.bindin(colseq, strcolvalue[ii], TABCOLS.m_vallcols[ii].collen);
        colseq++;
    }
    
    //绑定where部分的输入参数。
    for(int ii = 0; ii < TABCOLS.m_vallcols.size(); ii++)
    {   
        // 如果是主键字段，也不需要拼接在set的后面。
        if(TABCOLS.m_vallcols[ii].pkseq == 0) continue;

        // 注意，strcolvalue数组的使用不是连续的，是和TABCOLS.m_vallcols的下标是一一对应的。
        stmtupt.bindin(colseq, strcolvalue[ii], TABCOLS.m_vallcols[ii].collen);
    
        colseq++;
    }
}

// 在处理xml文件之前，如果stxmltotable.execsql不为空，就执行它。
bool execsql()
{
    if(strlen(stxmltotable.execsql) == 0) return true;

    sqlstatement stmt;
    stmt.connect(&conn);
    stmt.prepare(stxmltotable.execsql);
    if(stmt.execute() != 0)
    {
        logfile.Write("stmt.execute() failed.\n%s\n%s\n", stmt.m_sql, stmt.m_cda.message); return false;
    }

    return true;
}

// 解析xml,存放在已绑定的输入变量strcolvalue数组中。
void splitbuffer(char *strBuffer)
{
    for(int ii = 0; ii < TABCOLS.m_allcount; ii++)
        memset(strcolvalue[ii], 0, TABCOLS.m_vallcols[ii].collen+1);

    char strtemp[31];

    for(int ii = 0; ii < TABCOLS.m_vallcols.size(); ii++)
    {
        // 如果是日期时间字段，提取数据就可以了。
        // 也就是说，xml文件中的日期时间只要包含了yyyymmddhh24miss就行，可以是任意分隔符。、
        if(strcmp(TABCOLS.m_vallcols[ii].datatype, "date") == 0)
        {  
            GetXMLBuffer(strBuffer, TABCOLS.m_vallcols[ii].colname, strtemp, TABCOLS.m_vallcols[ii].collen);
            PickNumber(strtemp, strcolvalue[ii], false, false);
            continue;
        }

        // 如果是数值字段，只提取数字、+-符号和圆点。
        if(strcmp(TABCOLS.m_vallcols[ii].datatype, "number") == 0)
        {  
            GetXMLBuffer(strBuffer, TABCOLS.m_vallcols[ii].colname, strtemp, TABCOLS.m_vallcols[ii].collen);
            PickNumber(strtemp, strcolvalue[ii], true, true);
            continue;
        }

        // 如果是字符字段，直接提取。
        GetXMLBuffer(strBuffer, TABCOLS.m_vallcols[ii].colname, strcolvalue[ii], TABCOLS.m_vallcols[ii].collen);

    }
}   
