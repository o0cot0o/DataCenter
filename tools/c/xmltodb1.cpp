/*
 *   程序名：xmltodb.cpp 本程序是数据中心的公共功能模块，用于把xml文件入库到MySQL的表中。
 *   作者：wzm
*/

#include "/project/public/_public.h"
#include "_mysql.h"


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


int main(int argc, char* argv[])
{
    if(argc != 3) { _help(); return -1; }

    // 关闭全部的信号和输入输出
    // CloseIOAndSignal(); 

    // 处理程序的退出信号
    // signal(SIGINT, EXIT); signal(SIGTERM, EXIT);

    // 打开日志文件
    if(logfile.Open(argv[1], "a+") == false)
    {
        printf("打开日志文件失败(%s)。\n", argv[1]); return -1;
    }

    // 把xml解析到参数starg结构中
    if(_xmltoarg(argv[2]) == false) return -1;

    // 连接数据源的数据库。
    if(conn.connecttodb(starg.connstr, starg.charset) != 0)
    {
        logfile.Write("connect database(%s) failed.\n%s\n", starg.connstr, conn.m_cda.message); return -1;
    }
    logfile.Write("connect database(%s) ok.\n", starg.connstr);

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
    printf("Using:/project/tools1/bin/xmltodb logfilename xmlbuffer\n");

    printf("Example:/project/tools1/bin/procctl 10 /project/tools1/bin/xmltodb /log/idc/xmltodb_vip1.log \"<connstr>127.0.0.1,root,root,mysql,3306</connstr><charset>utf8</charset><inifilename>/project/tools/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip1</xmlpath><xmlpathbak>/idcdata/xmltodb/vip1bak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/vip1err</xmlpatherr><timetvl>5</timetvl><timeout>30</timeout><pname>dminingmysql_ZHOBTCODE</pname>\"\n\n");
    
    printf("本程序是数据中心的公共功能模块，用于把xml文件入库到MySQL的表中。\n");
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
