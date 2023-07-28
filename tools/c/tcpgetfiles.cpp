/*
 * 程序名：tcpgetfiles.cpp，采用tcp协议，实现文件发送的客户端
 * 作者：wzm。
 */
#include "_public.h"

// 程序运行的参数结构体
struct st_arg
{
    int     clienttype;             // 客户端类型，1-上传文件；2-下载文件。
    char    ip[31];                 // 服务端的IP地址。
    int     port;                   // 服务端的端口。
    int     ptype;                  // 文件下载成功后服务端文件的处理方式：1-删除文件；2-移动到备份目录。
    char    srvpath[301];           // 服务端文件存放的根目录。
    char    srvpathbak[301];        // 文件成功下载后，服务端文件备份的根目录，当ptype=2时有效。
    bool    andchild;               // 是否下载srvpath目录下各级子目录的文件, true-是；false-否。
    char    matchname[301];         // 待下载文件名的匹配方式，如"*.TXT,*.XML"，注意用大写。
    char    clientpath[301];        // 客户端文件存放的根目录。
    int     timetvl;                // 扫描服务端目录文件的时间间隔，单位：秒。
    int     timeout;                // 进程心跳的超时时间。
    char    pname[51];              // 进程名，建议用"tcpgetfiles_后缀"的方式。
} starg;

CLogFile logfile;

// 程序退出和信号2、15的处理函数。
void EXIT(int sig);

void _help();

// 把xml解析到参数starg结构体中。
bool _xmltoarg(char *strxmlbuffer);

CTcpClient TcpClient;

bool Login(const char *argv);  // 登录业务。

char strrecvbuffer[1024]; // 发送报文的buffer
char strsendbuffer[1024]; // 接收报文的buffer

// 文件下载的主函数
void _tcpgetfiles();

// 接收文件的内容
bool RecvFile(const int socket, const char *filename, const char *mtime, int filesize);

CPActive PActive; // 进程心跳

int main(int argc, char *argv[])
{
    if (argc != 3) { _help(); return -1; }

    // 处理程序的退出信号
    // 设置信号，在shell状态下可以 "kill + 进程号" 正常终止该程序
    // 但请不要用 "kill -9 + 进程号" 强行终止
    CloseIOAndSignal(); signal(SIGINT, EXIT); signal(SIGTERM, EXIT);

    // 打开日志文件
    if (logfile.Open(argv[1], "a+") == false)
    {
        printf("打开日志文件失败(%s)。\n", argv[1]); return -1;
    }

    // 解析xml,得到程序运行的参数
    if (_xmltoarg(argv[2]) == false) return -1;

    PActive.AddPInfo(starg.timeout, starg.pname); // 把进程的心跳信息写入共享内存

    // 向服务端发起连接请求。
    if (TcpClient.ConnectToServer(starg.ip, starg.port) == false)
    {
        printf("TcpClient.ConnectToServer(%s,%d) failed.\n", starg.ip, starg.port); EXIT(-1);
    }

    // 登录业务。
    if (Login(argv[2]) == false)
    {
        printf("Login(%s) failed.\n", argv[2]); EXIT(-1);
    }

    // 调用文件下载的主函数
    _tcpgetfiles();

    EXIT(0);
}


// 登录业务。
bool Login(const char *argv)
{
    memset(strsendbuffer, 0, sizeof(strsendbuffer));
    memset(strrecvbuffer, 0, sizeof(strrecvbuffer));

    SPRINTF(strsendbuffer, sizeof(strsendbuffer), "%s<clienttype>2</clienttype>", argv);
    logfile.Write("发送:%s\n", strsendbuffer);
    if (TcpClient.Write(strsendbuffer) == false) return false; // 向服务端发送请求报文。

    if (TcpClient.Read(strrecvbuffer, 20) == false) return false; // 接收服务端的回应报文。
    logfile.Write("接收:%s\n", strrecvbuffer);

    logfile.Write("登录(%s:%d)成功。\n", starg.ip, starg.port);

    return true;
}

void EXIT(int sig)
{
    logfile.Write("程序退出, sig=%d\n\n", sig);
    exit(0);
}

void _help()
{
    printf("\n");
    printf("Using:/project/tools1/bin/tcpgetfiles logfilename xmlbuffer\n");

    printf("Example:/project/tools1/bin/procctl 20 /project/tools1/bin/tcpgetfiles /log/idc/tcpgetfiles_surfdata.log \"<ip>124.222.210.41</ip><port>5005</port><ptype>1</ptype><srvpath>/tmp/tcp/surfdata2</srvpath><srvpathbak>/tmp/tcp/surfdata2bak</srvpathbak><andchild>true</andchild><matchname>*.XML,*.CSV,*.JSON</matchname><clientpath>/tmp/tcp/surfdata3</clientpath><timetvl>10</timetvl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>\"\n");
    printf("        /project/tools1/bin/procctl 20 /project/tools1/bin/tcpgetfiles /log/idc/tcpgetfiles_surfdata.log \"<ip>124.222.210.41</ip><port>5005</port><ptype>2</ptype><srvpath>/tmp/tcp/surfdata2</srvpath><srvpathbak>/tmp/tcp/surfdata2bak</srvpathbak><andchild>true</andchild><matchname>*.XML,*.CSV,*.JSON</matchname><clientpath>/tmp/tcp/surfdata3</clientpath><timetvl>10</timetvl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>\"\n\n\n");
    
    printf("本程序是数据中心的公共功能模块, 采用tcp协议把文件发送给服务端。\n");
    printf("logfilename     是本程序运行的日志文件。\n");
    printf("xmlbuffer       本程序运行的参数，如下：\n");
    printf("ip              服务端的IP地址。\n");
    printf("port            服务端的端口。\n");
    printf("ptype           文件下载成功后的处理方式: 1-删除文件; 2-移动到备份目录。\n");
    printf("srvpath         服务端文件存放的根目录。\n");
    printf("srvpathbak      文件成功下载后, 服务端文件备份的根目录, 当ptype=2时有效。\n");
    printf("andchild        是否下载srvpath目录下各级子目录的文件, true-是;false-否, 缺省为false。\n");
    printf("matchname       待下载文件匹配的规则，如\"*.TXT,*.XML\"。\n");
    printf("clientpath      客户端文件存放的根目录。\n");
    printf("timetvl         扫描本地目录文件的时间间隔, 单位: 秒, 取值在1-30之间。\n");
    printf("timeout         本程序的超时时间, 单位: 秒, 视文件大小和网络带宽而定, 建议设置50以上。\n");
    printf("pname           进程名，尽可能采用易懂的、与其他进程不同的名称，方便故障排查。\n\n\n");

}

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer)
{
    memset(&starg, 0, sizeof(struct st_arg));

    GetXMLBuffer(strxmlbuffer, "ip", starg.ip);         // 服务端的IP地址
    if(strlen(starg.ip) == 0) { logfile.Write("ip is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "port", &starg.port);    // 服务端的端口
    if(starg.port == 0)   { logfile.Write("port is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "ptype", &starg.ptype);   // 文件下载成功后的处理方式：1-删除文件；2-移动到备份目录。
    if(starg.ptype != 1 && starg.ptype != 2) { logfile.Write("ptype not in (1,2).\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "srvpath", starg.srvpath);      // 服务端文件存放的根目录。
    if(strlen(starg.srvpath) == 0) { logfile.Write("srvpath is error.\n"); return false; }
    
    GetXMLBuffer(strxmlbuffer, "srvpathbak", starg.srvpathbak, 300);   // 文件成功下载后，客户端文件备份的根目录，当ptype=2时有效。
    if((starg.ptype==2)&&(strlen(starg.srvpathbak)==0))
    { logfile.Write("srvpathbak is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "andchild", &starg.andchild);   // 是否下载clientpath目录下各级子目录的文件，true-是;false-否，缺省为false。

    GetXMLBuffer(strxmlbuffer, "matchname", starg.matchname, 100);    // 待下载文件匹配的规则。
    if(strlen(starg.matchname) == 0) { logfile.Write("matchname is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "clientpath", starg.clientpath);   // 客户端文件存放的根目录。
    if(strlen(starg.clientpath) == 0)
    { logfile.Write("clientpath is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "timetvl", &starg.timetvl);      // 扫描本地目录文件的时间间隔，单位：秒，取值在1-30之间。
    if(starg.timetvl == 0) { logfile.Write("timetvl is null.\n"); return false;}
    
    // timeval没有必要超过30秒
    if(starg.timetvl > 30) starg.timetvl = 30;

    GetXMLBuffer(strxmlbuffer, "timeout", &starg.timeout);      // 进程心跳的超时时间，没有必要小于50秒
    if(starg.timeout == 0) { logfile.Write("timeout is null.\n"); return false;}
    if(starg.timeout < 50) starg.timeout = 50;

    GetXMLBuffer(strxmlbuffer, "pname", starg.pname, 50);       // 进程名
    if(strlen(starg.pname) == 0)
    { logfile.Write("pname is null.\n"); return false;}

    return true;
}

// 文件下载的主函数
void _tcpgetfiles()
{
    PActive.AddPInfo(starg.timeout, starg.pname);

    while(true)
    {
        memset(strsendbuffer, 0, sizeof(strsendbuffer));
        memset(strrecvbuffer, 0, sizeof(strrecvbuffer));

        PActive.UptATime();

        // 接受服务端的报文
        if(TcpClient.Read(strrecvbuffer, starg.timeout+10) == false)
        {
            logfile.Write("TcpClient.Read() failed.\n"); return;
        }
        // logfile.Write("strrecvbuffer=%s\n", strrecvbuffer);

        // 处理心跳报文
        if(strcmp(strrecvbuffer, "<activetest>ok</activetest>") == 0)
        {
            strcpy(strsendbuffer, "ok");
            // logfile.Write("strsendbuffer=%s\n", strsendbuffer);
            if(TcpClient.Write(strsendbuffer) == false)
            {
                logfile.Write("TcpClient.Write() failed.\n"); return;
            }
        }

        // 处理下载文件的请求报文
        if(strncmp(strrecvbuffer, "<filename>", 10) == 0)
        {
            // 解析下载文件请求报文的xml
            char srvfilename[301];   memset(srvfilename, 0, sizeof(srvfilename));
            char mtime[21];             memset(mtime, 0, sizeof(mtime));
            int filesize = 0;

            GetXMLBuffer(strrecvbuffer, "filename", srvfilename, 300);
            GetXMLBuffer(strrecvbuffer, "mtime", mtime, 19);
            GetXMLBuffer(strrecvbuffer, "size", &filesize);
            
            // 客户端和服务端文件的目录是不一样的，以下代码生成客户端的文件名
            // 把文件名中的srvpath替换成clientpath,要小心第三个参数
            char clientilename[301]; memset(clientilename, 0, sizeof(clientilename));
            strcpy(clientilename, srvfilename);
            UpdateStr(clientilename, starg.srvpath, starg.clientpath, false);

            // 接收文件的内容
            logfile.Write("recv %s(%d) ...", clientilename, filesize);
            if(RecvFile(TcpClient.m_connfd, clientilename, mtime, filesize) == true)
            {
                logfile.Write("ok.\n");
                SNPRINTF(strsendbuffer, sizeof(strsendbuffer), 1000, "<filename>%s</filename><result>ok</result>", srvfilename);
            }
            else
            {
                logfile.Write("failed.\n");
                SNPRINTF(strsendbuffer, sizeof(strsendbuffer), 1000, "<filename>%s</filename><result>failed</result>", srvfilename);
            }
            
            // 把接收结果返回给对端
            // logfile.Write("strsendbuffer=%s\n", strsendbuffer);
            if(TcpClient.Write(strsendbuffer) == false)
            {
                logfile.Write("TcpClient.Write() failed.\n"); return;
            }
        }
    }
}

// 接收文件的内容
bool RecvFile(const int socket, const char *filename, const char *mtime, int filesize)
{
    // 生成临时文件名
    char strfilenametmp[301]; memset(strfilenametmp, 0, sizeof(strfilenametmp));
    SNPRINTF(strfilenametmp, sizeof(strfilenametmp), 300, "%s.tmp", filename);

    int totalbytes = 0;     // 已接收文件的总字节数
    int onread= 0;          // 本次打算接收的字节数
    char buffer[1000];      // 接收文件内容的缓存区
    FILE *fp = NULL;

    // 创建临时文件
    if((fp = FOPEN(strfilenametmp, "wb")) == NULL) return false;

    while(true)
    {
        memset(buffer, 0, sizeof(buffer));

        // 计算本次应该接收的字节数。
        if(filesize - totalbytes > 1000) onread = 1000;
        else onread = filesize - totalbytes;

        // 接收文件内容
        if(Readn(socket, buffer, onread) == false) { fclose(fp); return false; } 

        // 把接收到的内容写入文件
        fwrite(buffer, 1, onread, fp);

        // 计算已接收文件的总字节数，如果文件接收完，跳出循环。
        totalbytes = totalbytes + onread;
        if(totalbytes == filesize) break;
    }

    // 关闭临时文件
    fclose(fp);

    // 重置文件的时间
    UTime(strfilenametmp, mtime);

    // 把临时文件RENAME为正式的文件
    if(RENAME(strfilenametmp, filename) == false) return false;

    return true;
}