#include "/project/public/_public.h"
#include "/project/public/_ftp.h"

struct st_arg
{
    char    host[31];           // 远程服务器ftp的IP和端口
    int     mode;               // 传输模式: 1-被动模式，2-主动模式，缺省采用被动模式
    char    username[31];       // 远程服务器ftp的用户名。
    char    password[31];       // 远程服务器ftp的密码。
    char    localpath[301];     // 本地文件存放的目录。
    char    remotepath[301];    // 远程服务器存放文件的目录。
    char    matchname[101];     // 待上传文件匹配的规则。
    int     ptype;              // 上传后客户端文件的处理方式：1-什么也不做；2-删除；3、备份。
    char    localpathbak[301];  // 上传后客户端文件的备份目录。
    char    okfilename[301];    // 已上传成功文件名清单。
    int     timeout;            // 进程心跳的超时时间
    char    pname[51];          // 进程名，建议用"ftpgetfile_后缀"的方式。
} starg;

struct st_fileinfo
{
    char filename[301];     // 文件名
    char mtime[21];         // 文件时间
};

vector<struct st_fileinfo> vlistfile1;       // 已上传成功文件名的容器，从okfilename中加载
vector<struct st_fileinfo> vlistfile2;       // 上传前列出服务器文件名的容器，从nlist文件中加载
vector<struct st_fileinfo> vlistfile3;       // 本次不需要上传的文件的容器
vector<struct st_fileinfo> vlistfile4;       // 本次需要上传的文件的容器

// 加载okfilename文件中的内容到容器vlistfile1中
bool LoadOKFile();

// 比较vlistfile2和vlistfile1, 得到vlistfile3和vlistfile4。
bool CompVector();

// 把容器vlistfile3中的内容写入okfilename文件，覆盖之前的旧okfilename文件
bool WriteToOkFile();

// 如果ptype==1, 把上传成功的文件记录追加到okfilename文件中
bool AppendToOkFile(struct st_fileinfo *st_fileinfo);

// 把ftp.nlist()方法获取到的list文件加载到容器vlistfile2中
bool LoadListFile();

CLogFile logfile;

Cftp ftp;

// 程序退出和信号2、15的处理函数
void EXIT(int sig);

void _help();

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer);

// 上传文件功能的主函数
bool _ftpgetfiles();

CPActive PActive;   // 进程心跳

int main(int argc, char* argv[])
{
    // 把服务器上某目录的文件全部上传到本地目录(可以指定文件名的匹配规则)
    if(argc != 3) { _help(); return -1; }

    // 处理程序的退出信号
    // 设置信号，在shell状态下可以 "kill + 进程号" 正常终止该程序
	// 但请不要用 "kill -9 + 进程号" 强行终止
    CloseIOAndSignal(); 
    signal(SIGINT, EXIT); signal(SIGTERM, EXIT);

    // 打开日志文件
    if(logfile.Open(argv[1], "a+") == false)
    {
        printf("打开日志文件失败(%s)。\n", argv[1]); return -1;
    }

    // 解析xml,得到程序运行的参数
    if(_xmltoarg(argv[2]) == false) return -1;

    PActive.AddPInfo(starg.timeout, starg.pname);   // 把进程的心跳信息写入共享内存

    // 登录ftp服务器
    if(ftp.login(starg.host, starg.username, starg.password, starg.mode) == false)
    {
        logfile.Write("ftp.login(%s,%s,%s) failed.\n", starg.host, starg.username, starg.password); return -1;
    }

    // logfile.Write("ftp.login ok.\n");

    _ftpgetfiles();

    ftp.logout();

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
    printf("Using:/project/tools1/bin/ftpputfiles logfilename xmlbuffer\n");

    printf("Example:/project/tools1/bin/procctl 30 /project/tools1/bin/ftpputfiles /log/idc/ftpputfiles_surfdata.log \"<host>127.0.0.1:21</host><mode>1</mode><username>real</username><password>r23456</password><localpath>/tmp/idc/surfdata</localpath><remotepath>/tmp/ftpputest</remotepath><matchname>SURF_ZH*.JSON</matchname><ptype>1</ptype><localpathbak>/tmp/idc/surfdatabak</localpathbak><okfilename>/idcdata/ftplist/ftpputfiles_surfdata.xml</okfilename><timeout>80</timeout><pname>ftpputfiles_surfdata</pname>\"\n\n\n");
   
    printf("本程序是通用的功能模块，用于把本地目录中的文件上传到远程ftp服务器。\n");
    printf("logfilename是本程序运行的日志文件。\n");
    printf("xmlbuffer为文件上传的参数，如下：\n");
    printf("<host>127.0.0.1:21</host> 远程服务器ftp的IP和端口。\n");
    printf("<mode>1</mode> 1-被动模式，2-主动模式，缺省采用被动模式。\n");
    printf("<username>real</username> 远程服务器ftp的用户名。\n");
    printf("<password>r23456</password> 远程服务器ftp的密码。\n");
    printf("<localpath>/tmp/idc/surfdata</localpath> 本地文件存放的目录。\n");
    printf("<remotepath>/tmp/ftpputtest</remotepath> 远程服务器存放文件的目录。\n");
    printf("<matchname>SURF_ZH*.JSON</matchname> 待上传文件匹配的规则。"\
            "不匹配的文件不会被上传，本字段尽可能设置精确，不建议用*匹配全部的文件。\n");

    printf("<ptype>1</ptype> 文件上传成功后，远程服务器文件的处理方式：1-什么也不做；2-删除；3、备份，如果为3，还要指定备份目录。\n");
    printf("<localpathbak>/tmp/idc/surfdatabak</localpathbak> 文件上传成功后，服务器文件的备份目录，此参数只有当ptype=3时才有效。\n");
    printf("<okfilename>/idcdata/ftplist/ftpputfiles_surfdata.xml</okfilename> 已上传成功文件名清单，此参数只有ptype=1时才有效。\n");

    printf("<timeout>80</timeout> 上传文件超时时间，单位：秒，视文件大小和网络带宽而定。\n");
    printf("<pname>ftpputfiles_surfdata</pname> 进程名，尽可能采用易懂的、与其他进程不同的名称，方便故障排查。\n\n\n");

}

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer)
{
    memset(&starg, 0, sizeof(struct st_arg));

    GetXMLBuffer(strxmlbuffer, "host", starg.host, 30);   // 远程服务器ftp的IP和端口
    if(strlen(starg.host) == 0)
    { logfile.Write("host is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "mode", &starg.mode);     // 传输模式: 1-被动模式，2-主动模式，缺省采用被动模式
    if(starg.mode != 2)   starg.mode = 1;

    GetXMLBuffer(strxmlbuffer, "username", starg.username, 30);   // 远程服务器ftp的用户名。
    if(strlen(starg.username) == 0)
    { logfile.Write("username is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "password", starg.password, 30);   // 远程服务器ftp的密码。
    if(strlen(starg.password) == 0)
    { logfile.Write("password is null.\n"); return false;}
    
    GetXMLBuffer(strxmlbuffer, "localpath", starg.localpath, 300);   // 本地文件存放的目录。
    if(strlen(starg.localpath) == 0)
    { logfile.Write("localpath is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "remotepath", starg.remotepath, 300);   // 远程服务器存放文件的目录。
    if(strlen(starg.remotepath) == 0)
    { logfile.Write("remotepath is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "matchname", starg.matchname, 100);    // 待上传文件匹配的规则。
    if(strlen(starg.matchname) == 0)
    { logfile.Write("matchname is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "ptype", &starg.ptype);      // 上传后客户端文件的处理方式：1-什么也不做；2-删除；3、备份。
    if(starg.mode != 1 && starg.mode != 2 && starg.mode != 3)
    { logfile.Write("ptype is error.\n"); return false; }

    GetXMLBuffer(strxmlbuffer, "localpathbak", starg.localpathbak, 300);   // 上传后客户端文件的备份目录。
    if(starg.ptype == 3 && strlen(starg.localpathbak) == 0)
    { logfile.Write("localpathbak is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "okfilename", starg.okfilename, 300);   // 已上传成功文件名清单
    if(starg.ptype == 1 && strlen(starg.okfilename) == 0)
    { logfile.Write("okfilename is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "timeout", &starg.timeout);      // 进程心跳的超时时间
    if(starg.timeout == 0)
    { logfile.Write("timeout is null.\n"); return false;}

    GetXMLBuffer(strxmlbuffer, "pname", starg.pname, 50);       // 进程名
    if(strlen(starg.pname) == 0)
    { logfile.Write("pname is null.\n"); return false;}

    return true;
}

// 上传文件功能的主函数
bool _ftpgetfiles()
{   
    // 把localpath目录下的文件加载到容器vlistfile2中
    if(LoadListFile() == false)
    {
        logfile.Write("LoadListFile() failed.\n"); return false;
    }

    PActive.UptATime(); // 更新进程的心跳

    if(starg.ptype == 1)
    {
        // 加载okfilename文件中的内容到容器vlistfile1中
        LoadOKFile();

        // 比较vlistfile2和vlistfile1, 得到vlistfile3和vlistfile4。
        CompVector();

        // 把容器vlistfile3中的内容写入okfilename文件，覆盖之前的旧okfilename文件
        WriteToOkFile();

        // 把vlistfile4中的内容复制到vlistfile2中。
        vlistfile2.clear(); vlistfile2.swap(vlistfile4);

    }

    PActive.UptATime(); // 更新进程的心跳

    char strremotefilename[301], strlocalfilename[301];
    // 遍历容器vlistfile2
    for(auto vlf : vlistfile2)
    {
        SNPRINTF(strremotefilename, sizeof(strremotefilename), 300, "%s/%s", starg.remotepath, vlf.filename);
        SNPRINTF(strlocalfilename, sizeof(strlocalfilename), 300, "%s/%s", starg.localpath, vlf.filename);
        
        logfile.Write("put %s ... ", strlocalfilename);
        // 调用ftp.put()方法把文件上传到服务端，第三个参数填true的目的是确保文件上传成功，对方不可抵赖
        if(ftp.put(strlocalfilename, strremotefilename, true) == false) 
        {
            logfile.WriteEx("failed.\n");
            return false;
        }

        logfile.WriteEx("ok.\n");

        PActive.UptATime(); // 更新进程的心跳
        
        // 如果ptype==1, 把上传成功的文件记录追加到okfilename文件中
        if(starg.ptype == 1) AppendToOkFile(&vlf);

        // 删除文件
        if(starg.ptype == 2)
        {   
            if(REMOVE(strlocalfilename) == false)
            {
                logfile.Write("REMOVE(%s) failed.\n", strlocalfilename); return false;
            }
        }
        // 转存到备份目录
        if(starg.ptype == 3)
        {
            char strlocalfilenamebak[301];
            SNPRINTF(strlocalfilenamebak, sizeof(strlocalfilenamebak), 300, "%s/%s", starg.localpathbak, vlf.filename);
            if(RENAME(strlocalfilename, strlocalfilenamebak) == false)
            {
                logfile.Write("RENAME(%s,%s) failed.\n", strlocalfilename, strlocalfilenamebak); return false;
            }
        }
    }
    return true;
}

// 把ftp.nlist()方法获取到的list文件加载到容器vlistfile2中
bool LoadListFile()
{
    vlistfile2.clear();

    CDir Dir;
    Dir.SetDateFMT("yyyymmddhh24miss");

    if(Dir.OpenDir(starg.localpath, starg.matchname) == false)
    {
        logfile.Write("Dir.OpenDir(%s) failed.\n", starg.localpath); return false;
    }

    struct st_fileinfo stfileinfo;

    while(true)
    {
        memset(&stfileinfo, 0, sizeof(struct st_fileinfo));

        if(Dir.ReadDir() == false) break;

        strcpy(stfileinfo.filename, Dir.m_FileName);        // 文件名，不包括目录名
        strcpy(stfileinfo.mtime, Dir.m_ModifyTime);         // 文件时间

        vlistfile2.push_back(stfileinfo);
    }
    
    return true;
}

// 加载okfilename文件中的内容到容器vlistfile1中
bool LoadOKFile()
{
    vlistfile1.clear();

    CFile File;
    // 注意：如果程序是第一次上传，okfilename是不存在的，并不是错误，所以返回true
    if(File.Open(starg.okfilename, "r") == false) return true;

    char strbuffer[501];
    struct st_fileinfo stfileinfo;

    while(true)
    {
        memset(&stfileinfo, 0, sizeof(struct st_fileinfo));

        if(File.Fgets(strbuffer, 300, true) == false) break;

        GetXMLBuffer(strbuffer, "filename", stfileinfo.filename);
        GetXMLBuffer(strbuffer, "mtime", stfileinfo.mtime);

        vlistfile1.push_back(stfileinfo);
    }

    return true;
}

// 比较vlistfile2和vlistfile1, 得到vlistfile3和vlistfile4。
bool CompVector()
{   
    vlistfile3.clear(); vlistfile4.clear();

    int ii, jj;
    // 遍历vlistfile2
    for(ii = 0; ii < vlistfile2.size(); ii++)
    {
        // 在vlistfile1中查找vlistfile2[ii]的记录
        for(jj = 0; jj < vlistfile1.size(); jj++)
        {
            // 如果找到了，把记录放入vlistfile3
            if(strcmp(vlistfile2[ii].filename, vlistfile1[jj].filename) == 0 && strcmp(vlistfile2[ii].mtime, vlistfile1[jj].mtime) == 0)
            {
                vlistfile3.push_back(vlistfile2[ii]); break;
            }
        }
        
        // 如果没找到，把记录放入vlistfile4
        if(jj == vlistfile1.size()) vlistfile4.push_back(vlistfile2[ii]);
    }

    return true;
}

// 把容器vlistfile3中的内容写入okfilename文件，覆盖之前的旧okfilename文件
bool WriteToOkFile()
{
    CFile File;

    if(File.Open(starg.okfilename, "w") == false)
    {
        logfile.Write("File.Open(%s) failed.\n", starg.okfilename); return false;
    }

    for(auto vlf3 : vlistfile3)
    {
        File.Fprintf("<filename>%s</filename><mtime>%s</mtime>\n", vlf3.filename, vlf3.mtime);
    }

    return true;
}


// 如果ptype==1, 把上传成功的文件记录追加到okfilename文件中
bool AppendToOkFile(struct st_fileinfo *st_fileinfo)
{
    CFile File;

    if(File.Open(starg.okfilename, "a") == false)
    {
        logfile.Write("File.Open(%s) failed.\n", starg.okfilename); return false;
    }

    File.Fprintf("<filename>%s</filename><mtime>%s</mtime>\n", st_fileinfo->filename, st_fileinfo->mtime);

    return true;
}