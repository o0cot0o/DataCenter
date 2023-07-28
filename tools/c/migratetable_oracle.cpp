/*
 *   程序名：migratetable_oracle.cpp 本程序是数据中心的公共功能模块，用于迁移表中的数据。
 *   作者：wzm
*/

#include "_tools_oracle.h"

#define MAXPARAMS 256

// 程序运行的参数的结构体
struct st_arg
{
    char    connstr[101];       // 数据库的连接参数。
    char    srctname[31];       // 待迁移的表名。
    char    dsttname[31];       // 目的表的表名。
    char    keycol[31];         // 待清理的表的唯一键字段名。
    char    where[1001];        // 待清理的数据需要满足的条件。
    char    starttime[31];      // 程序运行的时间区间。
    int     maxcount;           // 每执行一次迁移操作的记录数。
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
bool _migratetable();

// 判断当前时间是否在程序运行的时间区间内。
bool instarttime();

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

    // 判断当前时间是否在程序运行的时间区间内。
    if(instarttime() == false) return 0;

    PActive.AddPInfo(starg.timeout, starg.pname);       // 设置进程心跳参数。
    // 注意，在调试程序时，可以启用以下代码，防止超时
    // PActive.AddPInfo(starg.timeout*100, starg.pname);       // 设置进程心跳参数。
    
    if(conn.connecttodb(starg.connstr, NULL) != 0)
    {
        logfile.Write("connect database(%s) failed.\n%s\n", starg.connstr, conn.m_cda.message); EXIT(-1);
    }
    
    _migratetable();

    return 0;
}

void EXIT(int sig)
{
    printf("程序退出, sig=%d\n\n", sig);
    
    conn.disconnect();

    exit(0);
}

void _help()
{
    printf("\n");
    printf("Using:/project/tools1/bin/migratetable_oracle logfilename xmlbuffer\n");

    printf("Example:/project/tools1/bin/procctl 3600 /project/tools1/bin/migratetable_oracle /log/idc/migratetable_oracle_ZHOBTMIND1.log \"<connstr>qxidc/qxidcpwd@snorcl11g_130</connstr><srctname>T_ZHOBTMIND1</srctname><dsttname>T_ZHOBTMIND_HIS</dsttname><keycol>rowid</keycol><where>where ddatetime<sysdate-0.03</where><starttime>01,02,03,04,05,13</starttime><maxcount>300</maxcount><timeout>120</timeout><pname>migratetable_oracle_ZHOBTMIND1</pname>\"\n\n");

    printf("本程序是数据中心的公共功能模块，采用增量的方法同步MySQL数据库之间的表。\n");
    printf("logfilename     本程序运行的日志文件。\n");
    printf("xmlbuffer       本程序运行的参数，用xml表示，具体如下：\n\n");
    printf("connstr         数据库的连接参数，格式：ip,username,password,dbname,port。\n");
    printf("srctname        待迁移数据表的唯一键字段名。\n");
    printf("dsttname        迁移目的表的表面，注意：stctname和dsttname的结构必须完全相同。\n");
    printf("keycol          待清理的表的唯一键字段名。\n");
    printf("where           待清理的数据需要满足的条件，即SQL语句中的where部分。\n");
    printf("starttime       程序运行的时间区间，例如02，13表示：如果程序运行时，踏中02时和13时则运行，其它时间不运行。如果starttime为空，本参数将失效，只要本程序启动就会执行数据迁移，为了减少对数据库的压力，数据迁移一般在数据库最闲的时候进行。\n");
    printf("maxcount        每执行一次迁移操作的记录数，不能超过MAXPARAMS(256)。\n");
    printf("timeout         本程序的超时时间，单位：秒，建议设置120以上。\n");
    printf("pname           进程名，尽可能采用易懂的、与其他进程不同的名称，方便故障排查。\n\n\n");

}

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer)
{
    memset(&starg, 0, sizeof(struct st_arg));

    GetXMLBuffer(strxmlbuffer,"connstr",starg.connstr,100);
    if (strlen(starg.connstr)==0) { logfile.Write("connstr is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer,"srctname",starg.srctname,30);
    if (strlen(starg.srctname)==0) { logfile.Write("srctname is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer,"dsttname",starg.dsttname,30);
    if (strlen(starg.dsttname)==0) { logfile.Write("dsttname is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer,"keycol",starg.keycol,30);
    if (strlen(starg.keycol)==0) { logfile.Write("keycol is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer,"where",starg.where,1000);
    if (strlen(starg.where)==0) { logfile.Write("where is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer,"starttime",starg.starttime,30);

    GetXMLBuffer(strxmlbuffer,"maxcount",&starg.maxcount);
    if (starg.maxcount==0) { logfile.Write("maxcount is null.\n"); return false; }
    if (starg.maxcount>MAXPARAMS) starg.maxcount=MAXPARAMS;

    GetXMLBuffer(strxmlbuffer,"timeout",&starg.timeout);
    if (starg.timeout==0) { logfile.Write("timeout is null.\n"); return false; }

    GetXMLBuffer(strxmlbuffer,"pname",starg.pname,50);
    if (strlen(starg.pname)==0) { logfile.Write("pname is null.\n"); return false; }

    return true;
}

// 业务处理主函数。
bool _migratetable()
{
    CTimer Timer;

    // 从数据字典中获取表中全部的字段名。
    CTABCOLS TABCOLS;

    // 获取待迁移数据表的字段名，用starg,stctname或starg.dsttname都可以。
    if(TABCOLS.allcols(&conn, starg.dsttname) == false)
    {
        logfile.Write("表%s不存在。\n", starg.dsttname); return false;
    }

    char tmpvalue[51];      // 存放从表提取待删除记录的唯一键的值。
    // 从表中提取待删除记录的唯一键。
    sqlstatement stmtsel(&conn);
    stmtsel.prepare("select %s from %s %s", starg.keycol, starg.srctname, starg.where);
    stmtsel.bindout(1, tmpvalue, 50);

    // 拼接绑定删除SQL语句where 唯一键 in (...)的字符串。
    char bindstr[2001];
    char strtemp[11];

    memset(bindstr, 0, sizeof(bindstr));

    for (int ii = 0; ii < starg.maxcount; ii++)
    {
        memset(strtemp, 0, sizeof(strtemp));
        sprintf(strtemp, ":%lu,", ii+1);
        strcat(bindstr, strtemp);
    }

    bindstr[strlen(bindstr)-1] = 0; // 最后一个逗号是多余的。

    char keyvalues[starg.maxcount][51];      // 存放唯一键字段的值。

    // 准备插入和删除数据的SQL，一次迁移starg.maxcount条记录。
    sqlstatement stmtins(&conn);
    stmtins.prepare("insert into %s(%s) select %s from %s where %s in (%s)", starg.dsttname, TABCOLS.m_allcols, TABCOLS.m_allcols, starg.srctname, starg.keycol, bindstr);

    sqlstatement stmtdel(&conn);
    stmtdel.prepare("delete from %s where %s in (%s)", starg.srctname, starg.keycol, bindstr);

    for (int ii = 0; ii < starg.maxcount; ii++)
    {
        stmtins.bindin(ii+1, keyvalues[ii], 50);
        stmtdel.bindin(ii+1, keyvalues[ii], 50);
    }

    int ccount = 0;
    memset(keyvalues, 0, sizeof(keyvalues));

    if(stmtsel.execute() != 0)
    {
        logfile.Write("stmtsel.execute() failed.\n%s\n%s\n", stmtsel.m_sql, stmtsel.m_cda.message); return false;
    }

    while(true)
    {
        memset(tmpvalue, 0, sizeof(tmpvalue));

        // 获取结果集。
        if(stmtsel.next() != 0) break;

        strcpy(keyvalues[ccount], tmpvalue);
        ccount++;

        // 每starg.maxcount条记录执行一次删除语句。
        if(ccount == starg.maxcount)
        {
            // 先插入starg.dsttname表。
            if(stmtins.execute() != 0)
            {
                logfile.Write("stmtins.execute() failed.\n%s\n%s\n", stmtins.m_sql, stmtins.m_cda.message);
                if(stmtins.m_cda.rc != 1) return false;
            }

            // 再删除starg.srctname表。
            if(stmtdel.execute() != 0)
            {
                logfile.Write("stmtdel.execute() failed.\n%s\n%s\n", stmtdel.m_sql, stmtdel.m_cda.message); return false;
            }

            conn.commit();
            ccount = 0;
            memset(keyvalues, 0, sizeof(keyvalues));

            PActive.UptATime();
        }
    }

    // 如果不足starg.maxcount条记录，再执行一次删除。
    if(ccount > 0)
    {
        // 先插入starg.dsttname表。
        if(stmtins.execute() != 0)
        {
            logfile.Write("stmtins.execute() failed.\n%s\n%s\n", stmtins.m_sql, stmtins.m_cda.message);
            if(stmtins.m_cda.rc != 1) return false;
        }

        // 再删除starg.srctname表。
        if(stmtdel.execute() != 0)
        {
            logfile.Write("stmtdel.execute() failed.\n%s\n%s\n", stmtdel.m_sql, stmtdel.m_cda.message); return false;
        }

        conn.commit();
    }

    if(stmtsel.m_cda.rpc > 0)
        logfile.Write("migrate %s to %s %d rows in %.2fsec.\n", starg.srctname, starg.dsttname, stmtsel.m_cda.rpc, Timer.Elapsed());

    return true;
}

// 判断当前时间是否在程序运行的时间区间内。
bool instarttime()
{
    // 程序运行的时间区间，例如02，13表示：如果程序启动时，踏中02时和13时则运行，其它时间不运行。

    if(strlen(starg.starttime) != 0)
    {
        char strHH24[3];

        memset(strHH24, 0, sizeof(strHH24));
        LocalTime(strHH24, "hhh24");
        if(strstr(starg.starttime, strHH24) == 0) return false;
    }

    return true;
}