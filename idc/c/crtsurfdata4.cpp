/*
 * 程序名：crtsurfdata4.cpp 本程序用于全球气象站点观测的分钟数据。
 * 作者： wzm
 */

#include "_public.h"

// 全国气象站点参数结构体。
struct st_stcode
{
	char provname[31];	// 省
	char obtid[11];		// 站号
	char obtname[31];	// 站名
	double lat;			// 纬度
	double lon;			// 经度
	double height;		// 海拔高度
};

// 存放全国气象站点参数的容器。
vector<struct st_stcode> vstcode;

// 把站点参数文件中加载到vstcode容器中。
bool LoadSTCode(const char *inifile);

// 全国气象站点分钟观测数据结构
struct st_surfdata
{
	char obtid[11];		// 站点代码
	char ddatetime[21];	// 数据时间： 格式yyyymmddhh24miss
	int t;				// 气温：单位，0.1摄氏度
	int p;				// 气压：0.1百帕
	int u;				// 相对湿度，0-100之间的值
	int wd;				// 风向，0-360之间的值
	int wf;				// 风速，单位0.1m/s
	int r;				// 降雨量：0.1mm
	int vis;			// 能见度：0.1米
};

vector<struct st_surfdata> vsurfdata;	// 存放全国气象站点分钟观测数据的容器

char strddatetime[21]; // 观测数据的时间

// 模拟生成全国气象站点分钟观测数据，存放在vsurfdata容器中
void CtrSurfData();

// 把容器vsurfdata中的全国气象站点分钟观测数据写入文件
bool CtrSurfFile(const char *output, const char *datafmt);

CLogFile logfile(10);

int main(int argc, char* argv[])
{

	// inifile outpath logfile 
	if(argc != 5)
	{
		printf("Using: ./crtsurfdata4 inifile outpath logfile datafmt\n");
		printf("Example: /project/idc1/bin/crtsurfdata4 /project/idc1/ini/stcode.ini /tmp/idc/surfdata /log/idc/crtsurfdata4.log xml,json,csv\n\n");

		printf("inifile 全国气象站点参数文件名。\n");
		printf("outpath 全国气象站点数据文件存放的目录。\n");
		printf("logfile 本程序运行的日志文件。\n");
		printf("datafmt 生成数据文件的格式, 支持xml、json和csv三种格式, 中间用逗号分隔。\n\n");

		return -1;
	}

	if(logfile.Open(argv[3]) == false)
	{
		printf("logfile.Open(%s) failed.\n", argv[3]); return -1;
	}
	logfile.Write("crtsurfdata4 开始运行。\n");

	// 这里插入业务代码
	// 把站点参数文件中加载到vstcode容器中。
	if(LoadSTCode(argv[1]) == false) return -1;

	// 模拟生成全国气象站点分钟观测数据，存放在vsurfdata容器中
	CtrSurfData();

	// 把容器vsurfdata中的全国气象站点分钟观测数据写入文件
	if(strstr(argv[4], "xml")) CtrSurfFile(argv[2], "xml");
	if(strstr(argv[4], "csv")) CtrSurfFile(argv[2], "csv");
	if(strstr(argv[4], "json")) CtrSurfFile(argv[2], "josn");

	logfile.Write("crtsurfdata4 运行结束。\n");

	return 0;
}

// 把站点参数文件中加载到vstcode容器中。
bool LoadSTCode(const char *inifile)
{
	CFile File;

	// 打开站点参数文件。
	if(File.Open(inifile, "r") == false)
	{
		logfile.Write("File.Open(%s) failed.\n", inifile); return false;
	}

	char strBuffer[301];

	CCmdStr CmdStr;

	struct st_stcode stcode;

	while(true)
	{
		// 从站点参数文件中读取一行，如果已读取完，跳出循环。
		if(File.Fgets(strBuffer, 300, true) == false)  break;

		// 把读取到的一行拆分。
		CmdStr.SplitToCmd(strBuffer, ",", true);

		if(CmdStr.CmdCount() != 6) continue; // 扔掉无效的行。

		// 把站点参数的每个数据项保存到站点参数结构体中。
		CmdStr.GetValue(0, stcode.provname, 30);	// 省
		CmdStr.GetValue(1, stcode.obtid, 10);		// 站号
		CmdStr.GetValue(2, stcode.obtname, 30);		// 站名
		CmdStr.GetValue(3, &stcode.lat);			// 纬度
		CmdStr.GetValue(4, &stcode.lon);			// 经度
		CmdStr.GetValue(5, &stcode.height);			// 海拔高度

		// 把站点参数结构体放入站点参数容器。
		vstcode.push_back(stcode);
	}
	/*
	for(auto st : vstcode)
	{
		logfile.Write("provname=%s, obtid=%s, obtname=%s, lat=.2f, lon=.2f, height=%.2f\n", \
					   stcode.provname, stcode.obtid, stcode.obtname, stcode.lat, stcode.lon, stcode.height);
	}
	*/
	return true;
}

// 模拟生成全国气象站点分钟观测数据，存放在vsurfdata容器中
void CtrSurfData()
{
	// 播随机数种子
	srand(time(0));

	// 获取当前时间，当成观测时间
	memset(strddatetime, 0, sizeof(strddatetime));
	LocalTime(strddatetime, "yyyymmddhh24miss");

	struct st_surfdata stsurfdata;

	// 遍历气象站点参数的vscode容器
	for(auto vst : vstcode)
	{
		memset(&stsurfdata, 0, sizeof(stsurfdata));
		// 用随机数填充分钟观测数据的结构体
		strncpy(stsurfdata.obtid, vst.obtid, 10);			// 站点代码
		strncpy(stsurfdata.ddatetime, strddatetime, 14);	// 数据时间：格式yyyymmddhh24miss
		stsurfdata.t = rand()%351;							// 气温：单位，0.1摄氏度
		stsurfdata.p = rand()%265+10000;					// 气压：0.1百帕
		stsurfdata.u = rand()%100+1;						// 相对湿度，0-100之间的值
		stsurfdata.wd = rand()%360;							// 风向，0-360之间的值
		stsurfdata.wf = rand()%150;							// 风速，单位0.1m/s
		stsurfdata.r = rand()%16;							// 降雨量：0.1mm
		stsurfdata.vis = rand()%5001+100000;				// 能见度：0.1米

		// 把观测数据的结构体放入vsurfdata容器
		vsurfdata.push_back(stsurfdata);

	}
}

// 把容器vsurfdata中的全国气象站点分钟观测数据写入文件
bool CtrSurfFile(const char *output, const char *datafmt)
{
	CFile File;
	// 拼接生成数据的文件名，例如： /tmp/surfdata/SURF_ZH_20210629092200_2254.csv
	char strFileName[301];
	sprintf(strFileName, "%s/SURF_ZH_%s_%d.%s", output, strddatetime, getpid(), datafmt);

	// 打开文件
	if(File.OpenForRename(strFileName, "w") == false)
	{
		logfile.Write("File.OpenForRename(%s) failed.\n", strFileName); return false;
	}

	// 写入第一行标题
	if(strcmp(datafmt, "csv") == 0)  File.Fprintf("站点代码,数据时间,气温,气压,相对湿度,风向,风速,降雨量,能见度\n");

	// 遍历存放观测数据的vsurfdata容器
	for(auto vsd : vsurfdata)
	{
		// 写入一条记录
		if(strcmp(datafmt, "csv") == 0)
			File.Fprintf("%s,%s,%.1f,%.1f,%d,%d,%.1f,%.1f,%.1f\n", 
						  vsd.obtid, vsd.ddatetime, vsd.t/10.0, vsd.p/10.0, vsd.u, vsd.wd, vsd.wf/10.0, vsd.r/10.0, vsd.vis/10.0);
	}

	// 关闭文件
	File.CloseAndRename();

	logfile.Write("生成数据文件%s成功, 数据时间%s, 记录数%d。\n", strFileName, strddatetime, vsurfdata.size());

	return true;
}