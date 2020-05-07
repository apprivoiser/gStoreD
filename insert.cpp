#include<iostream>
#include <stdlib.h>
#include<string>
#include <unistd.h>
using namespace std;
int main(int argc, char* argv[])
{	
	string txt_name=argv[1];
	string name=argv[2];
	string op=argv[3];
	chdir("/opt/workspace/gStoreD");
	if(op.compare("6")==0)
	{
		system(("mpiexec -f hosts.txt -n 5 bin/gbuildD_VP "+name+" "+txt_name).data());
	}
	else
	{
		system(("/opt/workspace/graph/main "+txt_name+" "+name+" 2 "+op).data());
		// cout<<op<<endl;
		if(op.compare("3")==0)
			op="PCP";
		else if(op.compare("4")==0)
			op="sub_hash";
		else if(op.compare("5")==0)
			op="metis"; 
		// cout<<"mpiexec -f hosts.txt -n 5 bin/gbuildD "+name+" "+txt_name+" /opt/workspace/"+op+"/"+name+"InternalPoints.txt";
		system(("mpiexec -f hosts.txt -n 5 bin/gbuildD "+name+" "+txt_name+" /opt/workspace/"+op+"/"+name+"InternalPoints.txt").data());
	}
	system("bin/gquery databaseInfo insert.q");
	system("bin/gquery databaseInfo dbInfo.q dbInfo.txt");
	return 0;
}