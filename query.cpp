#include<iostream>
#include <stdlib.h>
#include<string>
#include <unistd.h>
using namespace std;
int main(int argc, char* argv[])
{	
	string name=argv[1];
	chdir("/opt/workspace/gStoreD");
	string tmp=name.substr(name.length()-3,3);
	if(tmp.compare("PCP")==0)
		system(("mpiexec -f hosts.txt -n 5 bin/gqueryD "+name+" SPARQL.txt 0 /opt/workspace/PCP/"+name+"crossingEdges.txt").data());
	else if(tmp.compare("ash")==0)
		system(("mpiexec -f hosts.txt -n 5 bin/gqueryD "+name+" SPARQL.txt 1").data());
	else if(tmp.compare("tis")==0)
		system(("mpiexec -f hosts.txt -n 5 bin/gqueryD "+name+" SPARQL.txt 1").data());
	else if(tmp.compare("_VP")==0)
		system(("mpiexec -f hosts.txt -n 5 bin/gqueryD_VP "+name+" SPARQL.txt "+"VPEdges/"+name+"Edges.txt").data());
	return 0;
}