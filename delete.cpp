#include<iostream>
#include <stdlib.h>
#include<string>
#include <unistd.h>
#include<fstream>
using namespace std;
int main(int argc, char* argv[])
{	
	string name=argv[1];
	chdir("/opt/workspace/gStoreD");
	system(("mpiexec -f hosts.txt -n 4 bin/gdrop "+name).data());
	string info="delete where{ <"+name+"> ?p ?o .}";
	ofstream out("/opt/workspace/gStoreD/delete.q");
    out<<info;
    out.close();
    system("bin/gquery databaseInfo delete.q");
	system("bin/gquery databaseInfo dbInfo.q dbInfo.txt");
	return 0;
}