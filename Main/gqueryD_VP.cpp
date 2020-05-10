/*=============================================================================
# Filename: gqueryD_VP.cpp
# Author: Cen Yan
# Last Modified: 
# Description: query
=============================================================================*/

#include <stdio.h> 
#include <iostream>
#include <string>
#include <set>
#include <fstream>
#include "../Database/Database.h"
#include "../Util/Util.h"
#include "../api/http/cpp/client.h"
#include <mpi.h>

using namespace std;
int main(int argc, char * argv[])
{
	int numprocs, myRank,namelen,size;
    double loadingStart, loadingEnd;
    MPI_Status status;
    char processor_name[100];
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Get_processor_name(processor_name,&namelen);
    // printf("%d on %s\n", myRank,processor_name);
    if(myRank==0)
    {
    	//-------------------------------------------receive signal----------------------------------------------------
		for(int p=1;p<numprocs;p++)
			MPI_Recv(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD, &status);
		for(int p=1;p<numprocs;p++)
			MPI_Send(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD);

		long _begin = Util::get_cur_time();
		string db_folder=string(argv[1]);
		string sparql=string(argv[2]);
		sparql=Util::getQueryFromFile(sparql.c_str());
		string pre_pos_file=string(argv[3]);
		vector<decompose_query_result> result;
	    GeneralEvaluation general_evaluation(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

		cout<<"gqueryD..."<<endl;
        {
            cout<<"DB_store:"<<argv[1]<<"\n";
            cout<<"sparql:"<<sparql<<"\n\n";
        }

        //------------------------------------------decompose_query------------------------------------------------------
	    long decompose_query_cost_begin = Util::get_cur_time();
	    long all_time;
	    unordered_map<string,int> pre_pos;
	    ifstream infile(pre_pos_file.c_str());
	    string buff;
	    while(getline(infile,buff))
	    {
	        vector<string> tmp=Util::split(buff,"\t");
	        pre_pos[tmp[0]]=atoi(tmp[1].c_str())+1;
	    }
	    infile.close();
    	general_evaluation.decompose_query_VP(sparql,result);
    	long decompose_query_cost_parse = Util::get_cur_time();
		cout << "decompose_query_cost used " << (decompose_query_cost_parse - decompose_query_cost_begin) << "ms." << endl;

    	//----------------------------------------------send query-----------------------------------------------------
		long send_query_begin = Util::get_cur_time();
	    set<vector<int> > query_ans_hash;
	    unordered_map<string,int> URIIDMap;
	    unordered_map<string,int> Var;
	    int Var_cnt=0;
	    int URIID=0;
	    vector<string> IDURIMap;
	    IDURIMap.push_back("");
		set<string> knownPoint;
		vector<unordered_map<string,int> > queryPoint(result.size(),unordered_map<string,int>());
		stringstream* query_ss = new stringstream[numprocs];
		vector<vector<vector<string> > > block_result(result.size(),vector<vector<string> >());
		for(int i=1;i<result.size();i++)block_result[i].push_back(vector<string>());

		vector<vector<int> >queryToMachine(numprocs,vector<int>());
	   	for(int i=1;i<result.size();i++)
		{
			cout<<endl<<"block: "<<i<<endl;
			string ssparql="";
			int queryPoint_cnt=0;
			vector<string> tmp=Util::split(result[i].edge[0],"\t");
			if(tmp[1][0]=='?')//pre
			{
				if(queryPoint[i].count(tmp[1])==0)
					queryPoint[i][tmp[1]]=1;
				for(int j=1;j<numprocs;j++)
					queryToMachine[pre_pos[tmp[1]]].push_back(j);	
			}
			else
				queryToMachine[pre_pos[tmp[1]]].push_back(i);
			if(tmp[0][0]=='?')
			{
				if(queryPoint[i].count(tmp[0])==0)
					queryPoint[i][tmp[0]]=1;
			}
			else knownPoint.insert(tmp[0]);
			if(tmp[2][0]=='?')
			{
				if(queryPoint[i].count(tmp[2])==0)
					queryPoint[i][tmp[2]]=1;
			}
			else knownPoint.insert(tmp[2]);
			ssparql+=result[i].edge[0]+" .\n";

			if(queryPoint[i].size())
			{
				string start="select ";
				for(unordered_map<string,int>::iterator it=queryPoint[i].begin();it!=queryPoint[i].end();it++)
				{
					if(Var.count(it->first)==0)
						Var.emplace(it->first,++Var_cnt);
					it->second=queryPoint_cnt++,start+=it->first+" ";
				} 
				ssparql=start+"where {\n"+ssparql+"}";
				cout<<ssparql<<endl;
				query_ss[pre_pos[tmp[1]]]<<ssparql<<"-1";
			}
			else
			{
				cout<<"all knownPoints"<<endl;
				block_result[i]=vector<vector<string> >(2,vector<string>());
			}

			cout<<"connection: "<<endl;
			for(int j=0;j<result[i].connection.size();j++)
				cout<<result[i].connection[j].first<<" "<<result[i].connection[j].second<<endl;
		}

		for(int p=1;p<numprocs;p++)
		{
			if(query_ss[p].str().size()==0)query_ss[p]<<"none";
			char* query_arr = new char[query_ss[p].str().size() + 1];
	        strcpy(query_arr,query_ss[p].str().c_str());
	        // cout<<p<<" "<<query_arr<<endl;
	        size = strlen(query_arr);
			MPI_Send(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD);
			MPI_Send(query_arr, size, MPI_CHAR, p, 10, MPI_COMM_WORLD);
	    	delete[] query_arr;
		}
    	delete[] query_ss;
    	long send_query_parse = Util::get_cur_time();
		cout << "send_query used " << (send_query_parse - send_query_begin) << "ms." << endl;
    	
    	//----------------------------------------receive local result--------------------------------------------------
	    long receive_local_result_begin = Util::get_cur_time();
    	unordered_map<string,int> connectionToID;
    	int connectionID=0;
    	unordered_map<unsigned long long,vector<int> >* connectionToBlock=new unordered_map<unsigned long long,vector<int> >[result.size()];
    	for(int p=1;p<numprocs;p++)
    	{
            for(int i=0;i<queryToMachine[p].size();i++)if(queryPoint[queryToMachine[p][i]].size())
            {
            	while(true)
            	{
            		MPI_Recv(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD, &status);
			        char* query_ans_arr = new char[size+1];
			        MPI_Recv(query_ans_arr, size, MPI_CHAR, p, 10, MPI_COMM_WORLD, &status);
			        query_ans_arr[size]=0;
			        if(strcmp(query_ans_arr, "done")==0) break;
        			vector<string> query_ans_tmp=Util::split(query_ans_arr,"\n");
	            	for(int j=0;j<query_ans_tmp.size();j++)
						block_result[queryToMachine[p][i]].push_back(Util::split(query_ans_tmp[j],"\t"));
	            	delete[] query_ans_arr;
            	}
            }
        }
        long receive_local_result_parse = Util::get_cur_time();
		cout << "receive_local_result used " << (receive_local_result_parse - receive_local_result_begin) << "ms." << endl;

        //---------------------------------------------connetion hash----------------------------------------------------
	    long connetion_hash_begin = Util::get_cur_time();
        for(int i=1;i<result.size();i++)
        {
        	if(block_result[i].size()==1)
        	{
        		cout<<"final results:0"<<endl;
        		long _parse = Util::get_cur_time();
        		all_time=(_parse - _begin);
				cout << "all used " << (_parse - _begin) << "ms." << endl;
				ofstream write;
			    write.open("ans.txt");
			    for(unordered_map<string,int>::iterator it=Var.begin();it!=Var.end();it++)
			    	write<<it->first<<" ";
			    write<<endl;
				write.close();
        		return 0;
        	}
        	for(int j=1;j<block_result[i].size();j++)
        	{
    			for(int k=0;k<result[i].connection.size();k++)
				{
					string connection_val=result[i].connection[k].first;
					if(connection_val[0]=='?')
					{
						int pos=queryPoint[i][connection_val];
						connection_val=block_result[i][j][pos];
					}
					unsigned int connection_block=result[i].connection[k].second;
					if(connectionToID.count(connection_val)==0)connectionToID[connection_val]=++connectionID;
					unsigned long long hash_val=connectionToID[connection_val];
					hash_val=(hash_val<<4)|connection_block;
					if(connectionToBlock[i].count(hash_val)==0)
						connectionToBlock[i][hash_val]=vector<int>();
					connectionToBlock[i][hash_val].push_back(j);
				}
        	}
        }
        long connetion_hash_parse = Util::get_cur_time();
		cout << "connetion_hash_result used " << (connetion_hash_parse - connetion_hash_begin) << "ms." << endl;

		//--------------------------------------------------join----------------------------------------------------------
		long join_begin = Util::get_cur_time();
		int mn=block_result[1].size(),st=1;//find the start
		for(int i=1;i<result.size();i++)
			if(mn>block_result[i].size())
			{
				mn=block_result[i].size();
				st=i;
			}

		queue<pair<vector<int>,queue<int> > >q;

		for(int i=1;i<block_result[st].size();i++)
		{
			queue<int> tmp_queue;
			tmp_queue.push(st);
			vector<int> tmp_ans(result.size(),0);
			tmp_ans[st]=i;
			q.push(make_pair(tmp_ans,tmp_queue));
			while(!q.empty())
			{
				tmp_ans=q.front().first;
				tmp_queue=q.front().second;
				q.pop();
				if(tmp_queue.empty())	//get the result
				{
					vector<int> ansss(Var.size());
					for(int j=1;j<result.size();j++)
					{
						for(unordered_map<string,int>::iterator it=queryPoint[j].begin();it!=queryPoint[j].end();it++)
						{
							string val=block_result[j][tmp_ans[j]][it->second];
							if(URIIDMap.count(val)==0)
	            			{
	            				URIIDMap[val]=++URIID;
	            				IDURIMap.push_back(val);
	            			}
	            			ansss[Var[it->first]-1]=URIIDMap[val];
						}
					}
					query_ans_hash.insert(ansss);
					continue;
				}
				int t=tmp_queue.front();
				tmp_queue.pop();
				int flag=1;
				queue<vector<int> >candidate_res;
				candidate_res.push(vector<int>());
				vector<int> candidateBlock;
				for(int j=0;j<result[t].connection.size();j++)
				{
					int b=result[t].connection[j].second;
					string cp=result[t].connection[j].first;

					if(tmp_ans[b])
					{
						if(cp[0]=='?')
						{
							if(block_result[t][tmp_ans[t]][queryPoint[t][cp]]!=block_result[b][tmp_ans[b]][queryPoint[b][cp]])
							{
								flag=0;
								break;
							}
						}
					}
					else
					{
						bool go=1;
						for(int k=0;k<candidateBlock.size();k++)if(candidateBlock[k]==b){go=0;break;}
						if(!go)continue;

						if(cp[0]=='?')cp=block_result[t][tmp_ans[t]][queryPoint[t][cp]];
						unsigned long long hash_val=connectionToID[cp.c_str()];
						hash_val=(hash_val<<4)|(unsigned int)t;
						if(connectionToBlock[b].count(hash_val)==0){flag=0;break;}
						candidateBlock.push_back(b);
						vector<int> candidate(connectionToBlock[b][hash_val]);
						while(true)
						{
							vector<int> last_candidate(candidate_res.front());
							if(last_candidate.size()<candidateBlock.size())candidate_res.pop();
							else break;
							for(int k=0;k<candidate.size();k++)
							{
								vector<int>next_candidate(last_candidate);
								next_candidate.push_back(candidate[k]);
								candidate_res.push(next_candidate);
							}
						}
						tmp_queue.push(b);
					} 
				}
				if(!flag)continue;
				while(!candidate_res.empty())
				{
					vector<int> tmp_candidate(candidate_res.front());
					candidate_res.pop();
					for(int j=0;j<candidateBlock.size();j++)
						tmp_ans[candidateBlock[j]]=tmp_candidate[j];
					q.push(make_pair(tmp_ans,tmp_queue));
				}
			}
		}
		long join_parse = Util::get_cur_time();
		cout << "join used " << (join_parse - join_begin) << "ms." << endl;
		
		long _parse = Util::get_cur_time();
		all_time=(_parse-_begin);
		cout << "all used " << (_parse - _begin) << "ms." << endl;

		delete[] connectionToBlock;
	    cout<<"final results:"<<query_ans_hash.size()<<endl;

	    ofstream write;
	    write.open("ans.txt");
	    for(unordered_map<string,int>::iterator it=Var.begin();it!=Var.end();it++)
	    	write<<it->first<<" ";
	    write<<endl;
	    for(set<vector<int> >::iterator it=query_ans_hash.begin();it!=query_ans_hash.end();it++)
	    {
	    	vector<int> tmp=*it;
	    	for(int i=0;i<tmp.size();i++)
	    		write<<IDURIMap[tmp[i]]<<" ";
	    	write<<endl;
	    }
		write.close();
	}
	else	//local
	{
		//-------------------------------------------------------load----------------------------------------------------------
		string db_folder=string(argv[1]);
		Util util;
	    Database _db(db_folder);
	    _db.load();
		// GstoreConnector gc("127.0.0.1", 9000, "root", "123456");
		// string rs=gc.load(db_folder);
		// cout<<rs<<endl;

		size = 1;//send signal
        MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
        MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);

        //-----------------------------------------------------local query--------------------------------------------------------
	   	long receive_local_query_begin = Util::get_cur_time();
	    MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
	    char* query_arr = new char[size+1];
		MPI_Recv(query_arr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
		query_arr[size]=0;
		string sparql=query_arr;
		delete[] query_arr;
		// cout<<sparql<<endl;
		long receive_local_query_parse = Util::get_cur_time();

		long local_query_total=0;
		long send_local_ans_total=0;
		if(sparql.compare("none"))
		{
			vector<string>ssparql=Util::split(sparql,"-1");
			stringstream query_ans;
			for(int i=0;i<ssparql.size();i++)
			{
				ResultSet _rs;
				long local_query_begin = Util::get_cur_time();
				_db.query(ssparql[i], _rs, stdout);
				long local_query_parse = Util::get_cur_time();
				local_query_total+=local_query_parse-local_query_begin;

				long send_local_ans_begin = Util::get_cur_time();
				string rs=_rs.to_str();
				// cout<<rs<<endl;
				if(rs.compare("[empty result]\n"))
				{
					long long st,ed;
					st=0;
					while(rs[st]!='\n')st++;
					ed=++st;
					int cnt=0;
					while(rs[ed])
					{
						if(rs[ed++]=='\n')
						{
							cnt++;
							if(cnt==5000000)
							{
								char* query_ans_arr = new char[ed-st+1];
								strcpy(query_ans_arr,rs.substr(st,ed-st).c_str());
								size = strlen(query_ans_arr);
						        MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
								MPI_Send(query_ans_arr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD);
								delete[] query_ans_arr;	
								cnt=0;
								st=ed;
							}	
						}
					}
					char* query_ans_arr = new char[ed-st+1];
					strcpy(query_ans_arr,rs.substr(st,ed-st).c_str());
					size = strlen(query_ans_arr);
			        MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
					MPI_Send(query_ans_arr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD);
					delete[] query_ans_arr;	
				}
				char* _finished_tag = new char[10];
	            strcpy(_finished_tag, "done");
	            size = strlen(_finished_tag);
	            
	            MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
	            MPI_Send(_finished_tag, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD);
	            long send_local_ans_parse = Util::get_cur_time();
            	send_local_ans_total+=send_local_ans_parse-send_local_ans_begin;
			}
		}

		cout << "local_query on "<<myRank<<" used " << (receive_local_query_parse-receive_local_query_begin+local_query_total+send_local_ans_total) << "ms." << endl;
	}
	MPI_Finalize();
	return 0;
}
