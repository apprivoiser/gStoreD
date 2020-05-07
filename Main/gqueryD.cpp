/*=============================================================================
# Filename: gqueryD.cpp
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

<<<<<<< HEAD
		long all_time;
=======
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
		long _begin = Util::get_cur_time();
		string db_folder=string(argv[1]);
		string sparql=string(argv[2]);
		sparql=Util::getQueryFromFile(sparql.c_str());
		string type=string(argv[3]);
		vector<decompose_query_result> result;
	    GeneralEvaluation general_evaluation(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
	    int star=0;
		cout<<"gqueryD..."<<endl;
	    {
	        cout<<"DB_store:"<<argv[1]<<"\n";
	        cout<<"sparql:"<<sparql<<"\n\n";
	    }
	    //------------------------------------------decompose_query------------------------------------------------------
	    long decompose_query_cost_begin = Util::get_cur_time();
		if(type.compare("0")==0)
	    {
	    	string crossingEdge_file=string(argv[4]);
	    	set<string> crossingEdge;
<<<<<<< HEAD
		    unordered_map<string,int> edge_cnt;
=======
		    map<string,int> edge_cnt;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
		    ifstream infile(crossingEdge_file.c_str());
		    string buff;
		    while(getline(infile,buff))
		    {
		        vector<string> tmp=Util::split(buff,"\t");
<<<<<<< HEAD
		        // edge_cnt[tmp[0]]=atoi(tmp[1].c_str());
=======
		        edge_cnt[tmp[0]]=atoi(tmp[1].c_str());
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
		        if(tmp[2].compare("1")==0)crossingEdge.insert(tmp[0]);
		    }
		    infile.close();
	    	star=general_evaluation.decompose_query(sparql,result,crossingEdge,edge_cnt);
	    }
	    else if(type.compare("1")==0)
	    {
	    	star=general_evaluation.decompose_query(sparql,result);
<<<<<<< HEAD
	    	// cout<<sparql<<endl;
=======
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
	    }
	    long decompose_query_cost_parse = Util::get_cur_time();
		cout << "decompose_query_cost used " << (decompose_query_cost_parse - decompose_query_cost_begin) << "ms." << endl;

<<<<<<< HEAD
		int query_ans_cnt=0;
	    set<vector<int> > query_ans_hash;
	    unordered_map<string,int> URIIDMap;
	    vector<string> IDURIMap;
	    IDURIMap.push_back("");
	    // unsigned long long _buffer_size;
	    // SITree* URIIDMap=new SITree(".", string("URIIDMap"), "new", _buffer_size);
=======
	    set<vector<int> > query_ans_hash;
	    map<string,int> URIIDMap;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
	    int URIID=0;
	    if(star==0) //not star
	    {   	
			//----------------------------------------------send query-----------------------------------------------------
			long send_query_begin = Util::get_cur_time();
			set<string> knownPoint;
<<<<<<< HEAD
			vector<unordered_map<string,int> > queryPoint(result.size(),unordered_map<string,int>());
=======
			vector<map<string,int> > queryPoint(result.size(),map<string,int>());
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
			stringstream query_ss;
			vector<vector<vector<string> > > block_result(result.size(),vector<vector<string> >());
			for(int i=1;i<result.size();i++)block_result[i].push_back(vector<string>());

		   	for(int i=1;i<result.size();i++)
			{
				cout<<endl<<"block: "<<i<<endl;
				string ssparql="";
				int queryPoint_cnt=0;
				for(int j=0;j<result[i].edge.size();j++)
				{
					vector<string> tmp=Util::split(result[i].edge[j],"\t");
					if(tmp[0][0]=='?')
					{
						if(queryPoint[i].count(tmp[0])==0)
<<<<<<< HEAD
							queryPoint[i].emplace(tmp[0],1);
=======
							queryPoint[i][tmp[0]]=1;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
					}
					else knownPoint.insert(tmp[0]);
					if(tmp[1][0]=='?')
					{
						if(queryPoint[i].count(tmp[1])==0)
<<<<<<< HEAD
							queryPoint[i].emplace(tmp[1],1);
=======
							queryPoint[i][tmp[1]]=1;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
					}
					if(tmp[2][0]=='?')
					{
						if(queryPoint[i].count(tmp[2])==0)
<<<<<<< HEAD
							queryPoint[i].emplace(tmp[2],1);
=======
							queryPoint[i][tmp[2]]=1;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
					}
					else knownPoint.insert(tmp[2]);
					ssparql+=result[i].edge[j]+" .\n";
				}
				if(queryPoint[i].size())
				{
					string start="select ";
<<<<<<< HEAD
					for(unordered_map<string,int>::iterator it=queryPoint[i].begin();it!=queryPoint[i].end();it++) it->second=queryPoint_cnt++,start+=it->first+" ";
=======
					for(map<string,int>::iterator it=queryPoint[i].begin();it!=queryPoint[i].end();it++) it->second=queryPoint_cnt++,start+=it->first+" ";
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
					ssparql=start+"where {\n"+ssparql+"}";
					cout<<ssparql<<endl;
					query_ss<<ssparql<<"-1";
				}
				else
				{
					cout<<"all known"<<endl;
					block_result[i]=vector<vector<string> >(2,vector<string>());
				}

				cout<<"connection: "<<endl;
				for(int j=0;j<result[i].connection.size();j++)
					cout<<result[i].connection[j].first<<" "<<result[i].connection[j].second<<endl;
			}

			char* query_arr = new char[query_ss.str().size() + 1];
            strcpy(query_arr,query_ss.str().c_str());
            size = strlen(query_arr);
			for(int p=1;p<numprocs;p++)
	    	{
				MPI_Send(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD);
				MPI_Send(query_arr, size, MPI_CHAR, p, 10, MPI_COMM_WORLD);
	    	}
	    	delete[] query_arr;
	    	long send_query_parse = Util::get_cur_time();
			cout << "send_query used " << (send_query_parse - send_query_begin) << "ms." << endl;

	    	//----------------------------------------receive local result--------------------------------------------------
	    	long receive_local_result_begin = Util::get_cur_time();
<<<<<<< HEAD
	    	unordered_map<unsigned long long,vector<int> >* connectionToBlock=new unordered_map<unsigned long long,vector<int> >[result.size()];
=======
	    	// map<unsigned long long,vector<int>* >* connectionToBlock=new map<unsigned long long,vector<int>* >[result.size()];
	    	map<unsigned long long,vector<int> >* connectionToBlock=new map<unsigned long long,vector<int> >[result.size()];
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
	    	for(int p=1;p<numprocs;p++)
	    	{
	            for(int i=1;i<result.size();i++)if(queryPoint[i].size())
	            {
	            	while(true)
	            	{
	            		MPI_Recv(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD, &status);
				        char* query_ans_arr = new char[size+1];
				        MPI_Recv(query_ans_arr, size, MPI_CHAR, p, 10, MPI_COMM_WORLD, &status);
				        query_ans_arr[size]=0;
				        if(strcmp(query_ans_arr, "done")==0) break;
	        			vector<string> query_ans_tmp=Util::split(query_ans_arr,"\n");
	        			// cout<<query_ans_tmp.size()<<endl;
		            	for(int j=0;j<query_ans_tmp.size();j++)
							block_result[i].push_back(Util::split(query_ans_tmp[j],"\t"));
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
<<<<<<< HEAD
	        		all_time=_parse - _begin;
					cout << "all used " << (_parse - _begin) << "ms." << endl;
					ofstream write;
				    write.open("ans.txt");
					write.close();
=======
					cout << "all used " << (_parse - _begin) << "ms." << endl;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
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
						unsigned long long hash_val=Util::BKDRHash(connection_val.c_str());
						// hash_val=((hash_val<<4)&0x7FFFFFFF)|connection_block;
						hash_val=(hash_val<<4)|connection_block;
						if(connectionToBlock[i].count(hash_val)==0)
<<<<<<< HEAD
							connectionToBlock[i].emplace(hash_val,vector<int>());
=======
							// connectionToBlock[i][hash_val]=new vector<int>();
							connectionToBlock[i][hash_val]=vector<int>();
						// (*connectionToBlock[i][hash_val]).push_back(j);
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
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
<<<<<<< HEAD
			{
=======
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
				if(mn>block_result[i].size())
				{
					mn=block_result[i].size();
					st=i;
				}
<<<<<<< HEAD
				cout<<i<<" "<<block_result[i].size()<<endl;
			}
=======
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8

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
						// cout<<"final"<<endl;
						vector<int> ansss;
<<<<<<< HEAD
						set<string> tmpx;
						for(int j=1;j<result.size();j++)
						{
							for(unordered_map<string,int>::iterator it=queryPoint[j].begin();it!=queryPoint[j].end();it++)
							{
								if(tmpx.count(it->first))
									continue;
								tmpx.insert(it->first);
								string val=block_result[j][tmp_ans[j]][it->second];
								if(URIIDMap.count(val)==0)
		            			{
		            				URIIDMap.emplace(val,++URIID);
		            				IDURIMap.push_back(val);
=======
						for(int j=1;j<result.size();j++)
						{
							for(map<string,int>::iterator it=queryPoint[j].begin();it!=queryPoint[j].end();it++)
							{
								string val=block_result[j][tmp_ans[j]][it->second];
								if(URIIDMap.count(val)==0)
		            			{
		            				URIIDMap[val]=++URIID;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
		            			}
		            			ansss.push_back(URIIDMap[val]);
							}
						}
						query_ans_hash.insert(ansss);
						continue;
					}
					int t=tmp_queue.front();
					// cout<<t<<endl;
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
							unsigned long long hash_val=Util::BKDRHash(cp.c_str());
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
<<<<<<< HEAD
			all_time=_parse - _begin;
=======
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
			cout << "all used " << (_parse - _begin) << "ms." << endl;
			delete[] connectionToBlock;
	    }
	    else	//star
	    {
	    	//----------------------------------------------send query-----------------------------------------------------
			long send_query_begin = Util::get_cur_time();
	    	sparql+="-1";
	    	char* query_arr = new char[sparql.length() + 1];
            strcpy(query_arr,sparql.c_str());
            size = strlen(query_arr);
	    	for(int p=1;p<numprocs;p++)
	    	{
				MPI_Send(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD);
				MPI_Send(query_arr, size, MPI_CHAR, p, 10, MPI_COMM_WORLD);
	    	}
	    	delete[] query_arr;
	    	long send_query_parse = Util::get_cur_time();
			cout << "send_query used " << (send_query_parse - send_query_begin) << "ms." << endl;

	    	//----------------------------------------receive local result--------------------------------------------------
	    	long receive_local_result_begin = Util::get_cur_time();
	    	for(int p=1;p<numprocs;p++)
	    	{
	    		while(true)
	    		{
	    			MPI_Recv(&size, 1, MPI_INT, p, 10, MPI_COMM_WORLD, &status);
			        char* query_ans_arr = new char[size+1];
			        MPI_Recv(query_ans_arr, size, MPI_CHAR, p, 10, MPI_COMM_WORLD, &status);
			        query_ans_arr[size]=0;
			        if(strcmp(query_ans_arr, "done")==0) break;
        			vector<string> query_ans_tmp=Util::split(query_ans_arr,"\n");
        			// cout<<p<<" "<<query_ans_tmp.size()<<endl;
<<<<<<< HEAD
	            	// if(star==1)
	            	if(1)
	            	{
	            		for(int i=0;i<query_ans_tmp.size();i++)
						{
							vector<string> tmpss=Util::split(query_ans_tmp[i],"\t");
		            		vector<int> ansss;
		            		for(int j=0;j<tmpss.size();j++)
		            		{
		            			if(URIIDMap.count(tmpss[j])==0)
		            			{
		            				URIIDMap.emplace(tmpss[j],++URIID);
		            				IDURIMap.push_back(tmpss[j]);
		            			}
		            			ansss.push_back(URIIDMap[tmpss[j]]);
		            		}
		            		query_ans_hash.insert(ansss);
						}
	            	}
	            	else
	            	{
	            		query_ans_cnt+=query_ans_tmp.size();
	            	}
=======
	            	for(int i=0;i<query_ans_tmp.size();i++)
					{
						vector<string> tmpss=Util::split(query_ans_tmp[i],"\t");
	            		vector<int> ansss;
	            		for(int j=0;j<tmpss.size();j++)
	            		{
	            			if(URIIDMap.count(tmpss[j])==0)
	            			{
	            				URIIDMap[tmpss[j]]=++URIID;
	            			}
	            			ansss.push_back(URIIDMap[tmpss[j]]);
	            		}
	            		query_ans_hash.insert(ansss);
					}
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
	            	delete[] query_ans_arr;
	    		}
	        }
	        long receive_local_result_parse = Util::get_cur_time();
			cout << "receive_local_result used " << (receive_local_result_parse - receive_local_result_begin) << "ms." << endl;
			long _parse = Util::get_cur_time();
<<<<<<< HEAD
			all_time=_parse - _begin;
			cout << "start!!! all used " << (_parse - _begin) << "ms." << endl;
	    }
	    cout<<"final results:";
	    // if(star==-1)
	    // 	cout<<query_ans_cnt<<endl;
	    // else
	    	cout<<query_ans_hash.size()<<endl;

	    ofstream write;
	    write.open("ans.txt");
	    for(set<vector<int> >::iterator it=query_ans_hash.begin();it!=query_ans_hash.end();it++)
	    {
	    	vector<int> tmp=*it;
	    	for(int i=0;i<tmp.size();i++)
	    		write<<IDURIMap[tmp[i]]<<" ";
	    	write<<endl;
	    }
		write.close();
=======
			cout << "start!!! all used " << (_parse - _begin) << "ms." << endl;
	    }
	    
	    cout<<"final results:"<<query_ans_hash.size()<<endl;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
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
		vector<string>ssparql=Util::split(sparql,"-1");
		long receive_local_query_parse = Util::get_cur_time();

		long local_query_total=0;
		long send_local_ans_total=0;
		for(int i=0;i<ssparql.size();i++)
		{
			// rs=gc.query(db_folder, "txt", ssparql[i]);
			// cout<<ssparql[i]<<endl;
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
<<<<<<< HEAD
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
=======
				vector<string> rs_vec=Util::split(rs,"\n");
				stringstream query_ans;
				for(int j=1;j<rs_vec.size();j++)
				{
					query_ans<<rs_vec[j]<<"\n";
					if(j%5000000==0||j==rs_vec.size()-1)
					{
						char* query_ans_arr = new char[query_ans.str().size() + 1];
						strcpy(query_ans_arr,query_ans.str().c_str());
						size = strlen(query_ans_arr);
				        MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
						MPI_Send(query_ans_arr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD);
						delete[] query_ans_arr;	
						query_ans.str("");
					}
				}
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
			}
			char* _finished_tag = new char[10];
            strcpy(_finished_tag, "done");
            size = strlen(_finished_tag);            	
            MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
            MPI_Send(_finished_tag, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD);            	
            delete[] _finished_tag;
            long send_local_ans_parse = Util::get_cur_time();
            send_local_ans_total+=send_local_ans_parse-send_local_ans_begin;
		}

		
		cout << "local_query on "<<myRank<<" used " << (receive_local_query_parse-receive_local_query_begin+local_query_total+send_local_ans_total) << "ms." << endl;
	}
	MPI_Finalize();
	return 0;
}