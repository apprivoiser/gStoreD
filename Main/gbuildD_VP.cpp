#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <stdlib.h>
#include <queue>
#include "../Database/Database.h"
#include "../Util/Util.h"
#include "../api/http/cpp/client.h"
void update(string RDF,int triples,int entityCnt,int label)
{
    string info="insert data{ <"+RDF+"> <triple> \""+to_string(triples)+"\" .<"+RDF+"> <entity> \""+to_string(entityCnt)+"\" .<"+RDF+"> <label> \""+to_string(label)+"\" .}";
    ofstream out("/opt/workspace/gStoreD/insert.q");
    out<<info;
    out.close();
}
int main(int argc, char* argv[])
{
    int numprocs, myRank, source,namelen,size;
    double loadingStart, loadingEnd;
    double partitionStart, partitionEnd;
    MPI_Status status;
    char processor_name[100];
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Get_processor_name(processor_name,&namelen);
    printf("%d on %s\n", myRank,processor_name);
    if (myRank==0) 
    {  
        string _db_path=string(argv[1]);
        string _rdf_file=string(argv[2]);

        cout << "gbuildD..." << endl;
        {
            cout << "argc: " << argc << "\n";
            cout << "DB_store:" << argv[1] << "\n";
            cout << "RDF_data: " << argv[2] << "\n";
            cout << "host number: " << numprocs << "\n";
            cout << endl;
        }

        ifstream _fin(_rdf_file.c_str());
        if(!_fin) {
            cerr << "Fail to open the RDF data file: " << _rdf_file << endl;
            exit(0);
        }

        partitionStart = MPI_Wtime();
        map<string,int>preToID;
        map<int,string>IDToPre;
        set<string> entity;
        int triples=0;
        int preCnt=0;
        vector<pair<long long,int> >block;
        RDFParser _preparser(_fin);
        TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];
        while(true)
        {
            int parse_triple_num = 0;
            _preparser.parseFile(triple_array, parse_triple_num);
            if(parse_triple_num == 0) break;
            for(int i=0; i<parse_triple_num; i++)
            {
                string _pre = triple_array[i].getPredicate();
                if(preToID.count(_pre)==0)
                {
                    preToID[_pre]=++preCnt;
                    IDToPre[preCnt]=_pre;
                    block.push_back(make_pair(0,preCnt));
                }
                block[preToID[_pre]-1].first++;
            }
            
        }
        sort(block.begin(),block.end());
        vector<int>pre_pos(preCnt+1);
        priority_queue<pair<long long,int> >q;
        for(int i=0;i<numprocs-1;i++)q.push(make_pair(0,i));
        ofstream outFile("VPEdges/"+_db_path+"Edges.txt"); 
        for(int i=block.size()-1;i>=0;i--)
        {
            pair<long long,int> tmp=q.top();
            q.pop();
            tmp.first-=block[i].first;
            pre_pos[block[i].second]=tmp.second;
            q.push(tmp);
            outFile<<IDToPre[block[i].second]<<"\t"<<tmp.second<<endl;
        }
        outFile.close();
        _fin.close();
        partitionEnd = MPI_Wtime();
        double time_cost_value = partitionEnd - partitionStart;
        printf("It takes %f min to partition!\n", time_cost_value/60);

        while(!q.empty())
        {
            cout<<q.top().second<<"\t"<<q.top().first<<endl;
            q.pop();
        }

        stringstream* _six_tuples_ss = new stringstream[numprocs - 1];
        ifstream fin_(_rdf_file.c_str());
        RDFParser _parser(fin_);
        while(true)
        {
            int parse_triple_num = 0;

            _parser.parseFile(triple_array, parse_triple_num);

            printf("parse and communicate %d triples!\n", parse_triple_num);
            if(parse_triple_num == 0) {
                for(int i = 1; i < numprocs; i++){
                    char* _finished_tag = new char[32];
                    strcpy(_finished_tag, "all tuples have sent");
                    size = strlen(_finished_tag);
                    
                    MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
                    MPI_Send(_finished_tag, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
                    
                    delete[] _finished_tag;
                }
                break;
            }
            
            for(int i = 0; i < numprocs - 1; i++){
                _six_tuples_ss[i].str("");
            }

            /* Process the Triple one by one */
            for(int i = 0; i < parse_triple_num; i ++)
            {
                triples++;
                string _sub = triple_array[i].getSubject();
                entity.insert(_sub);
                string _pre = triple_array[i].getPredicate();
                string _obj = triple_array[i].getObject();
                
                if(!triple_array[i].isObjEntity())
                {
                    _obj=_obj.substr(1,_obj.size()-2);
                    string::iterator it = _obj.begin(); 
                    while(it!=_obj.end())
                    {
                        if(*it=='"'||*it=='\\'){it=_obj.insert(it,'\\');it++;}
                        it++;
                    }
                    _obj="\""+_obj+"\"";
                }
                else
                {
                    entity.insert(_obj);
                }

                int pos=pre_pos[preToID[_pre]];
                _six_tuples_ss[pos] << _sub   << '\t' << _pre << '\t' << _obj << " ." << endl;
            }
            
            for(int i = 1; i < numprocs; i++){
                char* _tuple_arr = new char[_six_tuples_ss[i - 1].str().size() + 1];
                strcpy(_tuple_arr, _six_tuples_ss[i - 1].str().c_str());
                size = strlen(_tuple_arr);
                // cout<<size<<endl;
                MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
                MPI_Send(_tuple_arr, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
                
                delete[] _tuple_arr;
            }
        }
        delete[] _six_tuples_ss;
        delete[] triple_array;
        fin_.close();
        update(argv[1],triples,entity.size(),preCnt);
    }
    else 
    {   
        remove("_distributed_gStore_tmp_rdf_triples.n3");
        ofstream _six_tuples_fout("_distributed_gStore_tmp_rdf_triples.n3");
        if(! _six_tuples_fout) {
            cerr << "Fail to open: _distributed_gStore_tmp_rdf_triples.n3" << endl;
            exit(0);
        }

        while(true){
            MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
            char* _tuples = new char[size+1];
            MPI_Recv(_tuples, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
            _tuples[size] = 0;
            
            if(strcmp(_tuples, "all tuples have sent") == 0){
                break;
            }else{
                _six_tuples_fout << _tuples;
            }
            delete[] _tuples;
        }
        _six_tuples_fout.close();

        string _db_path = string(argv[1]);
        
        // GstoreConnector gc("127.0.0.1", 9000, "root", "123456");
        // gc.drop(_db_path, false);
        // string rs=gc.build(_db_path, "_distributed_gStore_tmp_rdf_triples.n3");
        loadingStart = MPI_Wtime();
        Database _db(_db_path);
        _db.build("_distributed_gStore_tmp_rdf_triples.n3");

        remove("_distributed_gStore_tmp_rdf_triples.n3");

        loadingEnd = MPI_Wtime();
        double time_cost_value = loadingEnd - loadingStart;
        
        // cout<<rs<<endl;
        printf("%d takes %f min and finish loading data!\n", myRank, time_cost_value/60);
        
    }
    MPI_Finalize();
    return 0;
} 