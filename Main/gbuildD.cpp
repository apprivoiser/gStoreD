#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include<stdlib.h>
#include "../Database/Database.h"
#include "../Util/Util.h"
#include "../api/http/cpp/client.h"
int main(int argc, char* argv[])
{
    int numprocs, myRank, source,namelen,size;
    double loadingStart, loadingEnd;
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
        string _internal_vertices_file=string(argv[3]);

        cout << "gbuildD..." << endl;
        {
            cout << "argc: " << argc << "\n";
            cout << "DB_store:" << argv[1] << "\n";
            cout << "RDF_data: " << argv[2] << "\n";
            cout << "internal_file_name: " << argv[3] << "\n";
            cout << "host number: " << numprocs << "\n";
            cout << endl;
        }
        
        map<string,int> verticesToMachine;
        ifstream infile(_internal_vertices_file.c_str());
        if(!infile){
            cout << "import internal vertices failed." << endl;
            exit(0);
        }
        string buff;
        while(getline(infile,buff))
        {
            vector<string> tmp=Util::split(buff,"\t");
            verticesToMachine[tmp[0]]=atoi(tmp[1].c_str());
        }
        infile.close();

        ifstream _fin(_rdf_file.c_str());
        if(!_fin) {
            cerr << "Fail to open the RDF data file: " << _rdf_file << endl;
            exit(0);
        }
        
        stringstream* _six_tuples_ss = new stringstream[numprocs - 1];
        RDFParser _parser(_fin);
        TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];
        
        int _sub_partition_id, _obj_partition_id;
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
                
                string _sub = triple_array[i].getSubject();
                string _pre = triple_array[i].getPredicate();
                string _obj = triple_array[i].getObject();
                
                if(!triple_array[i].isObjEntity())
                {
                    int ed=_obj.length()-1;
                    while(_obj[ed]!='"')ed--;
                    string _tmp=_obj.substr(1,ed-1);
                    _obj=_obj.substr(ed+1,_obj.size()-ed-1);
                    string::iterator it = _tmp.begin(); 
                    while(it!=_tmp.end())
                    {
                        if(*it=='"'||*it=='\\'){it=_tmp.insert(it,'\\');it++;}
                        it++;
                    }
                    _obj="\""+_tmp+"\""+_obj;
                }

                bool flag=0;
                if(verticesToMachine.count(_sub))
                {
                     flag=1;
                     _sub_partition_id=verticesToMachine[_sub];
                     _six_tuples_ss[_sub_partition_id] << _sub   << '\t' << _pre << '\t' << _obj << " ." << endl;
                }
                // obj is entity
                if (triple_array[i].isObjEntity())
                {
                    _obj_partition_id=verticesToMachine[_obj];
                    if(_sub_partition_id!=_obj_partition_id)
                    {
                        _six_tuples_ss[_obj_partition_id] << _sub   << '\t' << _pre << '\t' << _obj << " ." << endl;
                    }
                }

                if(!flag)
                {
                    for(int j=1;j<numprocs;j++)
                        _six_tuples_ss[j-1] << _sub   << '\t' << _pre << '\t' << _obj << " ." << endl;
                }
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
        _fin.close();
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