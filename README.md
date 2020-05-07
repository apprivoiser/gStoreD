# gStoreD System
基于gStore开发

1.安装gStoreD，运行make完成编译

2.building
PCP/Subject Hash/Metis: `mpiexec -f host_file_name -n host_number+1 bin/gbuildD db_name rdf_triple_file_name internal_vertices_file_name` 

eg: `mpiexec -f hosts.txt -n 5 bin/gbuildD LUBM10M LUBM10M.nt LUBM100MInternalPoints.txt`

Vertical Partition: `mpiexec -f host_file_name -n host_number+1 bin/gbuildD_VP db_name`

eg: `mpiexec -f hosts.txt -n 5 bin/gbuildD_VP LUBM10M LUBM10M.nt` 

3.query
PCP: `mpiexec -f host_file_name -n host_number+1 bin/gqueryD db_name query_file_name 0 crossingEdges_file_name`

eg: `mpiexec -f hosts.txt -n 5 bin/gqueryD LUBM10M LUBM_Q1.txt 0 LUBM10McrossingEdges.txt`

Subject Hash/Metis: `mpiexec -f host_file_name -n host_number+1 bin/gqueryD db_name query_file_name 1`

eg: `mpiexec -f hosts.txt -n 5 bin/gqueryD LUBM10M LUBM_Q1.txt 1`

Vertical Partition: `mpiexec -f host_file_name -n host_number+1 bin/gqueryD_VP db_name query_file_name VPEdges_file_name`

eg: `mpiexec -f hosts.txt -n 5 bin/gqueryD_VP LUBM10M LUBM_Q1.txt VPEdges/LUBM10M_VPEdges.txt`

查询结果保存在ans.txt文件中
