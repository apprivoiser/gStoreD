for ((i=1;i<=7;i++))
do
    mpiexec -f hosts.txt -n 5 bin/gqueryD_VP LinkedBrainz_VP /opt/workspace/RDFData/query/Q$i.txt VPEdges/LinkedBrainz_VPEdges.txt
done

for ((i=1;i<=7;i++))
do
    mpiexec -f hosts.txt -n 5 bin/gqueryD_VP YAGO_VP /opt/workspace/RDFData/yagoquery/$i.q VPEdges/YAGO_VPEdges.txt
done

for ((i=1;i<=7;i++))
do
    mpiexec -f hosts.txt -n 5 bin/gqueryD_VP LUBM100M_VP /opt/workspace/RDFData/LUBMquery/LUBM_Q$i.txt VPEdges/LUBM100M_VPEdges.txt
done
