# hbase-coprocessor-demo
the demo for hbase coprocessor


## build
mvn clean package


## use

### add coprocessor in hbase 
alter 'coprocessor_test', METHOD => 'table_att', 'Coprocessor'=>'hdfs://localhost:9000/jar/coprocessor-1.0.0.jar|com.poul.hbase.HbaseDataWriteObserver|1073741823|copEnv=dev,tableName=coprocessor_test,namespace=default'

### drop coprocessor in hbase
alter 'coprocessor_test', METHOD => 'table_att_unset', NAME => 'coprocessor$1'

