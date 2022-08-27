--创建命名空间
create_namespace 'bigdata'

--查看所有的命名空间
list_namespace

--创建表格
create 'student','info'

create 'bigdata:person',{NAME => 'info', VERSIONS => 5}

--查看一个表的详情
describe 'student'

--修改表
--增加列族和修改信息都使用覆盖方法

--修改版本号
alter 'studnet', NAME => 'info', VERSIONS => '2'

--删除表:先disable，再drop
disable 'student'
drop 'student'


--DML
--写入数据
put 'bigdata:student','1001','info:name','zhangsan'
put 'bigdata:student','1001','info:name','lisi'
put 'bigdata:student','1002','info:age','26'

--读取数据
get 'bigdata:student','1001'
get 'bigdata:student','1001',{COLEMN =>'info:name'}

--读取多行数据
scan 'bigdata:student'
scan 'bigdata:student'{STARTROW => '1001', STOPROW => '1005'}  --扫描多行数据，左闭右开"[)"


















//使用命名空间
use 空间名
use 'bigdata'

// 创建表
create 'bigdata:student', 'info'

//插入数据到表
//put '表名','rowkey','列族:列','值'
put 'student','1001','info:sex','male'
put 'student','1001','info:age','18'
put 'student','1002','info:name','jan'
put 'student','1002','info:sex','female'
put 'student','1002','info:age','20'


//删除表
//先让表为disable状态，再删表
disable 'student'
drop 'student'

//删除某一条rowkey
delete 'student','1001'

//删除某一列
delete 'student','1001','info:sex'

//查看表结构
describe 'student'

//变更表信息
alter 'student',{NAME=>'info',VERSIONS=>3}

//清空表数据
truncate 'student'

//扫描查看表数据
scan 'student'

//查看"指定行"或"指定列族:列"的数据
get 'student','1001'   //单行查询，结果会是rowkey为1001的所有数据，
get 'student','1001','info:name'

//统计表行数
count 'student'

//更新指定字段的数据
put 'student','1001','info:name','Nice'



















































