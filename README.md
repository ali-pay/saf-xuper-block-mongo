# 如果编译发生错误

**1. jason-cn-dev：将该字段改成：ali-pay**

**2. xuperdata：将该字段改成：saf-xuper-block-mongo**

# Mongodb

## Docker-Mongodb安装

拉取镜像

```bash
docker pull mongo:latest
```

启动容器

```bash
docker run -itd --name mongo -p 27017:27017 mongo --auth
```

进入容器

```bash
docker exec -it mongo mongo admin
```

创建账户

```bash
db.createUser({ user:'admin',pwd:'this is mongodb admin password',roles:[ { role:"userAdminAnyDatabase", db:"admin" }, "readWriteAnyDatabase" ]});
```

登录账户

```bash
db.auth("admin", "this is mongodb admin password")
```

选择数据库

```bash
use jy_chain
```

## 区块订阅同步

```bash
#编译
cd xuperdata
go build -mod vendor
#生成xuperdata执行文件

#启动，-restore：清空数据库，不想清空就不要加上
./xuperdata -restore -s 'mongodb://admin:this is mongodb admin password@0.0.0.0:27017'

#选项说明：
-f：订阅的json文件，默认是：json/block.json （订阅所有的出块）
-c：订阅或取消订阅，默认是：subscribe
-id：订阅事件的uuid，默认是：000
-h：节点的ip，默认是：localhost:37101
-s：mongodb数据源，默认是：mongodb://localhost:27017
-b：mongodb数据库，默认是：jy_chain
-port：供钱包发送交易id过来的端口，默认是：8081
-gosize：同步区块时的线程数，默认是：10
-restore：是否清空数据库重新同步，默认是：false
-show：是否显示接收到区块时打印该区块的高度，默认是：false

#钱包发交易来的接口：
get请求： ip:8081/getTxid?txid="123456"
#例如：http://161.117.39.102:8081/getTxid?txid=9ba381ebcc9e9066bcd4c1bbbda887c398248d4986172d60ddbaeeb117beaed3
```


