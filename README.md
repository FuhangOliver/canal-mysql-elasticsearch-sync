canal同步es数据组件开发
## 开发文档

### 1. 在http://tech.ciwei:8087/cw_java_team下载microservice-api,microservice-common项目
### 2. 复制helloworld项目到D:\work\ALD\将项目中的helloworld（涉及到的文件夹名称和文件名称和代码）全改成您的微服务名称
### 3. 在microservice-api工程生成接口文件
    a.在D:\work\ALD\microservice-api\src\main\thrift目录下编写thrift定义文件
    b.在idea的Project Structure的Facets项加入thrift（详情请Google）
      配置thrift的Java编译OutPut path路径D:/work/ALD/microservice-api/src/main/java
    c.编译thrift文件，自动在OutPut path路径目录下生成对应的Java对象和接口
### 5. 在相应的微服务工程， 比如microservice-helloworld
    a.首先要在数据库里面建立表结构
    b.在generatorConfig.xml按照文件注释修改相应的table表名和 target路径
    c.通过generatorConfig.xml自动生成mapper文件（mapper文件生成后要加@Mapper注解）和对应的*mapper.xml文件
    d.然后再service目录下编写对应的服务接口实现类
### 3. 根据情况修改D:\work\ALD\microservice-您的微服务名称\src\main\resources\application-dev.properties配置文件
### 6. 单元测试在D:\work\ALD\microservice-helloworld\src\test\java\com\ciwei目录下
# canal-mysql-elasticsearch-sync
