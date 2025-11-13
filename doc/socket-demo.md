## mac启动netcat
### 1.下载
brew install netcat

### 2.mac启动
nc -l -p 9000

### 3.socket本地调试
不支持从ck，sp启动，查资料了解是因为flink ck，sp支持条件为源端需要为有状态数据源（能够记录读取到的位置）
