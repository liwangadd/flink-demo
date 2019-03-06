### flink自定义trigger
该仓库用于示范在Flink中如何自定义trigger实现窗口的触发逻辑，工程中由三个java文件构成
- CountWindowDemo.java 工程的入口文件，定义了flink计算的算子和数据流向
- IntegerData.java 数据文件，如果没有通过命令行参数出入原始数据文件的路径，则默认使用该文件中的数据进行计算演示
- MyCountTrigger.java 主要的触发逻辑，当窗口内的元素个数大于maxCount或者窗口内元素和大于maxSum时触发计算

### clone运行时的tip
为了便于调试，在代码中将并行度设置为1，这样每次运行的结果都相同。如果想测试并行效果，可以通过`setParallelism(int parallelism)`函数自定义并行度
