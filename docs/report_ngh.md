# <center>课程设计2 -哈利波特的魔法世界

## 小组成员及分工

①张乃仁 201870048【组长】
②钟亚晨 201220096 
③聂冠华 201220147 
④陈毅琦201220167

各成员完成的任务：


## 运行方法

首先要把`confs`文件夹上传,之后的相关配置都是在相关yaml文件中配置。

运行task1

`MPLab-1.jar potternet.GetNameSeq.GetNameSeqMain /home/2023stu_22/confs/task1_conf.yaml`

运行task2

`MPLab-1.jar potternet.CountCooccurrence.CountCooccurMain /home/2023stu_22/confs/task2_conf.yaml`

运行task3

`MPLab-1.jar potternet.BuildCharacterMap.BuildMapMain /home/2023stu_22/confs/task3_conf.yaml`

运行task4

`MPLab-1.jar potternet.ComputePageRank.PageRankMain /home/2023stu_22/confs/task4_conf.yaml`

运行task5

`MPLab-1.jar potternet.SyncLabelPropagation.SyncLPAMain /home/2023stu_22/confs/task5_conf.yaml`


task1-5全部执行的命令

`MPLab-1.jar potternet.UpperModule.UpperModuleMain /home/2023stu_22/confs/task_conf.yaml`
## 设计

### Task1
输入：小说文集和小说中出现的人名列表，输出：保留人名

#### Driver部分
使用addCacheFile函数把人名列表文件放到每个map的本地缓存中
#### Mapper部分
输入键值对`<LongWritable, Text>` 输出键值对`<Text, NullWritable>`

输入键值对的含义为`<当前行的偏移量，行的内容>`

输出键值对的含义为`<name list,null>`，name list是当前行中的人名部分，name list是一个字符串，它的格式是`"<name1>,<name2>,<name3>..."`

人名提取实现：
1. 在mapper类的setup函数创建一个字典，这个字典首先硬编码地加入若干项，用与将人名统一化处理，即把key统一化为value。然后再读入`person_name_list.txt`文件，对于其中的每个人名name：
   * 如果这个name中含有空格，首先向字典中加入{name,name}项，然后用空格进行分词，对得到的每个子串token，如果字典中现在没有key为token，就向字典中加入{token,name}。
   * 否则，如果将name加入`Set<String> characters`中
  
    最后，对于`characters`中的每个name，如果name不在字典的key中，向字典中加入{name,name}
2. 在map函数中首先将每行中的非英文字母，非连字符全部替换为空格。再用空格进行分词得到一个字符串数组。
3. 然后就是使用滑动窗口提取人名（因为人名列表中人名最大长度为3，所以设置华东窗口最大长度也是3即可）：
    ```
        // nameDict为setup函数中构造出的字典
        List<String> item
        left = 0        
        right = min(2, words.length - 1);
        while left < words.length :
            i = right
            for i =right to left :
                str = words[left:i] //闭区间  
                if str in nameDict.key:
                    item.add(nameDict[str]);
                    break
            
            if i < left:
                left++
            else :
                left = i+1
            
            right = min(2, words.length - 1);


        emit(string(item),null)

    ```

#### reduce部分
输入键值对`Text, NullWritable>` 输出键值对`<Text, NullWritable>>`

reduce啥也不用干，直接原样输出即可

### Task2

输入：Task1的输出

输出：人物之间的同现次数

#### Driver部分
常规检查参数，设置相关类等

#### Mapper部分
输入键值对`<LongWritable, Text>`

输出键值对`<Text, IntWritable>`

输入键值对的含义为`<当前行的偏移量，行的内容>`，此处行的内容对应Task1的输出，每行的格式为`"<name1>,<name2>,<name3>..."`

输出键值对的含义为`<"name1,name2" , num>`，表示在当前行name1和name2的同现次数

算法较为简单，此处不再赘述

#### Combiner部分
为了减少通讯开销，重写Combiner来合并每个map节点key相同的键值对。

#### Reducer部分

输入键值对`<Text, IntWritable>`

输出键值对`<Text, IntWritable>`

输入键值对的含义为`<"name1,name2" , [num1,num2……]>`，即Mapper的输出
输出键值对的含义为`<"name1,name2" , num2>`,表示name1和name2同现了num次

### Task3

输入：Task2的输出

输出：归一化权重后的任务关系图（邻接链表形式）

#### Mapper部分
输入键值对`<LongWritable, Text>`

输入键值对的含义为`<当前行的偏移量，行的内容>`，此处行的内容对应Task1的输出，每行的格式为`"name1,name2  name1和name2在文本中的同现次数"`

输出键值对`<Text, Text>`

输出键值对的`<name1 , string(name2 + "#" + name1和name2在文本中的同现次数">`

#### Reducer部分

输入键值对`<Text, Text>`

输入键值对的含义为`< <out_node> , [ <in_node1>#<probability1>,<in_node2>#<probability2>... ] >`。对应Mapper的输出

输出键值对`<Text, Text>`

输出键值对的含义为`< out_node , <in_node1>#<in_node1的归一化权重>|<in_node2>#<in_node2的归一化权重>··· >`

算法实现也较为简单，因为对对于每个输入键值对，key一定是唯一出现的。那么只需要用这一个键值对即可算出out_node各个出边对应的端点以及各边的归一化权重。

算法伪代码
```
    ArrayList<Pair<String, Double>> recorder = new ArrayList<>();
    sum_frequency = 0;
    for  val : values 
        对val进行分词得到in_node和其对应的frequency
        sum_frequency += frequency
        recorder.add(<in_node,frequency>)

    string result

    for <in_node, frequency> : recorder
        probability = frequency / sum_frequency;

        if result == "":
            result = in_node + string(probability)
        else :
            result += "|" + in_node + string(probability)
    
    emit(key,result)

```


### Task4

输入：Task3的输出

输出：每个节点即其对应的的pagerank值


#### Driver部分
分了三步完成：
1. 初始化每个节点的pangerank值为1
2. 迭代计算每个节点的pangrank值
3. 按照指定格式输出节点
   
这3步需要不同的mapper和reducer

```
    设置启动第一步的map和reduce

    // 下面是第二步的

    itrMaxNum    // 最大迭代轮数，在task4_conf.yaml文件中配置
    itrDoneNum=0 //当前迭代轮数
    while itrDoneNum < itrMaxNum:
        if itrDoneNum == 0 : //第一次迭代
            输入设置为上一步reduce的输出
        else :
            输入设置为上一轮迭代的reduce的输出

        设置启动第二步的map和reduce

        if itrDoneNum > 1 :
            if 本轮迭代结果与上轮迭代结果相同 ：
                break

        itrDoneNum += 1


    设置启动第三步的map和reduce，得到task4的结果
        
```
这里还要求出人物的总数，用allItemNum来记录


下面是三步对应的mapreduce详解

#### 第一步
初始化每个节点的pangerank值

##### Mapper

输入键值对`<LongWritable, Text>`，含义为`<当前行的偏移量，行的内容>`，这来自task3的输出，其中行的内容为`"out_node   <in_node1>#<in_node1的归一化权重>|<in_node2>#<in_node2的归一化权重>··· "`

输出键值为`<Text, Text>`,含义为`<out_node , 0.3#<in_node1>#<in_node1的归一化权重>|<in_node2>#<in_node2的归一化权重>···>`。这里0.3为我们设定的每个节点的初始pagerank值。

##### Reducer
啥也不敢，就是按照Mapper输出的那样输出。

#### 第二步

##### Mapper

输入键值对为`<LongWritable, Text>`,其含义为`<当前行的偏移量，行的内容>`，其中行的内容为`<out_node , <out_node节点当前pagerank值>#<in_node1>#<in_node1的归一化权重>|<in_node2>#<in_node2的归一化权重>···>`.

为了发个遍讨论，这里用link_list表示`{<in_node1>#<in_node1的归一化权重>, ···}`

输出键值对为`<Text, Text>`，有两种：
    1. `<URL, link_list>`,URL为当前人物名称
    2. For each u in link_list, 输出 `<u, cur_pagerank/link_list.len>`

#### Reducer

输入键值对为Mappeer的输出，即那两类键值对

输出键值对为`<Text, Text>`,其含义为`< URL , <URL对应的pagerank>#in_node1>#<in_node1的归一化权重>|<in_node2>#<in_node2的归一化权重>··· >`

在setup函数中先拿到总人物数allItemNum和dampingFactor，dampingFactor可在task4_conf.yaml文件中配置。

然后在reduce函数中的处理如下：

迭代计算公式：PR(A) = (1-d) /N+ d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))

```
    void reduce(key,values)
        newPRValue = (1.0 - dampingFactor) / allItemNum;
        sum = 0.0;
        for  val : values
            if val 是 link_list :
                nodeStruct.ppend(val)
            else :
                sum += val

        newPRValue += dampingFactor * sum

        emit(key, string(newPRValue + "#" + nodeStruct))

```


#### 第三步

读入第二步最后一轮迭代的输出，然后最后输出的文件每行的格式为`name  pagerank值`。较为简单，此处不再赘述。


### Task5

输入：Task3的输出

输出：每个节点即其对应的的pagerank值

需要用到4种不同的mapreduce


#### Driver部分
```
    设置启动第一种的map和reduce


    itrMaxNum    // 最大迭代轮数，在task5_conf.yaml文件中配置
    itrDoneNum=0 //当前迭代轮数
    while itrDoneNum < itrMaxNum:
           
        // 先运行第二种mapreduce
        if itrDoneNum == 0 : //第一次迭代
            第二种mapreduce的输入设置为第一种的mapreduce的输出
        else :
            第三种mapreduce的输入设置为上一轮迭代的第三种的mapreduce的输出

        设置启动第二步的map和reduce

        // 再运行第三种mapreduce
        if itrDoneNum == 0 : //第一次迭代
            第三种mapreduce的输入设置为第一种的mapreduce的输出
        else :
            第三种mapreduce的输入设置为上一轮迭代的第三种的mapreduce的输出

        调用addcachefile将本轮第二种mapreduce的输出文件添加到map节点的缓存
        设置启动第三种mapreduce
        
        
        itrDoneNum += 1


    设置启动第4种的map和reduce，得到task5的结果
        
```
#### 第一种
SyncLPABuildMapAsCacheMapper和SyncLPABuildMapAsCacheReducer

为每个节点打上初始标签，初始标签为自己。
生成结果文件的每行的格式为

` out_node  <initLabel>#<in_node1,frequency1>|<in_node2,frequency2>...  `

这一步与pagerank的第一步高度相似，此处不再赘述

#### 第二种
SyncLPABuildMapAsCacheMapper和SyncLPABuildMapAsCacheReducer

以第i轮迭代为例：
根据上一轮迭代生成的iteri-1文件或者task3的结果（第0次迭代时用这个）。
输出一个文件cachei，每行的格式为`节点   节点对应的标签`

这个文件给出了当前每个人物对应的标签。


#### 第三种
SyncLPAIterMapper，SyncLPAIterCombiner和SyncLPAIterReducer

##### SyncLPAIterMapper
在setup函数中先处理cachefile，cachefile中存放的是当前每个人物及其标签，由第二种mapreduce生成。

解析cache文件，将所有的`<人物，标签>`加入到字典`nameKeyMap`中。

同时设置好`outEdgeParamAlpha`和`inEdgeParamBeta`

然后是map函数

map的输入键值对是`<LongWritable, Text>` ,输出键值对是`<Text, Text> `

输入文件是上一轮迭代输出的iter文件或者第一种mapreduce生成的初始文件。

输入键值对的含义为`<当前行偏移，行的内容>`。

其中行的内容为` out_node  <Label>#<in_node1,frequency1>|<in_node2,frequency2>...  `

输出键值有两种：
    1. `<out_node , <in_node1,frequency1>|<in_node2,frequency2>... >`
    2. `<node , (label , val)>`

代码解析：
```
    for each <in_node,frequency> :
        emit(in_node , (nameKeyMap[out_node], inEdgeParamBeta* frequency) )
        emit(outNodeName , (nameKeyMap[in_node],outEdgeParamAlpha*frequency) )

    emit(out_node , <initLabel>#<in_node1,frequency1>|<in_node2,frequency2>...)

```

##### SyncLPAIterReducer

Reducer的输入键值对是`<Text, Text>` ,输出键值对是`<Text, Text> `

输入键值值对实际上有两种
1. `<node , (label, val) >`
2. `<node , node_list>`

代码解析：

```
    //输入为<key , Iterable<Text> values>

    Map<String, Double> labelVoteMap = new HashMap<>();
    for val : values
        if val 是 node_list ：
            structKeeper = node_list
        else ： // <node , (label, val) >
            if label in labelVoteMap.key :
                labelVoteMap[label] + = val
            else :
                labelVoteMap[label]  = val
    
    遍历labelVoteMap找到得票最多的res_label

    emit(key , string(res_label+'#'+structKeeper))

```

#### 第四种
SyncLPAViewerMapper和SyncLPAViewerReducer

这一步就是从最后一轮迭代结果中根据label的不同进行社区化分类

注意到最后一轮迭代生成的输出文件每行的格式是：

` out_node  <Label>#<in_node1,frequency1>|<in_node2,frequency2>...  `

map要做的就是针对这么每行发射`<label , out_node>`

reduce要做的就是针对每个label，将标签为label的节点输出到一行中

最后的输出文件每行代表一个社区


## 实验运行结果

## 拟解决的问题

标签传播的实现使用同步方式，可以尝试异步方式实现
pagerank初始pagerank值设置问题
pagerank迭代轮数控制
目前集群测试无法正常运行，疑似为任务调度问题