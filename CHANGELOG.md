# Todo😫

------

## 项目层面

- [ ] Task0：相同名称统一？（Potter到底对应于Harray Potter还是Lily Potter？）[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] Task4：可拓展的迭代停止条件（目前采取指定次数迭代，希望追加诸如“排序不变/结果特征向量不更新”等更多迭代停止条件，以及一个具备拓展性的可选类）[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] Task4：PageRank算法中目前使用的初始值为全1，是否有更为合理的初值设定方法？[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] Task4：针对公式$R(u)=\frac{1-d}{N}+d\sum\limits_{v\in B_u}\frac{R(v)}{N_v}$中的页面/人名总数$N$，目前所采用的是单机/线程/进程扫描文件行数，这在大数据条件下会导致时间开销过大，尚需使用一个MapReduce进行全局统计 [propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] All：所有任务的串联并行执行 [propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] All：所有任务各自的正确性检验、性能检测 [propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] All：所有任务的性能优化 [propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] All：Task3、Task4的可视化结果演示，及运行时可视化动态展示？（基于小型本地测试集，使用Python读入MapReduce输出文件进行额外实现）[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] All：整个项目的详细文档编写、在线文档部署 [propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] All：整个项目性能优化效果对比演示（通过修改更换细节实现方法 & 真实集群环境下比对运行来展示，需整理完整的日志及截图信息），后续绘制图标进行演示。包括但不限于：[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
    - [ ] Task1：使用长度为3的滑动窗口提取人名 .vs 使用KMP比对，暴力遍历人名列表和语句提取人名；
    - [ ] Task2：人物同现使用本地Combiner聚合 .vs 人物同现未使用本地Combiner聚合；
    - [ ] Task4：参数$N$通过单线程扫描获取 .vs 参数$N$通过MapReduce并行/其它方法获取；


## 课程任务层面

- [ ] Submit：集群服务器上真实环境的运行部署，及所有结果的截图；[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] Submit：最终实验报告撰写（使用Latex）；[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [ ] Submit：最终小组展示时的PPT制作等演示准备工作；[propose @pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))

# CHANGELOG

------

## 2023.06.22

### Done😀

- [x] Task4：PageRank算法分析 + （拓展1）结果排序【已完成本地单机可行性测试√】[@pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))

## 2023.06.21

### Done

- 完成PageRank论文阅读：《The PageRank Citation Ranking: Bringing Order to the Web》[@pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))

## 2023.06.20

### Done😀

- [x] Task1：人物名称提取【已完成本地单机可行性测试√】[@pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [x] Task2：人物同现频率统计【已完成本地单机可行性测试√】[@pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))
- [x] Task3：构建邻接表，并归一化人物同现频率【已完成本地单机可行性测试√】[@pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))

# Contributors

------

- [@pawx2]([pawx2 (pawx2) (github.com)](https://github.com/pawx2))



