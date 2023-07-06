# TodoBroad😫

------

## 项目层面

- [x] Task0：相同名称统一？（Potter到底对应于Harray Potter还是Lily
  Potter？）[propose @pawx2:2023/06/20](https://github.com/pawx2)
- [x] 
  Task4：可拓展的迭代停止条件（目前采取指定次数迭代，这将导致冗余/不足迭代，影响结果可靠性和运行效率，希望追加诸如“排序不变/结果特征向量不更新”等更多迭代停止条件，尤其是结果特征向量不更新的迭代终止条件，以及一个具备拓展性的可选类，增强代码可拓展性）[propose @pawx2:2023/06/22](https://github.com/pawx2)
- [x] 
  Task4：PageRank算法中目前使用的初始值为全1，是否有更为合理的初值设定方法？[propose @pawx2:2023/06/22](https://github.com/pawx2)
- [x] Task4：针对公式$R(u)=\frac{1-d}{N}+d\sum\limits_{v\in B_u}\frac{R(v)
  }{N_v}$中的页面/人名总数$N$，目前所采用的是单机/线程/进程扫描文件行数，这在大数据条件下会导致时间开销过大，尚需使用一个MapReduce进行全局统计 [propose @pawx2:2023/06/22](https://github.com/pawx2)
- [ ] 
  Task5：怎样实现MapReduce实现“异步+随机序”的标签传播更新方式，从而降低二部图or相似二部图子图震荡导致的对结果正确性和运行性能的负面影响？（Hadoop难以实现，或许需考虑Spark实现？）[propose @pawx2:2023/06/23](https://github.com/pawx2)
- [ ] 
  Task5：可拓展的LPA迭代终止条件，尤其是标签无变更下的迭代终止，可减少冗余迭代；[propose @pawx2:2023/06/23](https://github.com/pawx2)
- [ ] Task5：如何在Mapper节点中共享可实时更新缓存？从而使用自增数字变量作为`unique label`
  ，从而增强泛用性？（Task5遇到瓶颈：多节点并行时需要的可实时更新共享缓存难以满足，Cache仅可满足只读共享）[propose @pawx2:2023/06/24](https://github.com/pawx2)
- [ ] All：实现顶层模块和对应配置文件，所有任务的串联并行执行 [propose @pawx2:2023/06/22](https://github.com/pawx2)
- [ ] All：所有任务各自的严格正确性检验、性能检测 [propose @pawx2:2023/06/23](https://github.com/pawx2)
  - [ ] All：各个部分的各阶段，逐步优化下的时间复杂度分析，详细的分析步骤，总体优化效果的定量、图标展现
  - [ ] All：各个模块的正确性测试【Hard】
- [ ] All：所有任务的性能优化 [propose @pawx2:2023/06/22](https://github.com/pawx2)
- [ ] 
  All：Task3、Task4的可视化结果演示，及运行时可视化动态展示？（基于小型本地测试集，使用Python读入MapReduce输出文件进行额外实现）[propose @pawx2:2023/06/22](https://github.com/pawx2)
- [ ] All：整个项目的详细文档编写、在线文档部署 [propose @pawx2:2023/06/22](https://github.com/pawx2)
- [ ] All：整个项目性能优化效果对比演示（通过修改更换细节实现方法 & 真实集群环境下比对运行来展示，需整理完整的日志及截图信息），后续绘制图标进行演示。**包括但不限于**：[propose @pawx2:2023/06/22](https://github.com/pawx2)
    - [ ] Task1：使用长度为3的滑动窗口提取人名 .vs 使用KMP比对，暴力遍历人名列表和语句提取人名；
    - [ ] Task2：人物同现使用本地Combiner聚合 .vs 人物同现未使用本地Combiner聚合；
    - [ ] Task3：构建邻接表时的性能改进etc…；
    - [ ] Task4：参数$N$通过单线程扫描获取 .vs 参数$N$通过MapReduce并行/其它方法获取；
    - [ ] Task5：异步更新方式 .vs 同步更新方式，在性能和结果准确性方面的度量/演示；
    - [ ] Task5：使用Combiner .vs 不使用Combiner，在性能方面的比较演示；
- [ ] All：所有模块调试完全后，关闭所有日志输出，或许可以进一步提升性能？；[propose @pawx2:2023/06/24](https://github.com/pawx2)


## 课程任务层面

- [ ] Submit：集群服务器上真实环境的运行部署，及所有结果的截图和各种杂项材料；[propose @pawx2:2023/06/22](https://github.com/pawx2)
- [ ] Submit：最终小组展示时的PPT制作等演示准备工作；[propose @pawx2:2023/06/22](https://github.com/pawx2)
- [ ] Key Submit：课程设计报告（使用Latex）；[propose 《Tutorial》]()
    - [ ] 小组信息（人员，学号，联系信息，导师及研究领域）；
    - [ ] 课题小组分工：需明确说明各成员在整个课题中分工负责完成的内容；
    - [ ] 课程设计题目；
    - [ ] 摘要；
    - [ ] 研究问题背景；
    - [ ] 主要技术难点和拟解决的问题，尤其要解释说明哪些地方、为什么需要采用MapReduce；
    - [ ] 主要解决方法和设计思路，尤其要解释说明如何采用MapReduce并行化算法解决问题；
    - [ ] 详细设计说明，包括详细算法设计、程序框架、功能模块、主要类的设计说明，包括主要类、函数的输入输出参数、尤其是map和reduce函数的输入输出键值对详细数据格式和含义；
        - [ ] 主要功能和算法代码中加清晰的注释说明；
        - [ ] 对于引用的部分，需要给出参考文献；

    - [ ] 输入文件数据和详细输入数据格式，输出结果文件数据片段和详细输出数据格式**（必须清晰描述）**；
    - [ ] 程序运行实验结果说明和分析；
    - [ ] 总结：特点总结，功能、性能、拓展性等方面存在的不足和可能的改进之处；
    - [ ] 参考文献

- [ ] Key Submit：带注释的源程序（必须提交）；[propose 《Tutorial》]()
- [ ] Key Submit：输入数据文件和运行结果文件（必须提交，数据量过大可取部分数据）；[propose 《Tutorial》]()
- [ ] Key Submit：实时可执行程序；[propose 《Tutorial》]()

# CHANGELOG

------

## 2023.06.25

### Added

- 追加了PageRank（Task4）、LPA（Task5）两篇论文的阅读记录，其中包含了整体设计流程和尚需完善的任务点 [@pawx2](https://github.com/pawx2)

### Changed

- Task5：调整了Viewer输出格式，每行输出一个社区列表，不再输出冗余的每个社区的标签 [@pawx2](https://github.com/pawx2)

## 2023.06.24

### Done

- [x] Task5：LPA (Label Propagation Alghorim) 初步实现，使用同步迭代 + 指定迭代次数的迭代终止条件 [@pawx2](https://github.com/pawx2)
    - 【已完成本地单机可行性测试√】
    - 在较大迭代次数下观察到**二部图/类似二部图子图的标签震荡问题**，等待**异步随机 + 随机序更新 + 无标签更新时迭代终止**实现

## 2023.06.23

### Done😀

- 完成LPA论文阅读：《Near linear time algorithm to detect community structures in large-scale networks》[@pawx2](https://github.com/pawx2)

## 2023.06.22

### Done😀

- [x] Task4：PageRank算法分析 + （拓展1）结果排序【已完成本地单机可行性测试√】[@pawx2](https://github.com/pawx2)

## 2023.06.21

### Done😀

- 完成PageRank论文阅读：《The PageRank Citation Ranking: Bringing Order to the Web》[@pawx2](https://github.com/pawx2)

## 2023.06.20

### Done😀

- [x] Task1：人物名称提取【已完成本地单机可行性测试√】[@pawx2](https://github.com/pawx2)
- [x] Task2：人物同现频率统计【已完成本地单机可行性测试√】[@pawx2](https://github.com/pawx2)
- [x] Task3：构建邻接表，并归一化人物同现频率【已完成本地单机可行性测试√】[@pawx2](https://github.com/pawx2)

# Reference

------

- Task4：PageRank Algorithm
    - [论文原文](http://web.mit.edu/6.033/2004/wwwdocs/papers/page98pagerank.pdf)、[论文带读](https://zhuanlan.zhihu.com/p/120962803)、[Markov Chains](https://www.youtube.com/watch?v=i3AkTO9HLXo&t=1s)/[中译](https://www.bilibili.com/video/BV1xa4y1w7aT/?spm_id_from=333.1007.tianma.2-2-4.click&vd_source=2a11f9f700546028a49b63c0d54f4bda)
- Task5：LabelPropagation Algorithm
    - [论文原文](https://journals.aps.org/pre/abstract/10.1103/PhysRevE.76.036106)、[参考1](https://www.cnblogs.com/LittleHann/p/10699988.html)、[参考2](https://blog.csdn.net/u013385018/article/details/95447955)、[异步更新下收敛思考](https://www.zhihu.com/question/277808560)、[带权有向图处理](https://blog.csdn.net/google19890102/article/details/51558148)、[相关论文1](https://proceedings.neurips.cc/paper_files/paper/2003/file/87682805257e619d49b8e0dfdc14affa-Paper.pdf)

# Contributors

------

- [@pawx2](https://github.com/pawx2)



