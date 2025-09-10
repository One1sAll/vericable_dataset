# 时序可验证的多步推理数据集

## 合成数据集
基于ChatTS数据集构建。
数据集构建代码流程如下：
#### 1. ChatTS原始数据集任务分类筛选
- 筛出异常检测、场景归因、推理计算三类难度较高的推理任务。
- 运行完毕后可以选择5%左右的样本进行人工检查。
- `classify_rule_based.py`
    
#### 2. 从output中提取label
- 正则匹配提取label+时序保留4位小数存放在timeseries2。
- 正则匹配已经尽量将各种情况包括在内，但也有一些特殊表述无法匹配，大家核查的时候重点关注Inferential calculation任务的label。另外，**提取失败（匹配失败）字段为空** 或 **计算推理任务label为非数字** 的id会被记录，最后输出，方便人工核查。输出的id是样本的id字段。
- **注意**：该步骤需要人工核查label提取是否成功以及准确性。可以只检查计算推理任务（直接搜索文档中字符串速度会快一些）。
- `extract_label.py`
#### 3. DeepSeek多步推理增强
- 调用DeepSeek接口，生成multi-step推理过程（分析任务意图→选关键模式→分析时序→出初步答案→反思验证→总结输出），丰富推理逻辑。
- 推理的过程保存在'cot_deepseekr1'字段里。已更新chatts多变量处理逻辑。
- `cot_deepseekr1.py`
#### 4. 模型输出正确性筛选&stepx_label构建
- 对deepseek的输出的准确性进行判断，同步提取cot_deepseekr1字段中的stepx label。
- 会分别输出推理正确和错误的两个文件，推理错误的文件大家请保留，后续可能会用到。最后会打印一个统计结果，运行完请大家重点检查 **处理错误的样本** 和 提取**stepx label为空**的样本。
- 注意：该步骤不再人工检查step6_label的准确性。只需要检查stepx_label是否为空字符串的情况。
- `cot_correct.py`
   
#### 5. 生成最终cot
- `<think> {cot_deepseekr1}</think><ANSWER>The answer is {step6_label}.</ANSWER>` 
- `generate_cot.py`
#### 6. step2_label补充
分析原始output中的内容，提取step2_label的代码
- `extract_step2label_from_output.py`



## 真实数据集
待补充


## 其他辅助代码文件
`format2jsonl.py`: 人工核查时，以jsonl文件存储的数据集一行一个样本，需要反复横向拖拉，先对其进行格式化，筛选完之后再运行该代码修复还原为jsonl格式。

`classify_cnt.py`: 计数jsonl文件下总样本以及各个任务类别样本数量。
``
