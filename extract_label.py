import re
import json
from typing import List, Dict
from word2number import w2n 


def extract_anomaly_label(output: str) -> str | None:
    """异常检测：提取输出中的 Yes/No（优先）或 Normal/Abnormal（次要）"""
    
    # 处理非字符串输入、去除多余空格/换行
    output_clean = output.strip().replace("\n", " ") if isinstance(output, str) else ""
    if not output_clean:
        return None
    
    yes_no_pattern = r"(?i)\b(Yes|No)\b"
    normal_abnormal_pattern = r"(?i)\b(Normal|Abnormal)\b"

    # 优先提取 Yes/No
    yes_no_match = re.search(yes_no_pattern, output_clean)
    if yes_no_match:
        # 首字母大写格式
        return yes_no_match.group().strip().capitalize()

    # 次要提取 Normal/Abnormal
    na_match = re.search(normal_abnormal_pattern, output_clean)
    if na_match:
        # 首字母大写格式
        return na_match.group().strip().capitalize()

    return None


def extract_scenario_label(output: str) -> str | None:
    """场景归因：提取输出中的第一句话"""
    output_clean = output.strip().replace("\n", " ").replace("\r", " ") if isinstance(output, str) else ""
    if not output_clean:
        return None
    
    first_sentence_pattern = r"^.*?[.!?]"
    sentence_match = re.search(first_sentence_pattern, output_clean)
    
    if sentence_match:
        first_sentence = sentence_match.group().rstrip(".!?").strip()
        return first_sentence if first_sentence else None
    else:
        # 无句末标点时，返回整个非空内容作为第一句话
        return output_clean.strip() if output_clean.strip() else None


def extract_inferential_label(output: str) -> str | None:
    """推理计算：提取计数结果（兼容中英文数字）"""
    output_clean = output.strip().replace("\n", " ") if isinstance(output, str) else ""
    if not output_clean:
        return None
    # 覆盖常见计数句式的正则模式
    infer_patterns = [
        r"I've found that there are (\w+|\d+)",
        r"I've found that there is (\w+|\d+)",
        r"I've found that there were (\w+|\d+)",
        r"I've found that there was (\w+|\d+)",
        r"I've found (\w+|\d+)",
        r"I've identified (\w+|\d+)",
        r"there is (\w+|\d+)",
        r"there are (?:approximately|about|roughly) (\w+|\d+)",
        r"there was (\w+|\d+)",
        r"there were (?:approximately|about|roughly) (\w+|\d+)",
        r"it was observed that (\w+|\d+)",
        r"the number of .*? is (\w+|\d+)",
        r"it took (\w+|\d+)",
        r"It took (\w+|\d+)",
        r"(\w+|\d+) \w+(?: \w+)* can be identified"
        
        # 优先级靠后
        r"occurred (\w+|\d+)"
        r"the time series shows (\w+|\d+)",
        r"(\w+|\d+) times\b",
        r"(\w+|\d+) day\b",
        r"(\w+|\d+) days\b",
        r"(\w+|\d+) minute\b",
        r"(\w+|\d+) minutes\b",
        r"(\w+|\d+) hour\b",
        r"(\w+|\d+) hours\b",
        r"(\w+|\d+) second\b",
        r"(\w+|\d+) seconds\b",
        r"(\w+|\d+) point\b",
        r"(\w+|\d+) points\b",
        r"on (\w+|\d+)", 

    ]

    special_map = {
        "no": "0",
        "zero": "0",
        "none": "0",
        "a": "1",
        "an": "1",
        "once": "1",
        "twice": "2"
    }
    # 遍历匹配
    for pattern in infer_patterns:
        match = re.search(pattern, output_clean, re.IGNORECASE)
        if match:
            num_raw = match.group(1).strip()
            
            # 数字直接返回
            if num_raw.isdigit():
                return num_raw
            
            if num_raw in special_map:
                return special_map[num_raw]

            try:                
                num_digit = w2n.word_to_num(num_raw)
                return str(num_digit)  # 转为字符串，保持输出格式统一
            # 容错：若英文数字格式不规范（如"onehundred"连写），返回原始值
            except ValueError:
                return num_raw
            
    return None

# 自定义异常：用于标识timeseries中的非数值类型错误
class NonNumericValueError(Exception):
    pass
# 提取标签为空的异常
class EmptyLabelError(Exception):
    pass

def round_timeseries_values(timeseries):
    """递归处理timeseries列表，将所有数值保留4位小数"""
    processed = []
    for item in timeseries:
        if isinstance(item, list):
            processed.append(round_timeseries_values(item))  # 递归处理嵌套列表
        elif isinstance(item, (int, float)):
            processed.append(round(item, 4))  # 保留4位小数
        else:
            # 非数值类型主动报错
            raise NonNumericValueError(
                f"timeseries中存在非数值类型数据: {item}(类型: {type(item).__name__})"
            )
    return processed

def process_jsonl_label(input_file: str, output_file: str, start_idx: int, end_idx: int) -> None:
    # 任务类型到提取函数的映射
    task_to_extractor = {
        "Anomaly detection": extract_anomaly_label,
        "Scenario attribution": extract_scenario_label,
        "Inferential calculation": extract_inferential_label
    }
    
    wrong_id = [] # 记录处理失败的ID，人工核查重点
    with open(output_file, 'a', encoding="utf-8") as f_out:
        with open(input_file, 'r', encoding="utf-8") as f_in:
            for idx, line in enumerate(f_in):
                if idx < start_idx:
                    continue
                if idx > end_idx:
                    break
                
                # 过滤空行
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line.strip())
                    
                    id = data["id"]
                    task = data["task"].strip()
                    output = data["output"]
                    timeseries = data["timeseries"]
                    
                    if task not in task_to_extractor:
                        print(f"ID {id}: 未知任务类型 {task}，跳过")
                        continue
                    
                    # 调用对应的提取函数 
                    extractor = task_to_extractor[task]
                    label = extractor(output)
                    if label == None:
                        print(f"ID {id}: 任务 '{task}' 提取到空字符串标签")
                        wrong_id.append(id)
                    if task == "Inferential calculation" and not str(label).isdigit():
                        print(f"ID {id}: 任务 {task}，提取标签为非数字: {data['label']}")
                        wrong_id.append(id)
                    
                    # 处理timeseries，每个数值只保留4位小数
                    data["timeseries2"] = round_timeseries_values(timeseries)
                    
                    data["label"] = label if label is not None else ""
                    # 写入输出文件
                    f_out.write(json.dumps(data, ensure_ascii=False) + '\n')
                    print(f"ID {id}: 任务 {task}，提取标签: {data['label']}")
                
                except json.JSONDecodeError as e:
                    print(f"ID {id}: JSON解析错误 {str(e)}，跳过")
                    wrong_id.append(id)
                except Exception as e:
                    print(f"ID {id}: 处理错误 {str(e)}，跳过")
                    wrong_id.append(id)
                
            print(f"失败 {len(wrong_id)} 条. 失败样本ID: {wrong_id}")
            print("已将timeseries中的数值最多保留4位小数，保留在timeseries2字段中")
    


if __name__ == "__main__":

    input_path = "./univariate_classified_2001_6000.jsonl"
    output_path = "./univariate_classified_2001_6000_testttt.jsonl"
    
    start_index = 0  # 起始索引(包含)
    end_index = 1300  # 结束索引(包含)
    
    # 清空输出文件
    open(output_path, 'w').close()

    # 执行批量处理
    process_jsonl_label(input_path,output_path,start_index,end_index)
    print(f"处理完成. 结果已保存到 {output_path}")