import json
import re
import time
from openai import OpenAI

"""conda envvironment: rebuttal"""

def classify_ts_task(input_text, output_text) -> int:
    """
    根据输入文本对时序任务进行分类
    任务定义: 
    1. Anomaly detection(类别1): 含指定异常相关关键词，且是判断时序/异常的True/False任务
    2. Scenario attribution(类别2): 含"choose from"，涉及场景归因或未来预测的多选题任务
    3. Inferential calculation(类别3): 含"how many"相关表述，核心是计数时序中的事件/现象
    4. Others(类别4): 不满足上述三类的其他任务
    返回: 任务类别编号(1/2/3/4)
    """
    # 不区分大小写
    input_lower = input_text.lower()
    output_lower = output_text.lower()
    
    # 1. Scenario attribution
    if re.search(r'\bchoose\b\s+\bfrom\b', input_lower):
        return 2
    
    # 2. Inferential calculation
    # 匹配"how many"及常见扩展形式(如how many occasions/times/days等)
    if re.search(r'\bhow\b\s+\bmany\b', input_lower):
        return 3
    
    # 3. Anomaly detection: 含异常相关关键词 + 是/否判断逻辑
    anomaly_keywords = {
        "normal", 
        "abnormal", "anomalous", "anomaly", "anomalies", 
        "usual", "unusual", 
        "expected", "unexpected",
        "extreme",
    }
    # 检查是否包含至少一个异常关键词
    has_anomaly_keyword = any(
        re.search(r'\b' + re.escape(keyword) + r'\b', input_lower) 
        for keyword in anomaly_keywords
    )
    # 检查output
    output_contains_yes_no = re.search(r'\byes\b', output_lower) or re.search(r'\bno\b', output_lower)
    
    if has_anomaly_keyword and output_contains_yes_no:
        return 1
    
    # 4. Others(类别4)
    return 4


def process_data(input_file, univariate_out_file, multivariate_out_file, start_idx, end_idx):
    open(univariate_out_file, 'w').close()
    open(multivariate_out_file, 'w').close()
    
    with open(univariate_out_file, 'a') as f_uni, open(multivariate_out_file, 'a') as f_multi:
        with open(input_file, 'r') as f_in:
            for idx, line in enumerate(f_in):
                if idx < start_idx:
                    continue
                if idx > end_idx:
                    break
                    
                try:
                    data = json.loads(line.strip())
                    input_text = data.get("input", "")
                    output_text = data.get("output", "")
                    
                    # rule based分类
                    category = classify_ts_task(input_text, output_text)
                                       
                    if category == 4:
                        print(f"ID {idx}: 分类为4(其他)，跳过")
                        continue
                    
                    # 统计<ts>标签数量
                    ts_count = len(re.findall(r'<ts><ts/>', input_text))
                    
                    # 确定任务类型
                    task_map = {
                        1: "Anomaly detection",
                        2: "Scenario attribution",
                        3: "Inferential calculation"
                    }
                    
                    # 构建输出对象
                    output_data = {
                        "id": idx,
                        "task": task_map[category],
                        "question": input_text,
                        "output": data["output"],
                        "label": "",
                        "timeseries": data["timeseries"]
                    }
                     
                    if ts_count == 1:
                        f_uni.write(json.dumps(output_data) + '\n')
                        print(f"ID {idx}: 写入univariate.json (分类: {category})")
                    elif ts_count >= 2:
                        f_multi.write(json.dumps(output_data) + '\n')
                        print(f"ID {idx}: 写入multivariate.json (分类: {category}, TS数量: {ts_count})")
                    else:
                        print(f"ID {idx}: 未找到<ts>标签")
                        
                except json.JSONDecodeError:
                    print(f"ID {idx}: JSON解析错误")
                except KeyError as e:
                    print(f"ID {idx}: 缺少必要字段 - {e}")
                except Exception as e:
                    print(f"ID {idx}: 处理错误 - {e}")
    

if __name__ == "__main__":
    input_file = "./sft/chatts_sft_train.jsonl"   #"./chatts_sft_train.jsonl"
    start_index = 0  # 起始索引(包含)
    end_index = 50000    # 结束索引(包含)
    
    univariate_out_file = 'univariate_rule_based.jsonl'
    multivariate_out_file = 'multivariate_rule_based.jsonl'

    process_data(input_file, univariate_out_file, multivariate_out_file, start_index, end_index)
    print(f"处理完成.结果已保存到{univariate_out_file}和{multivariate_out_file}")

