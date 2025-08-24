

import json
import re
import time
from openai import OpenAI

"""conda environment: rebuttal"""

# 配置OpenAI客户端
gpt_model = "gpt-4o-mini"
OPENAI_API_KEY = ""  # 替换为你的API密钥
client = OpenAI(api_key=OPENAI_API_KEY, base_url="https://api.chatanywhere.tech/v1")

# 大模型请求函数（复用）
def gpt_chat(content, max_retries=3):
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = client.chat.completions.create(
                model=gpt_model,
                temperature=0.2,
                messages=[{"role": "user", "content": content}]
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"API请求失败 (尝试 {retry_count + 1}/{max_retries}): {e}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(5)
    print("已达到最大重试次数，请求失败。")
    return None


def process_secondary(input_file, output_file, start_idx, end_idx):
    # 最新任务定义（基于修改后内容）
    prompt_template = """
        **Task:** Evaluate if the given question is correctly classified into the task category based on the task definitions. If correctly, only ouput the corresponding category number (1/2/3/4). If not, reclassify it into the correct task category and only output the final category number (1/2/3/4).  

        **Task Definitions:**  
        1. Anomaly detection: The question must contain at least one of the following keywords: "normal", "abnormal", "anomalous", "anomaly", "anomalies", "usual", "unusual", or "expected", and is a true/false task that explicitly asks whether the time series data is normal, abnormal, or usual.
        2. Scenario attribution: The question involves scenario attribution or future scenario prediction, and must explicitly require choosing from several provided options (a multiple-choice task). Questions that involve scenario attribution or prediction but do not provide options are excluded.
        3. Inferential calculation:  The question must contain the phrase "how many" (including but not limited to "how many occasions", "how many times", "how many days", "how many events", "how many instances"), and its core task is to count the number of "events, occasions, data-related phenomena or points conforming to the relevant definitions" in the time series.
        4. Others: General questions (e.g., feature extraction, trend analysis) that do not fit the above categories.  

        **Original Classification:** {original_category}  
        **Question:** {question}  
        
        **Guidelines:**  
        - Absolute Priority Rule for Counting Tasks: If the question contains "how many" and focuses on counting, it must be Category 3 (Inferential calculation), even if it involves anomaly detection (e.g., "abnormal") or scenario attribution (e.g., scenario prediction/attribution) content, or has an original classification of 1/2.
        - If uncertain, default to Category 4. 
        - Only output the category number based on the Output format. 

        **Requirements:**  
        1. First, determine if the question fits the original task category based on the task definitions.  
        2. If yes, only ouput the original category number based on the Output format..
        3. If not, reclassify it into the correct category and output the final category number based on the Output format.   

        **Output format:**  
        - Final Category: [1/2/3/4]  
    """
    
    # 映射任务名称到原始类别编号（1/2/3）
    task_to_category = {
        "Anomaly detection": 1,
        "Scenario attribution": 2,
        "Inferential calculation": 3,
        "Others": 4
    }
    
    cnt = 0
    # 打开输出文件（_2round）
    with open(output_file, 'a') as f_sec:
        
        with open(input_file, 'r') as f_in:
            for idx, line in enumerate(f_in):
                if idx < start_idx:
                    continue
                if idx > end_idx:
                    break
                
                try:
                    data = json.loads(line.strip())
                    question = data["question"]
                    original_task = data["task"]

                    # 获取原始类别编号（1/2/3）
                    if original_task not in task_to_category:
                        print(f"ID {id}: 原始任务类型无效 - {original_task}，跳过")
                        continue
                    original_category = task_to_category[original_task]

                    # 构建二次筛选prompt
                    prompt = prompt_template.format(
                        original_category=original_task,
                        question=question
                    )
                    response = gpt_chat(prompt)

                    if response is None:
                        print(f"ID {id}: API调用失败")
                        continue

                    # 提取最终分类结果
                    match = re.search(r'Final Category:\s*(\d)', response)
                    if not match:
                        print(f"ID {id}: 未找到最终分类结果 - {response}")
                        continue
                    final_category = int(match.group(1))
                    
                    # 计数二次筛选掉的样本
                    if final_category != original_category:
                        cnt += 1

                    # print("question:", question)
                    # 仅保留1/2/3类
                    if final_category not in [1, 2, 3]:
                        print(f"ID {id}: 最终分类为{final_category}(其他)，跳过")
                        continue

                    # 更新任务类型
                    task_map = {
                        1: "Anomaly detection", 
                        2: "Scenario attribution", 
                        3: "Inferential calculation",
                        4: "Others"
                    }
                    
                    data["task"] = task_map[final_category]

                    # 写入输出文件
                    f_sec.write(json.dumps(data) + '\n')
                    print(f"ID {id}: 二次分类为{final_category}，已写入./univariate_2round.jsonl")

                except json.JSONDecodeError:
                    print(f"ID {id}: JSON解析错误")
                except KeyError as e:
                    print(f"ID {id}: 缺少必要字段 - {e}")
                except Exception as e:
                    print(f"ID {id}: 处理错误 - {e}")

                time.sleep(1)
                
    print(f"数据二次筛选完成，共修改 {cnt} 条记录。")

if __name__ == "__main__":
    # 二次/多次筛选代码同时适用于单变量和多变量
    # 修改输入文件：一次筛选的结果
    input_path = "./multivariate.jsonl"
    output_path = "./multivariate_0_2000_filtered_2round.jsonl"
    
    start_index = 0  # 起始索引(包含)
    end_index = 250  # 结束索引(包含)
    
    # 清空输出文件
    open(output_path, 'w').close()
    
    process_secondary(input_path, output_path, start_index, end_index)
    print(f"二次筛选完成. 结果已保存到{output_path}")