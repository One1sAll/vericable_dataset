import json
import re
import time
from openai import OpenAI

"""conda envvironment: rebuttal"""

# 配置OpenAI客户端
gpt_model = "gpt-4o-mini"
OPENAI_API_KEY = ""  # 替换为你的API密钥
client = OpenAI(api_key=OPENAI_API_KEY, base_url="https://api.chatanywhere.tech/v1")

# 大模型请求函数
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


def process_data(input_file, start_idx, end_idx):
    prompt_template = """
        **Task:** Classify the given question into one of these categories:  
        1. Anomaly detection: Anomaly detection: The question must contain at least one of the following keywords: "normal", "abnormal", "anomalous", "anomaly", "anomalies", "usual", "unusual", or "expected", and is a true/false task that explicitly asks whether the time series data is normal, abnormal, or usual.
        2. Scenario attribution: The question involves scenario attribution or future scenario prediction, and must explicitly require choosing from several provided options (a multiple-choice task). Questions that involve scenario attribution or prediction but do not provide options are excluded.
        3. Inferential calculation:  The question must contain the phrase "how many" (including but not limited to "how many occasions", "how many times", "how many days", "how many events", "how many instances"), and its core task is to count the number of "events, occasions, data-related phenomena or points conforming to the relevant definitions" in the time series.
        4. Others: General questions (e.g., feature extraction, trend analysis) that do not fit the above categories.   

        **Examples:**  
        1. **Anomaly detection** (Category 1):  
        - You are a time series analysis expert. This is a metric called Ad Frequency collected from Marketing and Sales with length of 256: <ts><ts/>. If the Ad Frequency data shows a steady trend and no periodic fluctuations, should this behavior be flagged as anomalous in a context where frequent changes are expected?  
        - You are a time series analysis expert. This is a metric called Physical Reads/Writes collected from Oracle Database with length of 256: <ts><ts/>. If the threshold for normal behavior in Physical Reads/Writes is set at -0.5, should the behavior at the minimum value be considered normal? 

        2. **Scenario attribution** (Category 2):  
        - You are a time series analysis expert. This is a metric called Storm Tracking collected from Weather Forecasting with length of 256: <ts><ts/>. According to the time series, what might have happened between time point 150 and 200? Choose from: increased storm activity, stable weather conditions, or system maintenance.
        - You are a time series analysis expert. This is a metric called Lightning Strikes collected from Weather Forecasting with length of 794: <ts><ts/>. Based on the time series for Lightning Strikes, what is likely to occur following this pattern? Choose from: scheduled downtime, network latency issue, or return to normal conditions.

        3. **Inferential calculation** (Category 3):  
        - You are a time series analysis expert. This is a metric called Container Image Pull Times collected from Kubernetes Cluster with length of 172: <ts><ts/>. The Container Image Pull Times data starts from an unspecified date, and each point represents a minute. In a Kubernetes cluster, the time taken to pull container images is being monitored. Considering the stability of the overall trend, how many significant downward spikes, indicating potential temporary issues in the image pull process, are present in the time series?  
        - You are a time series analysis expert. This is a metric called Manufacturing Costs collected from Manufacturing with length of 256: <ts><ts/>. The manufacturing costs data starts from January 1, and each point represents a day. During this period, a significant cost-saving initiative was implemented, causing a dramatic drop in costs. How many days did the manufacturing costs drop by more than 50 units within a short period? 

        **Guidelines:**  
        - Absolute Priority Rule for Counting Tasks: If the question contains "how many" and focuses on counting, it must be Category 3 (Inferential calculation), even if it involves anomaly detection (e.g., "abnormal") or scenario attribution (e.g., scenario prediction/attribution) content.
        - If uncertain, default to Category 4.  
        - Only output the category number based on the Output format.  

        **Question:** {question}   

        **Output format:**  
        - Category: [1/2/3/4]  
    """
    
    with open('./univariate_1round.jsonl', 'a') as f_uni, open('./multivariate_1round.jsonl', 'a') as f_multi:
        with open(input_file, 'r') as f_in:
            for idx, line in enumerate(f_in):
                if idx < start_idx:
                    continue
                if idx > end_idx:
                    break
                    
                try:
                    data = json.loads(line.strip())
                    input_text = data["input"]
                    
                    # 构建prompt并调用API
                    prompt = prompt_template.format(question=input_text)
                    response = gpt_chat(prompt)
                    
                    if response is None:
                        print(f"ID {idx}: API调用失败")
                        continue
                    
                    # 提取分类结果
                    match = re.search(r'Category:\s*(\d)', response)
                    if not match:
                        print(f"ID {idx}: 未找到分类结果 - {response}")
                        continue
                        
                    category = int(match.group(1))                    
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
                
                time.sleep(1)

if __name__ == "__main__":
    input_file = "./sft/chatts_sft_train.jsonl"   #"./chatts_sft_train.jsonl"
    start_index = 0  # 起始索引(包含)
    end_index = 2000    # 结束索引(包含)
    """
    卓敏0-10000; 湘婷10001-20000; 李林20001-30000; 奕非30001-40000
    """
    
    open('univariate_1round.jsonl', 'w').close()
    open('multivariate_1round.jsonl', 'w').close()
    
    process_data(input_file, start_index, end_index)
    print("处理完成.结果已保存到univariate_1round.jsonl和multivariate_1round.jsonl")



