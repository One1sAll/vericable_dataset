import json
import re
import time
from openai import OpenAI

# 配置OpenAI客户端
gpt_model = "deepseek-r1"
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


template_Anomaly_detection = '''
Please think step by step and strictly follow the specified output format for each step:
Step 1. **Analyzing task intent**: 
Analyze the intent of the given problem and identify its core objective in the context of anomaly detection. Specifically, determine whether the task requires identifying an abnormal case, a normal case, or another type of detection task. 
**Output Format**:
Step 1 Analyzing task intent:
[Judgment] <Your classification judgement (e.g., This is an anomaly detection task. / This is a normal detection task.)>
[Description] <Provide a rationale for the classification based on the given problem context. Cite specific keywords or requirements from the problem and explain how these details align with the defined task type.>
Step 2. **Selecting task-relevant key patterns**: 
Align with the task's core objective to identify core features that directly support task completion. These features must be actionable and critical to deriving valid conclusions. Valid pattern categories include temporal patterns (e.g., trend; amplitude; fluctuation; continuity), judgment criteria (e.g., task-specific definitions of patterns), threshold values (e.g., upper bounds; lower bounds; percentage deviations), and other decisive patterns or criteria that are critical for resolving the task.
**Output Format**:
Step 2 Selecting task-relevant key patterns:
[Judgment] <Only list the names of the selected key patterns (no extra details, analysis, or conclusions); separate multiple items with semicolons.>
[Description] <For each selected pattern: Clarify the pattern's specific details; explain how it aligns with the task’s core objective; elaborate on why it is critical to match the task's core objective.>
Step 3. **Analyzing time series samples using selected key patterns**: 
Clarify the specific characteristics (e.g., occurrence timing, duration, intensity) and inherent rules of the selected key patterns. Then evaluate whether the time series sample conforms to these task-relevant patterns and criteria.  You can analyze the entire series holistically, or split it into meaningful segments (e.g., by time period, event node) based on task requirements.
**Output Format**:
Step 3 Analyzing time series samples using selected key patterns:
[Analysis] <Your analysis process.>
Step 4. **Generating preliminary answers by combining task intent and key patterns**: 
Based on the analysis of task requirements, patterns, and time series data from prior steps, formulate preliminary answers. 
**Output Format**:
Step 4 Generating preliminary answers by combining task intent and key patterns:
[Judgment] <Preliminary conclusion (e.g., Yes / No).> 
[Description] <Provide a rationale for the judgement.>
Step 5. **Enhancing answers through reflection**: 
Verify whether the selection of key patterns is comprehensive, ensuring no relevant features are omitted. Check the correctness of the analysis. Eliminate interfering factors that may affect the validity of the analysis.
**Output Format**:
Step 5 Enhancing answers through reflection:
[Analysis] <Your reflection and verification process.>
Step 6. **Summarizing the thinking process to output the answer**: 
Integrate the entire analytical process, clearly presenting the complete logic from understanding the task, analyzing patterns, generating conclusions to verifying results. Finally, output an answer that meets the task requirements. **Output Format**:
Step 6 Summarizing the thinking process to output the answer: 
[Description] <Summary of the reasoning process across all steps.>
[Judgment] <Final answer (e.g., Yes / No).>
'''

template_Inferential_calculation = '''
Please think step by step and strictly follow the specified output format for each step:
Step 1. **Analyzing task intent**: 
Analyze the intent of the given problem and identify its core objective in the context of time series reasoning. Specifically, determine the specific object that needs to count in the numerical calculation task.
**Output Format**:
Step 1 Analyzing task intent:
[Judgment] <The specific counting object (e.g., days/occasions/siginifcant drops).>
[Description] <Provide a rationale for the judgement based on the given problem context. Cite specific keywords or requirements from the problem and explain how these details align with the judgement.>
Step 2. **Selecting task-relevant key patterns**: 
Align with the task's core objective to identify core features that directly support task completion. These features must be actionable and critical to deriving valid conclusions. Valid pattern categories include temporal patterns (e.g., trend; amplitude; fluctuation; continuity), judgment criteria (e.g., task-specific definitions of patterns), threshold values (e.g., upper bounds; lower bounds; percentage deviations), and other decisive patterns or criteria that are critical for resolving the task.
**Output Format**:
Step 2 Selecting task-relevant key patterns:
[Judgment] <Only list the names of the selected key patterns (no extra details, analysis, or conclusions); separate multiple items with semicolons.>
[Description] <For each selected pattern: Clarify the pattern's specific details; explain how it aligns with the task’s core objective; elaborate on why it is critical to match the task's core objective.>
Step 3. **Analyzing time series samples using selected key patterns**: 
Clarify the specific characteristics (e.g., occurrence timing, duration, intensity) and inherent rules of the selected key patterns. Then evaluate whether the time series sample conforms to these task-relevant patterns and criteria.  You can analyze the entire series holistically, or split it into meaningful segments (e.g., by time period, event node) based on task requirements.
**Output Format**:
Step 3 Analyzing time series samples using selected key patterns:
[Analysis] <Your analysis process.>
Step 4. **Generating preliminary answers by combining task intent and key patterns**: 
Based on the analysis of task requirements, patterns, and time series data from prior steps, formulate preliminary answers.
**Output Format**:
Step 4 Generating preliminary answers by combining task intent and key patterns:
[Judgment] <Preliminary conclusion (e.g., calculated value X).> 
[Description] <Provide a rationale for the judgement.>
Step 5. **Enhancing answers through reflection**: 
Verify whether the selection of key patterns is comprehensive, ensuring no relevant features are omitted. Check the correctness of the analysis. Eliminate interfering factors that may affect the validity of the analysis.
**Output Format**:
Step 5 Enhancing answers through reflection:
[Analysis] <Your reflection and verification process.>
Step 6. **Summarizing the thinking process to output the answer**: 
Integrate the entire analytical process, clearly presenting the complete logic from understanding the task, analyzing patterns, generating conclusions to verifying results. Finally, output an answer that meets the task requirements. 
**Output Format**:
Step 6 Summarizing the thinking process to output the answer: 
[Description] <Summary of the reasoning process across all steps.>
[Judgment] <Final answer. (e.g., calculated value X) (The calculated numerical value must be in digit form.)>
'''

template_Scenario_attribution = '''
Please think step by step and strictly follow the specified output format for each step:
Step 1.**Analyzing task intent**:
Analyze the intent of the given problem and identify its core objective in the context of scenario inference. First  clarify the background of the time series data involved in the problem (e.g., the meaning of the data field, its corresponding scenario), then specifically determine whether the task requires scenario attribution, scenario prediction, or another type of scenario-related task.
**Output Format**:
Step 1 Analyzing task intent:
[Judgment] <Your classification judgement (e.g., This is a scenario attribution task. / This is a scenario prediction task.)>
[Description] <Provide a rationale for the classification based on the given problem context. Cite specific keywords or requirements from the problem and explain how these details align with the defined task type.>
Step 2.**Selecting task-relevant key patterns**: 
Align with the task's core objective to identify core features that directly support task completion. These features must be actionable and critical to deriving valid conclusions. Valid pattern categories include temporal patterns (e.g., trend; amplitude; fluctuation; continuity), judgment criteria (e.g., task-specific definitions of patterns), threshold values (e.g., upper bounds; lower bounds; percentage deviations), and other decisive patterns or criteria that are critical for resolving the task.
**Output Format**:
Step 2 Selecting task-relevant key patterns:
[Judgment] <Only list the names of the selected key patterns (no extra details, analysis, or conclusions); separate multiple items with semicolons.>
[Description] <For each selected pattern: Clarify the pattern's specific details; explain how it aligns with the task’s core objective; elaborate on why it is critical to match the task's core objective.
Step 3.**Analyzing time series samples using selected key patterns**: 
Clarify the specific characteristics (e.g., occurrence timing, duration, intensity) and inherent rules of the selected key patterns. Then evaluate whether the time series sample conforms to these task-relevant patterns and criteria.  You can analyze the entire series holistically, or split it into meaningful segments (e.g., by time period, event node) based on task requirements.
**Output Format**:
Step 3 Analyzing time series samples using selected key patterns:
[Analysis] <Your analysis process.>
Step 4.**Generating preliminary answers by combining task intent and key patterns**: 
Based on the analysis of task requirements, patterns, and time series data from prior steps, formulate preliminary answers.
**Output Format**:
Step 4 Generating preliminary answers by combining task intent and key patterns:
[Judgment] <Preliminary answer. (Select one option from the given choices. **MUST** retain the complete original text of the option. )> ；
[Description] <Provide a rationale for the judgement.>
Step 5.**Enhancing answers through reflection**: 
Verify whether the selection of key patterns is comprehensive, ensuring no relevant features are omitted. Check the correctness of the analysis. Eliminate interfering factors that may affect the validity of the analysis.
**Output Format**:
Step 5 Enhancing answers through reflection:
[Analysis] <Your reflection and verification process.>
Step 6.**Summarizing the thinking process to output the answer**:  
Integrate the entire analytical process, clearly presenting the complete logic from understanding the task, analyzing patterns, generating conclusions to verifying results. Finally, output an answer that meets the task requirements. 
**Output Format**:
Step 6 Summarizing the thinking process to output the answer: 
[Description] <Summary of the reasoning process across all steps.>
[Judgment] <Final answer. (Select one option from the given choices. **MUST** retain the complete original text of the option.)>
'''


def process_jsonl_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            data = json.loads(line.strip())
            
            # 提取所需字段
            task = data.get('task', '')
            question = data.get('question', '')
            timeseries2 = data.get('timeseries2', [])
            id = data.get('id', '未知')
            
            # 获取时间序列数据（假设timeseries2是一个二维列表，取第一个元素）
            ts_data = timeseries2[0] if isinstance(timeseries2[0], list) else timeseries2
            
            # 将时间序列数据转换为字符串
            ts_str = ', '.join(map(str, ts_data))
            
            # 替换question中的<ts><ts/>标记
            updated_question = question.replace('<ts><ts/>', ts_str)
            # print(updated_question)
            
            # 根据任务类型选择模板
            if task == "Anomaly detection":
                prompt = updated_question + template_Anomaly_detection
            elif task == "Inferential calculation":
                prompt = updated_question + template_Inferential_calculation
            elif task == "Scenario attribution":
                prompt = updated_question + template_Scenario_attribution
            

            print(f"处理ID {id}，任务: {task}")
            cot_response = gpt_chat(prompt)
            
            # 在label和timeseries之间添加cot_deepseekr1字段
            # 创建一个新字典，保持原有字段顺序
            new_data = {}
            for key, value in data.items():
                new_data[key] = value
                if key == 'label':
                    new_data['cot_deepseekr1'] = cot_response
            
            json.dump(new_data, outfile)
            outfile.write('\n')
            
            time.sleep(1)


if __name__ == "__main__":
    input_filename = "./univariate_0_2000_filtered_labeled.jsonl"
    output_filename = "./univariate_0_2000_filtered_labeled_cot.jsonl"
    
    process_jsonl_file(input_filename, output_filename)
