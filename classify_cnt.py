import re
import json
from typing import List, Dict
from word2number import w2n 


def process_jsonl_label(input_file: str) -> None:
    anomaly_cnt = 0
    scenario_cnt = 0
    inferential_cnt = 0
    wrong_id = []
    total_cnt = 0
    with open(input_file, 'r', encoding="utf-8") as f_in:
        for idx, line in enumerate(f_in):           
            # 过滤空行
            line = line.strip()
            if not line:
                continue
            
            try:
                data = json.loads(line.strip())
                
                id = data["id"]
                task = data["task"].strip()
                
                # 分类任务
                if task == "Anomaly detection":
                    anomaly_cnt += 1
                elif task == "Scenario attribution":
                    scenario_cnt += 1
                elif task == "Inferential calculation":
                    inferential_cnt += 1
                else:
                    print(f"ID {id}: 未知任务类型 '{task}'，跳过")
                    wrong_id.append(id)
                    continue
    
                print(f"ID {id}: 任务 {task}")
                total_cnt += 1
            
            except json.JSONDecodeError as e:
                print(f"错误：id:{id}样本处理失败 - {str(e)}，跳过")
            except KeyError as e:
                print(f"错误：id:{id}样本处理失败 - {str(e)}，跳过")
            
        print(f"\n分类统计结果:")
        print(f"总样本数: {total_cnt}")
        print(f"Anomaly detection: {anomaly_cnt}")
        print(f"Scenario attribution: {scenario_cnt}")
        print(f"Inferential calculation: {inferential_cnt}")
        print(f"处理失败样本ID: {wrong_id}")
    


if __name__ == "__main__":

    input_path = "./univariate_0_2000_filtered_labeled_cot_stepLabeled_correct_step2label.jsonl"

    # 执行批量处理
    process_jsonl_label(input_path)