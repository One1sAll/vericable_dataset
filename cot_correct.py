import re
import json
from typing import List, Dict
from word2number import w2n 

def parse_cot_steps(cot_content: str) -> Dict[str, str | None]:
    """
    解析cot_deepseekr1字段，提取Step1~Step6的Judgment（Step6为final answer）
    返回格式：{step1_label, step2_label, step4_label, step6_label}
    """
    step_labels = {
        "step1_label": None,
        "step2_label": None,
        "step4_label": None,
        "step6_label": None
    }
    if not isinstance(cot_content, str) or cot_content.strip() == "":
        return step_labels

    # 先统一处理换行和多余空格，避免格式干扰
    cot_clean = cot_content.strip().replace("\n", " ").replace("  ", " ")
    
    # Step1: 匹配 Step1 中 [Judgment] 到 [Description] 之间的纯内容
    step1_pattern = r"Step 1.*?(?:\*\*)?\s*\[Judgment\]\s*(?:\*\*)?\s*([\s\S]+?)\s*(?:\*\*)?\s*\[Description\]\s*(?:\*\*)?"
    match = re.search(step1_pattern, cot_clean, re.IGNORECASE)
    if match:
        # 去除内容首尾空白和**符号
        step_labels["step1_label"] = match.group(1).strip().replace("**", "").capitalize()

    # Step2
    step2_pattern = r"Step 2.*?(?:\*\*)?\s*\[Judgment\]\s*(?:\*\*)?\s*([\s\S]+?)\s*(?:\*\*)?\s*\[Description\]\s*(?:\*\*)?"
    match = re.search(step2_pattern, cot_clean, re.IGNORECASE)
    if match:
        step_labels["step2_label"] = match.group(1).strip().replace("**", "").capitalize()

    # Step4
    step4_pattern = r"Step 4.*?(?:\*\*)?\s*\[Judgment\]\s*(?:\*\*)?\s*([\s\S]+?)\s*(?:\*\*)?\s*\[Description\]\s*(?:\*\*)?"
    match = re.search(step4_pattern, cot_clean, re.IGNORECASE)
    if match:
        step_labels["step4_label"] = match.group(1).strip().replace("**", "").capitalize()

    # Step6: 匹配 Step6 中 [Judgment] 到字符串结尾的纯内容（无Description）
    step6_pattern = r"Step 6.*?(?:\*\*)?\s*\[Judgment\]\s*(?:\*\*)?\s*([\s\S]+?)\s*$"
    match = re.search(step6_pattern, cot_clean, re.IGNORECASE)
    if match:
        step_labels["step6_label"] = match.group(1).strip().replace("**", "").capitalize()

    # 统一格式：去除多余符号，标准化空值
    for key in step_labels:
        if step_labels[key] in ["", "none", "null"]:
            step_labels[key] = None
            
    return step_labels

def generate_cot_field(cot_content: str, step6_label: str | None) -> str:
    cot_clean = cot_content.strip() if isinstance(cot_content, str) else ""
    answer_part = f"The answer is {step6_label}." if step6_label else "The answer is unknown."
    return f"<think>{cot_clean}</think><ANSWER>{answer_part}</ANSWER>"

def process_jsonl(input_file: str, correct_file: str, wrong_file: str) -> None:
    total_count = 0
    correct_count = 0
    wrong_count = 0
    error_count = 0
    error_id = []
    empty_label_id = []
    
    with open(correct_file, 'w', encoding="utf-8") as f_match, \
         open(wrong_file, 'w', encoding="utf-8") as f_mismatch :

        with open(input_file, 'r', encoding="utf-8") as f_in:
            for line_num, line in enumerate(f_in, 1):
                line = line.strip()
                if not line:
                    continue
                total_count += 1

                try:
                    data = json.loads(line)
                    # 必要字段校验
                    required_fields = ["id", "task", "output", "timeseries", "cot_deepseekr1", "label"]
                    for field in required_fields:
                        if field not in data:
                            raise KeyError(f"缺失必要字段: {field}")
                        
                    label = data["label"]
                    id = data["id"]
                    cot_content = data["cot_deepseekr1"]
                    
                    # 提取stepx_label, 生成cot字段
                    step_labels = parse_cot_steps(cot_content)
                    
                    # 检查是否有空白标签并记录
                    for step, l in step_labels.items():
                        if l is None or l.strip() == "":
                            empty_label_id.append(id)
                            print(f" ID {id} : {step} 标签为none或空白")

                    step6_label = step_labels.get("step6_label") or "unknown"
                    cot_field = generate_cot_field(cot_content, step6_label)

                    # 准备要插入的新字段（cot + stepx字段）
                    new_fields = {**step_labels, "cot": cot_field}
                    # 创建新字典并保持字段顺序，在label后插入新字段
                    new_data = {}
                    label_found = False  # 标记是否已插入新字段
                    for key, value in data.items():
                        new_data[key] = value
                        if key == "label" and not label_found:
                            new_data.update(new_fields)
                            label_found = True
                    
                    # 若原始数据中没有label字段，将新字段添加到末尾
                    if not label_found:
                        new_data.update(new_fields)
                        
                        
                    # 推理最终答案是否正确,忽略大小写
                    def normalize_text(text: str) -> str:
                        return re.sub(r"[^\w\s]", "", text.strip().lower()).replace(" ", "")

                    norm_base = normalize_text(label)
                    norm_step6 = normalize_text(step6_label)
                    is_match = norm_step6 in norm_base or norm_base in norm_step6

                    # 分别输出
                    if is_match:
                        f_match.write(json.dumps(new_data, ensure_ascii=False) + "\n")
                        correct_count += 1
                        print(f" ID {id} : 推理正确 | Step6_label: {step6_label} | label: {label}")
                    else:
                        f_mismatch.write(json.dumps(new_data, ensure_ascii=False) + "\n")
                        wrong_count += 1
                        print(f" ID {id} : 推理失败 | Step6_label: {step6_label} | label: {label}")

                except json.JSONDecodeError as e:
                    error_count += 1
                    error_id.append(id)
                    print(f" ID {id} : JSON解析错误 - {str(e)}")

                except KeyError as e:
                    error_count += 1
                    error_id.append(id)
                    print(f" ID {id} : 字段缺失 - {str(e)}")

                except Exception as e:
                    error_count += 1
                    print(f" ID {id} : 未知错误 - {str(e)}")

    # 输出最终统计报告
    print("\n" + "="*50)
    print("处理完成！统计结果：")
    print(f"总数据量：{total_count} 条")
    print(f"匹配成功：{correct_count} 条（输出至 {correct_file}）")
    print(f"匹配失败：{wrong_count} 条（输出至 {wrong_file}）")
    print(f"处理错误：{error_count} 条, 失败ID: {error_id}")
    print(f"包含空白标签的ID: {empty_label_id}")
    


if __name__ == "__main__":

    input_path = "./univariate_0_2000_filtered_labeled_cot.jsonl"
    correct_path = "./univariate_0_2000_filtered_labeled_cot_stepLabeled_correct.jsonl"       # 匹配成功
    wrong_path = "./univariate_0_2000_filtered_labeled_cot_stepLabeled_wrong.jsonl" # 匹配失败
    
    # 清空输出文件
    open(correct_path, 'w').close()
    open(wrong_path, 'w').close()

    # 执行批量处理
    process_jsonl(input_path, correct_path, wrong_path)