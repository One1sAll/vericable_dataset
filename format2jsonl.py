import json
import re

def fix_jsonl_format(input_path, output_path):
    # 读取原始文件内容
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 用正则表达式匹配每个独立的 JSON 对象（以 { 开头，} 结尾）
    # 注意：确保你的 JSON 中没有嵌套的 {} 导致匹配错误（你的数据中未出现这种情况）
    json_objects = re.findall(r'\{[\s\S]*?\}', content)
    
    wrong_id = []  # 记录无效 JSON 的 ID
    # 写入修复后的 JSONL 文件（每行一个 JSON 对象）
    with open(output_path, 'w', encoding='utf-8') as f:
        for obj_str in json_objects:
            try:
                # 解析 JSON 确保格式正确，再压缩为一行
                obj = json.loads(obj_str)
                id = obj.get("id", "unknown_id")
                f.write(json.dumps(obj, ensure_ascii=False) + '\n')
            except json.JSONDecodeError as e:
                print(f"ID {id} 跳过无效 JSON: {e}")
                wrong_id.append(id)
    
    print(f"修复完成，已保存至 {output_path}")
    print(f"跳过无效 JSON 的 ID 列表: {wrong_id}")

# 使用示例
if __name__ == "__main__":
    fix_jsonl_format("univariate_0_2000_filtered_labeled_cot_stepLabeled_correct2 copy.jsonl", "univariate_0_2000_filtered_labeled_cot_stepLabeled_correct2 copy testtt.jsonl")  # 替换为你的输入输出路径