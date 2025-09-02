import json

def read_ts_dataset(file_path):
    """
    读取特殊格式多变量TS数据集：变量间用冒号分隔，最后一个冒号后为label
    格式示例：变量1数据(逗号分隔):变量2数据(逗号分隔):变量3数据(逗号分隔):label
    返回：(元信息字典, 数据列表)，数据列表含label和多变量时序二维列表
    """
    meta_info = {}
    data_list = []
    single_series_len = 0  # 单个变量的序列长度（从@seriesLength获取）
    var_count = None       # 变量数量（自动从第一条有效数据推断）

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]

        # 1. 解析元信息（重点获取单个变量长度@seriesLength）
        for line in lines:
            if line.startswith('#'):
                continue
            if line.startswith('@'):
                key_val = line.split(' ', 1)
                if len(key_val) == 2:
                    key = key_val[0][1:]
                    val = key_val[1].strip()
                    meta_info[key] = val
                    # 提取单个变量的序列长度（用于校验每个变量的数据点数量）
                    if key == 'seriesLength':
                        single_series_len = int(val)

        # 2. 定位数据开始位置（@data之后的行）
        data_start_idx = None
        for idx, line in enumerate(lines):
            if line.strip().lower() == '@data':
                data_start_idx = idx + 1
                break

        # 3. 核心逻辑：解析"变量1:变量2:变量3:label"格式数据
        if data_start_idx is not None and data_start_idx < len(lines):
            for line_num, data_line in enumerate(lines[data_start_idx:], start=data_start_idx+1):
                # 跳过无冒号的无效行（至少需有"变量:label"，即至少1个冒号）
                if ':' not in data_line:
                    print(f"第{line_num}行：无冒号，跳过无效行 -> {data_line}")
                    continue

                # 分割变量数据和label：最后一个冒号前是所有变量数据，后面是label
                parts = data_line.rsplit(':', 1)
                if len(parts) != 2:
                    print(f"第{line_num}行：分割label失败，跳过 -> {data_line}")
                    continue
                all_var_str = parts[0].strip()  # 所有变量的字符串（变量间用冒号分隔）
                label = parts[1].strip()        # 标签（确保非空）

                # 校验label有效性（若为数值标签，可根据需求调整校验规则）
                if not label:
                    print(f"第{line_num}行：label为空，跳过 -> {data_line}")
                    continue

                # 分割各个变量的字符串（变量间用冒号分隔）
                var_str_list = all_var_str.split(':')
                # 推断变量数量（第一条有效数据确定后，后续数据需保持一致）
                if var_count is None:
                    var_count = len(var_str_list)
                    print(f"自动推断变量数量：{var_count}（从第{line_num}行数据获取）")
                # 校验当前行变量数量与推断值一致
                elif len(var_str_list) != var_count:
                    print(f"第{line_num}行：变量数量不匹配（期望{var_count}个，实际{len(var_str_list)}个），跳过 -> {data_line}")
                    continue

                # 解析每个变量的时序数据（转为列表）
                multivariate_series = []
                valid_var = True  # 标记当前行所有变量是否解析有效
                for var_idx, var_str in enumerate(var_str_list, start=1):
                    # 分割当前变量的所有数据点（数据点间用逗号分隔）
                    point_str_list = [p.strip() for p in var_str.split(',') if p.strip()]
                    # 校验当前变量的数据点数量（若元信息有@seriesLength则强制匹配）
                    if single_series_len > 0 and len(point_str_list) != single_series_len:
                        print(f"第{line_num}行：变量{var_idx}数据点数量不匹配（期望{single_series_len}个，实际{len(point_str_list)}个），跳过 -> {data_line}")
                        valid_var = False
                        break
                    # 转换为浮点数（捕获单个数据点的解析错误）
                    try:
                        var_data = [float(point) for point in point_str_list]
                    except ValueError as e:
                        print(f"第{line_num}行：变量{var_idx}解析失败（{e}），跳过 -> {data_line}")
                        valid_var = False
                        break
                    multivariate_series.append(var_data)

                # 所有变量解析有效，加入数据列表
                if valid_var:
                    data_list.append({
                        'label': label,
                        'time_series': multivariate_series
                    })

    # 补充元信息（变量数量、单个变量长度）
    if var_count is not None:
        meta_info['variableCount'] = var_count
    if single_series_len > 0:
        meta_info['singleSeriesLength'] = single_series_len

    return meta_info, data_list

def convert_ts_to_jsonl(ts_file_path, jsonl_output_path, task, id2label, question):
    
    # 读取TS数据
    meta_info, ts_data_list = read_ts_dataset(ts_file_path)
    
    print(f'数据集元信息: \n{meta_info} \n')
    
    # 按格式生成JSONL内容
    with open(jsonl_output_path, 'w', encoding='utf-8') as f:
        for idx, data in enumerate(ts_data_list):
            # 生成保留4位精度的时序数据（二维列表结构不变）
            time_series_4dp = []
            for var_data in data['time_series']:
                var_data_4dp = [round(point, 4) for point in var_data]
                time_series_4dp.append(var_data_4dp)
            
            
            # 构建单条JSON数据
            json_data = {
                "id": idx,  # 从0开始递增的ID
                "task": task,  # 固定任务字段
                "question": question, 
                "label": id2label[data['label']],  # 原数据集中的类别标签
                "timeseries": data['time_series'],  # 原始精度时序数据
                "timeseries2": time_series_4dp  # 4位精度时序数据
            }
            
            # 写入JSONL（每条一行，使用ensure_ascii=False保留可能的特殊字符）
            f.write(json.dumps(json_data, ensure_ascii=False) + '\n')
    
    print(f"转换完成！JSONL文件已保存至: {jsonl_output_path}")
    print(f"共处理 {len(ts_data_list)} 条时序数据")
    print(f"变量数量: {len(ts_data_list[0]['time_series'])}")

# ------------------- 示例调用 -------------------
if __name__ == "__main__":

    INPUT_TS_PATH = "./TEE/TEE_TRAIN.ts"
    OUTPUT_JSONL_PATH = "./TEE/TEE_TRAIN.jsonl"
    task = "TEE"
    id2label = {
        "0": "CG Positive",
        "1": "IR Negative",
        "2": "Subsequent Return Stroke",
        "3": "Impulsive",
        "4": "Impulsive Pair",
        "5": "Gradual Intra-Cloud",
        "6": "Off-record",
    }
    question = "You are a time series analysis expert. This is a time series signal derived from lightning-related electromagnetic events, recorded by the FORTE satellite: <ts><ts/>. Your task is to classify the signal into one of the following seven event types: - CG Positive: A positive charge is lowered from a cloud to the ground. The waveform shows a sharp turn-on of radiation followed by hundreds of microseconds of noise. - IR Negative: A negative charge moves cloud-to-ground. The waveform gradually ramps up, peaks sharply (attachment point), then declines exponentially. - Subsequent Return Stroke: A follow-up negative stroke after an initial one. Similar waveform but without the ramp-up phase. - Impulsive: A sudden, sharp peak in the waveform, typical of intra-cloud events. - Impulsive Pair: Two sharp, closely spaced peaks—also known as TIPPs (Trans-Ionospheric Pulse Pairs). - Gradual Intra-Cloud: A gradual increase in power, more spread out than impulsive types. - Off-record: The signal is incomplete; the event extends beyond the 800 microsecond window. You are required to identify and report the approximate value ranges (minimum and maximum) of the signals over the time period. Choose the best matching label for the full signal from: a)CG Positive, b)IR Negative, c)Subsequent Return Stroke, d)Impulsive, e)Impulsive Pair, f)Gradual Intra-Cloud, g)Off-record."
    
    # INPUT_TS_PATH = "./HAR/HAR_TRAIN.ts"
    # OUTPUT_JSONL_PATH = "./HAR/HAR_TRAIN.jsonl"
    # task = "HAR"
    # id2label = {
    #     "0": "walking",
    #     "1": "walking_upstairs",
    #     "2": "walking_downstairs",
    #     "3": "sitting",
    #     "4": "standing",
    #     "5": "laying",
    # }
    # question = ""
    
    
    # 执行转换
    convert_ts_to_jsonl(INPUT_TS_PATH, OUTPUT_JSONL_PATH, task, id2label, question)
    