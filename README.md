# 数据导出系统 (Data Export System)

一个用于问答数据标注系统的数据导出模块，支持多种格式和灵活的筛选条件。

## 功能特性

- **多格式导出**: 支持 CSV、JSON 和 Excel 格式
- **灵活筛选**: 提供多维度数据筛选功能
- **中文支持**: 完整支持中文字符编码
- **统计信息**: 提供导出数据的详细统计
- **Web集成**: 与 Flask 框架无缝集成

## 系统架构

### 核心组件

- `DataExporter`: 主要的数据导出类
- 数据库支持: SQLite 数据库存储
- 字段映射: 完整的中英文字段对照

## 安装依赖

```bash
# 基础依赖
pip install flask

# Excel导出支持（可选）
pip install openpyxl
```

## 快速开始

### 基本使用

```python
from DataExporter import DataExporter

# 初始化导出器
exporter = DataExporter(db_path='users.db')

# 导出所有数据为CSV
response = exporter.export_data(export_format='csv')

# 导出JSON格式
response = exporter.export_data(export_format='json')

# 导出Excel格式
response = exporter.export_data(export_format='xlsx')
```

### 高级筛选

```python
# 定义筛选条件
filters = {
    'status': 'completed',      # 状态筛选
    'annotator': 'user123',     # 标注员筛选
    'agreement': 'agreed',      # 一致性筛选
    'result': 'good'           # 结果筛选
}

# 应用筛选条件导出
response = exporter.export_data(
    export_format='csv',
    filters=filters,
    filename_prefix='filtered_data'
)
```

## 数据字段

系统支持以下数据字段的导出：

| 英文字段名 | 中文名称 | 描述 |
|------------|----------|------|
| id | ID | 数据唯一标识符 |
| question_cn | 中文问题 | 中文版本的问题内容 |
| answer_cn | 中文答案 | 中文版本的答案内容 |
| question_en | 英文问题 | 英文版本的问题内容 |
| answer_en | 英文答案 | 英文版本的答案内容 |
| subject | 学科 | 问题所属学科分类 |
| clue_urls | 线索URLs | 相关线索的URL链接 |
| traces | 推理轨迹 | 推理过程记录 |
| answer_clue | 答案线索 | 答案相关的线索信息 |
| answer_url | 答案URL | 答案来源URL |
| check_info | 检查信息 | 质量检查信息 |
| dfsw_info | DFSW信息 | DFSW相关信息 |
| created_at | 创建时间 | 数据创建时间戳 |
| uploaded_by | 上传者 | 数据上传用户 |
| first_annotator | 第一标注员 | 第一轮标注的用户 |
| first_annotation_result | 第一标注结果 | 第一轮标注的结果 |
| first_annotation_time | 第一标注时间 | 第一轮标注的时间 |
| second_annotator | 第二标注员 | 第二轮标注的用户 |
| second_annotation_result | 第二标注结果 | 第二轮标注的结果 |
| second_annotation_time | 第二标注时间 | 第二轮标注的时间 |
| final_status | 最终状态 | 数据的最终状态 |
| annotation_result | 管理员标注结果 | 管理员的标注结果 |
| annotated_by | 管理员 | 执行管理员标注的用户 |
| annotated_at | 标注时间 | 管理员标注的时间 |

## 筛选选项

### 状态筛选 (status)
- `all`: 所有状态
- `pending`: 待标注（未开始标注）
- `first_completed`: 第一轮标注完成
- `completed`: 双重标注完成

### 标注员筛选 (annotator)
- `all`: 所有标注员
- 具体用户名: 筛选特定标注员的数据

### 一致性筛选 (agreement)
- `all`: 所有一致性状态
- `agreed`: 两轮标注结果一致
- `conflicted`: 两轮标注结果冲突

### 结果筛选 (result)
- `all`: 所有结果
- `good`: 包含好评结果
- `bad`: 包含差评结果
- `uncertain`: 包含不确定结果
- `agreed_good`: 双方一致认为好
- `agreed_bad`: 双方一致认为差
- `agreed_uncertain`: 双方一致认为不确定
- `admin_approved`: 管理员认为好
- `high_quality`: 高质量数据（管理员好评或双方一致好评）

## API 参考

### DataExporter 类

#### `__init__(db_path='users.db')`
初始化数据导出器。

**参数:**
- `db_path` (str): 数据库文件路径

#### `export_data(export_format='csv', filters=None, filename_prefix="data_export")`
统一的数据导出接口。

**参数:**
- `export_format` (str): 导出格式 ('csv', 'json', 'xlsx')
- `filters` (dict): 筛选条件字典
- `filename_prefix` (str): 文件名前缀

**返回:**
- Flask Response 对象

#### `get_annotators()`
获取所有标注员列表。

**返回:**
- list: 标注员用户名列表

#### `get_export_statistics(filters=None)`
获取导出数据的统计信息。

**参数:**
- `filters` (dict): 筛选条件字典

**返回:**
- dict: 包含统计信息的字典

### 统计信息字段

```python
{
    'total_count': 总数据量,
    'completed_count': 完成双重标注的数量,
    'pending_count': 待标注数量,
    'first_completed_count': 完成第一轮标注的数量,
    'agreed_count': 一致标注数量,
    'conflicted_count': 冲突标注数量,
    'good_count': 好评数量,
    'bad_count': 差评数量,
    'uncertain_count': 不确定数量
}
```

## 文件格式特性

### CSV 格式
- UTF-8 编码，支持中文
- 包含 BOM 标识，确保 Excel 正确识别
- JSON 字段自动格式化

### JSON 格式
- UTF-8 编码
- 保持原始 JSON 结构
- 美化输出（2空格缩进）

### Excel 格式
- 需要安装 `openpyxl` 库
- 自动列宽调整
- 表头样式美化
- 长文本自动换行
- 冻结首行便于浏览

## 错误处理

系统提供完善的错误处理机制：

- 缺少依赖库时的友好提示
- JSON 解析错误的容错处理
- 数据库连接异常的处理
- 不支持格式的错误响应

## 文件命名规则

导出文件使用时间戳命名：
```
{filename_prefix}_{YYYYMMDD_HHMMSS}.{extension}
```

示例：
- `data_export_20250915_143022.csv`
- `filtered_data_20250915_143022.xlsx`

## 注意事项

1. **Excel 导出**: 需要安装 `openpyxl` 库
2. **编码支持**: 所有格式均支持中文字符
3. **内存使用**: 大量数据导出时注意内存占用
4. **数据库连接**: 确保数据库文件路径正确且可访问

## 向后兼容

为保持向后兼容性，保留了原有的函数接口：
- `export_to_csv()`
- `export_to_json()`
- `export_to_excel()`

## 贡献指南

欢迎提交 Issue 和 Pull Request 来改进这个项目。

## 许可证

请查看项目根目录下的 LICENSE 文件。