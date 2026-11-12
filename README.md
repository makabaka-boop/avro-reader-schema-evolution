# Avro 跨版本 Schema 读取服务（Writer → Reader 转换）

数据平台升级事件结构后，历史 Avro Object Container File（OCF）仍保留**旧的字段名与
类型**。本服务接收一个真实的 `.avro` 容器文件和一份 **reader schema**，按 Avro
1.11.x 的 schema resolution 规则，把文件中的**全部记录**解释为新结构后返回。

核心原则：

- 成熟库（`avro` 1.11.x）只负责**容器解析、block 解压（null/deflate）与按 writer
  schema 读取二进制数据**；
- **writer → reader 的解析全部由本服务自行实现**，从不把 reader schema 传给现成解码器；
- 保留 union 的**实际 writer 分支身份**，输出采用 Avro JSON 编码；
- 任何错误都定位到 schema 路径（以及记录下标），**整次请求要么全部成功，要么整体失败，
  绝不返回部分记录**。

## 能力边界

| 维度 | 支持 |
| --- | --- |
| 数据类型 | `null`、`boolean`、`int`、`long`、`string`、`record`、`array`、`union` |
| 命名类型 | 命名类型引用、递归 record（按对象身份去重，不会无限展开或误判不兼容） |
| 压缩 | `null`、`deflate`（deflate 按 spec 使用 raw deflate，可读取 Java 实现写出的文件） |
| 不支持 | 逻辑类型，以及 `bytes`/`float`/`double`/`enum`/`fixed`/`map` 等其它类型；其它压缩编码 |
| 类型提升 | 仅 `int → long`（含数组元素、union 分支、嵌套 record 内） |

### 实现的 Avro 1.11.x resolution 规则

- 命名空间解析、record 名称匹配与 **reader 侧 record/field 别名**（含全名/短名资格化）；
- 字段**重排**、字段**删除**（writer 多出的字段被安全跳过）、reader **新增字段取默认值**；
- 字段别名歧义（一个 reader 字段命中多个 writer 字段，或多个 reader 字段争抢同一 writer
  字段）判定为错误；
- **union 按实际 writer 分支匹配 reader 中第一个兼容分支**，并在输出中保留分支身份；
- **writer 字段存在且值为显式 `null` 时，绝不使用 reader 默认值替换**；默认值只用于
  writer 中根本不存在的字段；
- union 默认值按规范针对**第一个分支**校验；record/array 嵌套默认值递归物化。

> 关于版本：Avro 1.11.4 没有发布对应版本的 Python 包（1.11.x 的 Python 实现停在
> 1.11.3，1.11.4 的改动不涉及 Python 端）。本项目依赖 `avro==1.11.3` 仅用于容器/二进制
> 读取，所有 resolution 语义均按 Avro 1.11.4 规范在本仓库内实现。

## 目录结构

```
app/
  errors.py           # 带 schema 路径/记录下标的领域错误
  schema_validate.py  # 支持类型白名单（拒绝逻辑类型与未支持类型）
  resolver.py         # 核心：兼容性预检、默认值、writer 读取、writer→reader 解析
  container.py        # OCF 容器接入（仅用库做信封/解压/writer 读取）
  avro_json.py        # Avro JSON 编码（union 输出为 {"分支名": 值}）
  main.py             # FastAPI 接口
scripts/
  generate_fixtures.py# 用真实 DataFileWriter 生成测试用 OCF（null + deflate）
tests/
  fixtures/           # 生成的真实 .avro 与 reader schema（生成物）
  test_conversion.py  # 端到端 + HTTP 测试
  test_resolution_rules.py
Dockerfile
```

## 本地运行

需要 Python 3.11。

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
```

健康检查：

```bash
curl http://127.0.0.1:8000/health
# {"status":"ok"}
```

## 接口

### `POST /convert`

`multipart/form-data`：

| 字段 | 说明 |
| --- | --- |
| `file` | 真实的 Avro Object Container File（`.avro`） |
| `reader_schema` | reader schema 的 JSON 文本 |

成功响应 `200`：

```json
{
  "record_count": 2,
  "records": [
    {
      "total": 3,
      "customer": {"name": "Ada", "customer_id": 7},
      "order_id": "o-1001",
      "status": "pending",
      "note": {"string": "rush"}
    },
    {
      "total": 10,
      "customer": {"name": "Lin", "customer_id": 8},
      "order_id": "o-1002",
      "status": "pending",
      "note": {"null": null}
    }
  ]
}
```

要点：

- 旧字段 `count:int = 3` 经 reader 字段别名映射为 `total:long`（唯一允许的 int→long 提升）；
- writer 中不存在的新字段 `status` 取 reader 默认值 `"pending"`；
- 第二条记录的 `note` 是**显式 null**，输出保留为 `{"null": null}`，没有被默认值覆盖；
- union 始终输出 `{"分支名": 值}`，null 分支也不例外，分支身份不丢失。

失败响应统一为 `422`，且不包含任何记录，例如：

```json
{
  "error": "missing_default",
  "message": "Reader field 'ghost' has no matching writer field and declares no default.",
  "writer_schema_path": "$",
  "reader_schema_path": "$.ghost"
}
```

错误类型：`unsupported_type`、`invalid_schema_json`、`incompatible_schemas`、
`ambiguous_alias`、`missing_default`、`invalid_default`、`unsupported_codec`、
`invalid_avro_file`；数据期错误还会带 `record_index`。

调用示例：

```bash
curl -X POST http://127.0.0.1:8000/convert \
  -F "file=@tests/fixtures/order_v1_deflate.avro;type=application/octet-stream" \
  -F "reader_schema=<tests/fixtures/order_v2_reader.json"
```

## Docker

```bash
docker build -t avro-resolution-service .
docker run --rm -p 8000:8000 avro-resolution-service
```

镜像以非 root 用户运行，内置 `/health` 健康检查。

## 测试

测试使用的是通过 `DataFileWriter` 生成的**真实 OCF 文件**，覆盖 null 与 deflate 两种
压缩，并包含字段改名、递归链、默认值与 union 分支选择等场景。

```bash
.venv/bin/pip install -r requirements-dev.txt
# 重新生成真实容器文件（可选，仓库已附带生成物）
.venv/bin/python scripts/generate_fixtures.py tests/fixtures
# 运行测试
.venv/bin/python -m pytest -q
```

测试覆盖的关键场景：

1. `count:int=3` 经别名映射为 `total:long`，新增字段取默认值，显式 null 不被默认值覆盖；
2. 递归 record 链（union 自引用 + array 自引用）正确终止、重复引用不报错，深层链（200+
   层）不无限递归；
3. union writer/reader 分支顺序不同，按实际 writer 分支选择 reader 第一个兼容分支；
4. 字段重排、删除、嵌套 record 别名、数组元素 int→long；
5. 未知/逻辑类型、歧义别名、缺默认值、非法提升、坏默认值、不支持的压缩、损坏容器，
   全部带 schema 路径（必要时带记录下标）整体失败。
