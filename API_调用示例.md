# API 调用示例

## 启动 API 服务

```bash
# 安装依赖
pip install flask flask-cors mysql-connector-python openpyxl

# 启动服务
python3 api_server.py
```

服务启动后监听在 `http://0.0.0.0:5000`

---

## 1. 健康检查

**接口**: `GET /api/health`

**请求示例**:
```bash
curl http://localhost:5000/api/health
```

**返回示例**:
```json
{
  "status": "ok",
  "timestamp": "2025-12-16T10:30:00"
}
```

---

## 2. 生成日报

**接口**: `POST /api/reports/daily`

**请求体**:
```json
{
  "report_date": "2025-12-12"
}
```

**CURL 示例**:
```bash
curl -X POST http://localhost:5000/api/reports/daily \
  -H "Content-Type: application/json" \
  -d '{"report_date": "2025-12-12"}' \
  --output 日报_2025-12-12.xlsx
```

**Python 示例**:
```python
import requests

response = requests.post(
    'http://localhost:5000/api/reports/daily',
    json={'report_date': '2025-12-12'}
)

if response.status_code == 200:
    with open('日报_2025-12-12.xlsx', 'wb') as f:
        f.write(response.content)
    print("✅ 日报下载成功")
else:
    print(f"❌ 错误: {response.json()}")
```

**JavaScript (Axios) 示例**:
```javascript
const axios = require('axios');
const fs = require('fs');

axios.post('http://localhost:5000/api/reports/daily', {
    report_date: '2025-12-12'
}, {
    responseType: 'blob'
}).then(response => {
    fs.writeFileSync('日报_2025-12-12.xlsx', response.data);
    console.log('✅ 日报下载成功');
}).catch(error => {
    console.error('❌ 错误:', error.response.data);
});
```

---

## 3. 生成周报

**接口**: `POST /api/reports/weekly`

**请求体**:
```json
{
  "week1_start": "2025-11-10",
  "week1_end": "2025-11-16",
  "week2_start": "2025-11-17",
  "week2_end": "2025-11-23"
}
```

**CURL 示例**:
```bash
curl -X POST http://localhost:5000/api/reports/weekly \
  -H "Content-Type: application/json" \
  -d '{
    "week1_start": "2025-11-10",
    "week1_end": "2025-11-16",
    "week2_start": "2025-11-17",
    "week2_end": "2025-11-23"
  }' \
  --output 周报.xlsx
```

**Python 示例**:
```python
import requests

response = requests.post(
    'http://localhost:5000/api/reports/weekly',
    json={
        'week1_start': '2025-11-10',
        'week1_end': '2025-11-16',
        'week2_start': '2025-11-17',
        'week2_end': '2025-11-23'
    }
)

if response.status_code == 200:
    with open('周报_两周对比.xlsx', 'wb') as f:
        f.write(response.content)
    print("✅ 周报下载成功")
```

---

## 4. 生成月报

**接口**: `POST /api/reports/monthly`

**请求体**:
```json
{
  "month1_start": "2025-09-01",
  "month1_end": "2025-09-30",
  "month2_start": "2025-10-01",
  "month2_end": "2025-10-31"
}
```

**Python 示例**:
```python
import requests

response = requests.post(
    'http://localhost:5000/api/reports/monthly',
    json={
        'month1_start': '2025-09-01',
        'month1_end': '2025-09-30',
        'month2_start': '2025-10-01',
        'month2_end': '2025-10-31'
    }
)

if response.status_code == 200:
    with open('月报_9月vs10月.xlsx', 'wb') as f:
        f.write(response.content)
    print("✅ 月报下载成功")
```

---

## 5. 生成自定义报表

**接口**: `POST /api/reports/custom`

**请求体**:
```json
{
  "period1_start": "2025-10-25",
  "period1_end": "2025-11-09",
  "period2_start": "2025-11-10",
  "period2_end": "2025-11-25",
  "shop_ids": [1001, 1002, 1003]
}
```

**说明**: `shop_ids` 为可选参数，不传则查询所有门店

**Python 示例 - 所有门店**:
```python
import requests

response = requests.post(
    'http://localhost:5000/api/reports/custom',
    json={
        'period1_start': '2025-10-25',
        'period1_end': '2025-11-09',
        'period2_start': '2025-11-10',
        'period2_end': '2025-11-25'
        # shop_ids 不传，查询所有门店
    }
)

if response.status_code == 200:
    with open('自定义报表_所有门店.xlsx', 'wb') as f:
        f.write(response.content)
    print("✅ 自定义报表下载成功")
```

**Python 示例 - 指定门店**:
```python
import requests

response = requests.post(
    'http://localhost:5000/api/reports/custom',
    json={
        'period1_start': '2025-10-25',
        'period1_end': '2025-11-09',
        'period2_start': '2025-11-10',
        'period2_end': '2025-11-25',
        'shop_ids': [1156694385, 1036063587, 1998417877]  # 指定门店ID
    }
)

if response.status_code == 200:
    with open('自定义报表_指定门店.xlsx', 'wb') as f:
        f.write(response.content)
    print("✅ 自定义报表下载成功")
```

---

## 6. 批量生成报表

**接口**: `POST /api/reports/batch`

**请求体**:
```json
{
  "reports": [
    {
      "type": "daily",
      "params": {
        "report_date": "2025-12-12"
      }
    },
    {
      "type": "weekly",
      "params": {
        "week1_start": "2025-11-10",
        "week1_end": "2025-11-16",
        "week2_start": "2025-11-17",
        "week2_end": "2025-11-23"
      }
    },
    {
      "type": "monthly",
      "params": {
        "month1_start": "2025-09-01",
        "month1_end": "2025-09-30",
        "month2_start": "2025-10-01",
        "month2_end": "2025-10-31"
      }
    }
  ]
}
```

**Python 示例**:
```python
import requests

response = requests.post(
    'http://localhost:5000/api/reports/batch',
    json={
        'reports': [
            {
                'type': 'daily',
                'params': {'report_date': '2025-12-12'}
            },
            {
                'type': 'weekly',
                'params': {
                    'week1_start': '2025-11-10',
                    'week1_end': '2025-11-16',
                    'week2_start': '2025-11-17',
                    'week2_end': '2025-11-23'
                }
            }
        ]
    }
)

result = response.json()
for item in result['results']:
    if item['status'] == 'success':
        print(f"✅ {item['type']} 生成成功: {item['filename']}")
    else:
        print(f"❌ {item['type']} 生成失败: {item['message']}")
```

**返回示例**:
```json
{
  "results": [
    {
      "type": "daily",
      "status": "success",
      "filename": "./reports/日报_2025-12-12.xlsx"
    },
    {
      "type": "weekly",
      "status": "success",
      "filename": "./reports/周报_2025-11-17_to_2025-11-23.xlsx"
    }
  ]
}
```

---

## 错误处理

**常见错误响应**:

### 1. 缺少参数
```json
{
  "error": "缺少参数 report_date"
}
```
HTTP 状态码: 400

### 2. 没有数据
```json
{
  "error": "2025-12-12 没有数据"
}
```
HTTP 状态码: 404

### 3. 服务器错误
```json
{
  "error": "Database connection failed"
}
```
HTTP 状态码: 500

---

## 前端集成示例

### Vue.js 示例

```vue
<template>
  <div>
    <button @click="generateDailyReport">生成日报</button>
    <button @click="generateWeeklyReport">生成周报</button>
  </div>
</template>

<script>
import axios from 'axios';

export default {
  methods: {
    async generateDailyReport() {
      try {
        const response = await axios.post(
          'http://localhost:5000/api/reports/daily',
          { report_date: '2025-12-12' },
          { responseType: 'blob' }
        );

        // 下载文件
        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', '日报_2025-12-12.xlsx');
        document.body.appendChild(link);
        link.click();
        link.remove();

        this.$message.success('日报生成成功');
      } catch (error) {
        this.$message.error('生成失败: ' + error.message);
      }
    },

    async generateWeeklyReport() {
      try {
        const response = await axios.post(
          'http://localhost:5000/api/reports/weekly',
          {
            week1_start: '2025-11-10',
            week1_end: '2025-11-16',
            week2_start: '2025-11-17',
            week2_end: '2025-11-23'
          },
          { responseType: 'blob' }
        );

        // 下载文件
        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', '周报_两周对比.xlsx');
        document.body.appendChild(link);
        link.click();
        link.remove();

        this.$message.success('周报生成成功');
      } catch (error) {
        this.$message.error('生成失败: ' + error.message);
      }
    }
  }
};
</script>
```

### React 示例

```jsx
import React from 'react';
import axios from 'axios';

function ReportGenerator() {
  const generateDailyReport = async () => {
    try {
      const response = await axios.post(
        'http://localhost:5000/api/reports/daily',
        { report_date: '2025-12-12' },
        { responseType: 'blob' }
      );

      // 下载文件
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', '日报_2025-12-12.xlsx');
      document.body.appendChild(link);
      link.click();
      link.remove();

      alert('日报生成成功');
    } catch (error) {
      alert('生成失败: ' + error.message);
    }
  };

  return (
    <div>
      <button onClick={generateDailyReport}>生成日报</button>
    </div>
  );
}

export default ReportGenerator;
```

---

## 部署建议

### 1. 生产环境部署

使用 Gunicorn 或 uWSGI:

```bash
# 安装 Gunicorn
pip install gunicorn

# 启动服务（4个工作进程）
gunicorn -w 4 -b 0.0.0.0:5000 api_server:app
```

### 2. Nginx 反向代理

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location /api/ {
        proxy_pass http://127.0.0.1:5000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### 3. Docker 部署

```dockerfile
FROM python:3.9

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "api_server:app"]
```

---

## 性能优化建议

1. **使用缓存**: 对于相同参数的请求，可以缓存生成的报表文件
2. **异步任务**: 对于大批量报表生成，建议使用 Celery 异步队列
3. **文件清理**: 定期清理过期的报表文件
4. **连接池监控**: 监控数据库连接池使用情况

---

## 安全建议

1. **添加认证**: 使用 JWT 或 API Key 进行身份认证
2. **限流**: 使用 Flask-Limiter 限制请求频率
3. **HTTPS**: 生产环境使用 HTTPS 加密传输
4. **参数验证**: 严格验证用户输入，防止 SQL 注入
