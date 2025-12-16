#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
江鑫数据报表 API 服务
提供 RESTful API 接口供前端调用生成报表
"""

from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import os
from datetime import datetime
from report_generator import (
    generate_daily_report,
    generate_weekly_report,
    generate_monthly_report,
    generate_custom_report
)

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 报表文件存储目录
REPORT_DIR = './reports'
if not os.path.exists(REPORT_DIR):
    os.makedirs(REPORT_DIR)


@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/reports/daily', methods=['POST'])
def api_generate_daily_report():
    """
    生成日报

    请求体 (JSON):
    {
        "report_date": "2025-12-12"
    }

    返回: Excel 文件下载
    """
    try:
        data = request.json
        report_date = data.get('report_date')

        if not report_date:
            return jsonify({'error': '缺少参数 report_date'}), 400

        # 生成报表
        filename = generate_daily_report(
            report_date=report_date,
            output_filename=os.path.join(REPORT_DIR, f'日报_{report_date}.xlsx')
        )

        if filename:
            return send_file(
                filename,
                as_attachment=True,
                download_name=os.path.basename(filename),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify({'error': f'{report_date} 没有数据'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports/weekly', methods=['POST'])
def api_generate_weekly_report():
    """
    生成周报

    请求体 (JSON):
    {
        "week1_start": "2025-11-10",
        "week1_end": "2025-11-16",
        "week2_start": "2025-11-17",
        "week2_end": "2025-11-23"
    }

    返回: Excel 文件下载
    """
    try:
        data = request.json
        week1_start = data.get('week1_start')
        week1_end = data.get('week1_end')
        week2_start = data.get('week2_start')
        week2_end = data.get('week2_end')

        if not all([week1_start, week1_end, week2_start, week2_end]):
            return jsonify({'error': '缺少必要参数'}), 400

        # 生成报表
        filename = generate_weekly_report(
            week1_start=week1_start,
            week1_end=week1_end,
            week2_start=week2_start,
            week2_end=week2_end,
            output_filename=os.path.join(REPORT_DIR, f'周报_{week2_start}_to_{week2_end}.xlsx')
        )

        if filename:
            return send_file(
                filename,
                as_attachment=True,
                download_name=os.path.basename(filename),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify({'error': '没有数据'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports/monthly', methods=['POST'])
def api_generate_monthly_report():
    """
    生成月报

    请求体 (JSON):
    {
        "month1_start": "2025-09-01",
        "month1_end": "2025-09-30",
        "month2_start": "2025-10-01",
        "month2_end": "2025-10-31"
    }

    返回: Excel 文件下载
    """
    try:
        data = request.json
        month1_start = data.get('month1_start')
        month1_end = data.get('month1_end')
        month2_start = data.get('month2_start')
        month2_end = data.get('month2_end')

        if not all([month1_start, month1_end, month2_start, month2_end]):
            return jsonify({'error': '缺少必要参数'}), 400

        # 生成报表
        filename = generate_monthly_report(
            month1_start=month1_start,
            month1_end=month1_end,
            month2_start=month2_start,
            month2_end=month2_end,
            output_filename=os.path.join(REPORT_DIR, f'月报_{month2_start}_to_{month2_end}.xlsx')
        )

        if filename:
            return send_file(
                filename,
                as_attachment=True,
                download_name=os.path.basename(filename),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify({'error': '没有数据'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports/custom', methods=['POST'])
def api_generate_custom_report():
    """
    生成自定义报表

    请求体 (JSON):
    {
        "period1_start": "2025-10-25",
        "period1_end": "2025-11-09",
        "period2_start": "2025-11-10",
        "period2_end": "2025-11-25",
        "shop_ids": [1001, 1002, 1003]  # 可选，不传则查询所有门店
    }

    返回: Excel 文件下载
    """
    try:
        data = request.json
        period1_start = data.get('period1_start')
        period1_end = data.get('period1_end')
        period2_start = data.get('period2_start')
        period2_end = data.get('period2_end')
        shop_ids = data.get('shop_ids')  # 可选参数

        if not all([period1_start, period1_end, period2_start, period2_end]):
            return jsonify({'error': '缺少必要参数'}), 400

        # 生成报表
        filename = generate_custom_report(
            period1_start=period1_start,
            period1_end=period1_end,
            period2_start=period2_start,
            period2_end=period2_end,
            shop_ids=shop_ids,
            output_filename=os.path.join(REPORT_DIR, f'自定义报表_{period2_start}_to_{period2_end}.xlsx')
        )

        if filename:
            return send_file(
                filename,
                as_attachment=True,
                download_name=os.path.basename(filename),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify({'error': '没有数据'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports/batch', methods=['POST'])
def api_generate_batch_reports():
    """
    批量生成报表

    请求体 (JSON):
    {
        "reports": [
            {
                "type": "daily",
                "params": {"report_date": "2025-12-12"}
            },
            {
                "type": "weekly",
                "params": {
                    "week1_start": "2025-11-10",
                    "week1_end": "2025-11-16",
                    "week2_start": "2025-11-17",
                    "week2_end": "2025-11-23"
                }
            }
        ]
    }

    返回: 生成结果列表
    """
    try:
        data = request.json
        reports = data.get('reports', [])

        results = []
        for report_config in reports:
            report_type = report_config.get('type')
            params = report_config.get('params', {})

            try:
                if report_type == 'daily':
                    filename = generate_daily_report(**params)
                elif report_type == 'weekly':
                    filename = generate_weekly_report(**params)
                elif report_type == 'monthly':
                    filename = generate_monthly_report(**params)
                elif report_type == 'custom':
                    filename = generate_custom_report(**params)
                else:
                    results.append({
                        'type': report_type,
                        'status': 'error',
                        'message': f'未知的报表类型: {report_type}'
                    })
                    continue

                if filename:
                    results.append({
                        'type': report_type,
                        'status': 'success',
                        'filename': filename
                    })
                else:
                    results.append({
                        'type': report_type,
                        'status': 'error',
                        'message': '没有数据'
                    })

            except Exception as e:
                results.append({
                    'type': report_type,
                    'status': 'error',
                    'message': str(e)
                })

        return jsonify({'results': results})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("江鑫数据报表 API 服务启动中...")
    print("=" * 60)
    print("\nAPI 端点:")
    print("  - POST /api/reports/daily    - 生成日报")
    print("  - POST /api/reports/weekly   - 生成周报")
    print("  - POST /api/reports/monthly  - 生成月报")
    print("  - POST /api/reports/custom   - 生成自定义报表")
    print("  - POST /api/reports/batch    - 批量生成报表")
    print("  - GET  /api/health           - 健康检查")
    print("\n服务地址: http://0.0.0.0:5000")
    print("=" * 60)

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
