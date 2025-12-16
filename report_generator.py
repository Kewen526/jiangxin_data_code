#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
江鑫数据报表生成系统
支持生成：日报、周报、月报、自定义报表
"""

import mysql.connector
from mysql.connector import pooling
import openpyxl
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ==================== 数据库连接池配置 ====================
DB_CONFIG = {
    'host': '8.146.210.145',
    'port': 3306,
    'user': 'root',
    'password': 'Kewen888@',
    'database': 'jx_data_info',  # 数据库名称
    'charset': 'utf8mb4',
    'use_unicode': True,
    'autocommit': True
}

# 创建全局连接池（单例模式）
CONNECTION_POOL = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="jx_pool",
    pool_size=20,  # 连接池大小
    pool_reset_session=True,
    **DB_CONFIG
)


# ==================== 辅助函数 ====================
def get_shop_info_mapping():
    """
    获取门店信息映射
    返回: dict {shop_id: {'operator': '', 'sales': '', 'city': ''}}
    """
    import json

    conn = CONNECTION_POOL.get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # 1. 查询 platform_accounts 和 saas_users
        sql = """
        SELECT
            pa.account,
            pa.stores_json,
            pa.sales_name,
            pa.city_name,
            pa.operator_id,
            su.name as operator_name
        FROM platform_accounts pa
        LEFT JOIN saas_users su ON pa.operator_id = su.id
        WHERE pa.stores_json IS NOT NULL
        """

        cursor.execute(sql)
        accounts = cursor.fetchall()

        # 2. 解析 stores_json 构建映射
        shop_mapping = {}

        for account in accounts:
            stores_json = account.get('stores_json')
            sales_name = account.get('sales_name', '')
            city_name = account.get('city_name', '')
            operator_name = account.get('operator_name', '')

            if stores_json:
                try:
                    # 解析 JSON（可能是字符串或已经是对象）
                    if isinstance(stores_json, str):
                        stores = json.loads(stores_json)
                    else:
                        stores = stores_json

                    # 遍历门店列表
                    if isinstance(stores, list):
                        for store in stores:
                            if isinstance(store, dict):
                                shop_id = str(store.get('shop_id', ''))
                                if shop_id:
                                    shop_mapping[shop_id] = {
                                        'operator': operator_name or '',
                                        'sales': sales_name or '',
                                        'city': city_name or ''
                                    }
                except (json.JSONDecodeError, TypeError) as e:
                    # 忽略解析错误
                    pass

        return shop_mapping

    finally:
        cursor.close()
        conn.close()


def clean_sheet_name(name, max_length=31):
    """
    清理 Sheet 名称，符合 Excel 规范
    - 最大 31 字符
    - 不能包含: \ / * ? : [ ]
    """
    if not name:
        return "Sheet"

    # 替换非法字符
    illegal_chars = ['\\', '/', '*', '?', ':', '[', ']']
    for char in illegal_chars:
        name = name.replace(char, '')

    # 截断到最大长度
    if len(name) > max_length:
        name = name[:max_length]

    return name or "Sheet"


# ==================== 核心功能：生成日报 ====================
def generate_daily_report(report_date, output_filename=None):
    """
    生成日报（每个门店一个独立的 Sheet）

    参数:
        report_date: str, 报表日期，格式: 'YYYY-MM-DD'
        output_filename: str, 输出文件名，默认自动生成

    返回:
        str: 生成的文件路径
    """
    # 1. 获取门店信息映射
    print("正在加载门店信息...")
    shop_mapping = get_shop_info_mapping()

    # 2. 从连接池获取连接
    conn = CONNECTION_POOL.get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # SQL 查询：关联 kewen_daily_report + promotion_daily_report + store_stats
        sql = """
        SELECT
            k.report_date,
            k.shop_id,
            k.shop_name,
            k.exposure_users,
            k.visit_users,
            k.order_users,
            k.verify_person_count as verify_users,
            k.order_coupon_count,
            k.verify_coupon_count,
            k.promotion_cost,
            k.new_good_review_count,
            k.intent_rate,
            k.order_sale_amount,
            k.verify_sale_amount,
            k.verify_after_discount,
            p.view_phone_count as phone_clicks,
            p.view_address_count as address_clicks,
            s.order_user_rank,
            s.verify_amount_rank
        FROM kewen_daily_report k
        LEFT JOIN promotion_daily_report p
            ON k.shop_id = p.shop_id AND k.report_date = p.report_date
        LEFT JOIN store_stats s
            ON k.shop_id = s.store_id AND k.report_date = s.date
        WHERE k.report_date = %s
        ORDER BY k.shop_id
        """

        cursor.execute(sql, (report_date,))
        rows = cursor.fetchall()

        if not rows:
            print(f"警告：{report_date} 没有数据")
            return None

        # 3. 创建 Excel 工作簿
        wb = openpyxl.Workbook()
        wb.remove(wb.active)  # 删除默认 Sheet

        # 表头定义
        headers = [
            '星期', '日期', '序号', '运营', '城市', '销售', '门店',
            '曝光人数', '访问人数', '下单人数', '核销人数', '下单券数', '核销券数',
            '电话点击', '地址点击', '推广通消耗', '好评', '意向转化率',
            '下单售价金额', '核销售价金额', '优惠后核销金额',
            '下单人数商圈排名', '核销金额商圈排名'
        ]

        # 格式化日期
        date_obj = datetime.strptime(report_date, '%Y-%m-%d')
        weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
        weekday = weekday_names[date_obj.weekday()]
        date_str = date_obj.strftime('%m月%d日')

        # 用于处理重名 Sheet
        sheet_names_used = {}

        # 4. 为每个门店创建一个 Sheet
        for idx, row in enumerate(rows, start=1):
            shop_id = str(row['shop_id'])
            shop_name = row['shop_name'] or f'门店{shop_id}'

            # 从映射中获取运营、城市、销售
            shop_info = shop_mapping.get(shop_id, {})
            operator = shop_info.get('operator', '')
            sales = shop_info.get('sales', '')
            city = shop_info.get('city', '')

            # 清理 Sheet 名称
            sheet_name = clean_sheet_name(shop_name)

            # 处理重名（添加序号）
            if sheet_name in sheet_names_used:
                sheet_names_used[sheet_name] += 1
                sheet_name = f"{sheet_name[:28]}_{sheet_names_used[sheet_name]}"
            else:
                sheet_names_used[sheet_name] = 1

            # 创建 Sheet
            ws = wb.create_sheet(title=sheet_name)

            # 写入表头
            ws.append(headers)

            # 格式化商圈排名
            order_rank = f"第{row['order_user_rank']}名" if row['order_user_rank'] and row['order_user_rank'] < 100 else ("大于100名" if row['order_user_rank'] and row['order_user_rank'] >= 100 else "--")
            verify_rank = f"第{row['verify_amount_rank']}名" if row['verify_amount_rank'] and row['verify_amount_rank'] < 100 else ("大于100名" if row['verify_amount_rank'] and row['verify_amount_rank'] >= 100 else "--")

            # 写入数据行
            data_row = [
                weekday,
                date_str,
                idx,
                operator,
                city,
                sales,
                shop_name,
                row['exposure_users'] or 0,
                row['visit_users'] or 0,
                row['order_users'] or 0,
                row['verify_users'] or 0,
                row['order_coupon_count'] or 0,
                row['verify_coupon_count'] or 0,
                row['phone_clicks'] or 0,
                row['address_clicks'] or 0,
                round(row['promotion_cost'], 2) if row['promotion_cost'] else 0,
                row['new_good_review_count'] or 0,
                row['intent_rate'] or '0%',
                round(row['order_sale_amount'], 2) if row['order_sale_amount'] else 0,
                round(row['verify_sale_amount'], 2) if row['verify_sale_amount'] else 0,
                round(row['verify_after_discount'], 2) if row['verify_after_discount'] else 0,
                order_rank,
                verify_rank
            ]
            ws.append(data_row)

            # 应用样式
            # 表头样式
            header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
            header_font = Font(bold=True, size=10)
            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal='center', vertical='center')

            # 设置列宽
            column_widths = [6, 8, 5, 12, 8, 8, 35, 10, 10, 10, 10, 10, 10, 10, 10, 12, 8, 12, 12, 12, 12, 14, 14]
            for col_idx, width in enumerate(column_widths, start=1):
                ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = width

            # 设置边框和对齐
            thin_border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )

            for row_cells in ws.iter_rows(min_row=1, max_row=2, min_col=1, max_col=len(headers)):
                for cell in row_cells:
                    cell.border = thin_border
                    if cell.row > 1:  # 数据行
                        if cell.column in [1, 2, 4, 5, 6, 7, 22, 23]:  # 文本列
                            cell.alignment = Alignment(horizontal='left', vertical='center')
                        else:  # 数字列
                            cell.alignment = Alignment(horizontal='right', vertical='center')

        # 5. 保存文件
        if not output_filename:
            output_filename = f"日报 非餐 {report_date.replace('-', '')} {datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"

        wb.save(output_filename)
        print(f"✅ 日报生成成功: {output_filename}（共 {len(rows)} 个门店）")
        return output_filename

    finally:
        cursor.close()
        conn.close()


# ==================== 核心功能：生成周报 ====================
def generate_weekly_report(week1_start, week1_end, week2_start, week2_end, output_filename=None):
    """
    生成周报（两周对比）

    参数:
        week1_start: str, 第一周开始日期 'YYYY-MM-DD'
        week1_end: str, 第一周结束日期 'YYYY-MM-DD'
        week2_start: str, 第二周开始日期 'YYYY-MM-DD'
        week2_end: str, 第二周结束日期 'YYYY-MM-DD'
        output_filename: str, 输出文件名
    """
    conn = CONNECTION_POOL.get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # 查询第一周数据
        sql_week = """
        SELECT
            k.shop_id,
            k.shop_name,
            SUM(k.verify_after_discount) as verify_after_discount,
            SUM(k.exposure_users) as exposure_users,
            SUM(k.visit_users) as visit_users,
            SUM(k.order_users) as order_users,
            SUM(k.order_coupon_count) as order_coupon_count,
            SUM(k.verify_person_count) as verify_users,
            SUM(k.verify_coupon_count) as verify_coupon_count,
            SUM(k.order_sale_amount) as order_sale_amount,
            SUM(k.verify_sale_amount) as verify_sale_amount,
            SUM(k.coupon_pay_order_count) as coupon_orders,
            SUM(p.view_phone_count) as phone_clicks,
            SUM(k.promotion_cost) as promotion_cost,
            SUM(k.promotion_exposure_count) as promotion_exposure,
            SUM(k.promotion_click_count) as promotion_clicks,
            SUM(p.order_count) as promotion_orders,
            SUM(p.view_groupbuy_count) as view_groupbuy,
            SUM(p.view_phone_count) as view_phone,
            SUM(k.consult_users) as consult_users,
            SUM(p.view_address_count) as address_clicks,
            SUM(k.new_collect_users) as new_collect,
            SUM(k.new_good_review_count) as new_good_reviews,
            SUM(k.new_review_count) as new_reviews
        FROM kewen_daily_report k
        LEFT JOIN promotion_daily_report p
            ON k.shop_id = p.shop_id AND k.report_date = p.report_date
        WHERE k.report_date BETWEEN %s AND %s
        GROUP BY k.shop_id, k.shop_name
        ORDER BY k.shop_id
        """

        # 获取第一周数据
        cursor.execute(sql_week, (week1_start, week1_end))
        week1_data = {row['shop_id']: row for row in cursor.fetchall()}

        # 获取第二周数据
        cursor.execute(sql_week, (week2_start, week2_end))
        week2_data = {row['shop_id']: row for row in cursor.fetchall()}

        # 获取所有门店列表
        all_shop_ids = set(week1_data.keys()) | set(week2_data.keys())

        if not all_shop_ids:
            print("警告：没有找到数据")
            return None

        # 创建 Excel
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "周报"

        # 格式化日期周期
        week1_period = f"{datetime.strptime(week1_start, '%Y-%m-%d').strftime('%Y.%m.%d')}-{datetime.strptime(week1_end, '%Y-%m-%d').strftime('%Y.%m.%d')}"
        week2_period = f"{datetime.strptime(week2_start, '%Y-%m-%d').strftime('%Y.%m.%d')}-{datetime.strptime(week2_end, '%Y-%m-%d').strftime('%Y.%m.%d')}"

        # 为每个门店生成 8 行数据
        for shop_id in sorted(all_shop_ids):
            w1 = week1_data.get(shop_id, {})
            w2 = week2_data.get(shop_id, {})
            shop_name = w2.get('shop_name') or w1.get('shop_name', '未知门店')

            # 获取数据（处理缺失值）
            def get_val(data, key, default=0):
                return data.get(key, default) if data else default

            # 计算转化率和比率
            def calc_rate(numerator, denominator):
                if denominator and denominator > 0:
                    return f"{int(round(numerator / denominator * 100))}%"
                return "0%"

            def calc_avg_price(total, count):
                if count and count > 0:
                    return round(total / count, 2)
                return 0

            # 第一周指标
            w1_verify_discount = get_val(w1, 'verify_after_discount')
            w1_exposure = get_val(w1, 'exposure_users')
            w1_visit = get_val(w1, 'visit_users')
            w1_order_users = get_val(w1, 'order_users')
            w1_order_coupons = get_val(w1, 'order_coupon_count')
            w1_verify_users = get_val(w1, 'verify_users')
            w1_verify_coupons = get_val(w1, 'verify_coupon_count')
            w1_order_amount = get_val(w1, 'order_sale_amount')
            w1_verify_amount = get_val(w1, 'verify_sale_amount')
            w1_coupon_orders = get_val(w1, 'coupon_orders')
            w1_phone_clicks = get_val(w1, 'phone_clicks')

            w1_exposure_rate = calc_rate(w1_visit, w1_exposure)
            w1_order_rate = calc_rate(w1_order_users, w1_visit)
            w1_avg_price = calc_avg_price(w1_verify_discount, w1_verify_users)

            # 第二周指标
            w2_verify_discount = get_val(w2, 'verify_after_discount')
            w2_exposure = get_val(w2, 'exposure_users')
            w2_visit = get_val(w2, 'visit_users')
            w2_order_users = get_val(w2, 'order_users')
            w2_order_coupons = get_val(w2, 'order_coupon_count')
            w2_verify_users = get_val(w2, 'verify_users')
            w2_verify_coupons = get_val(w2, 'verify_coupon_count')
            w2_order_amount = get_val(w2, 'order_sale_amount')
            w2_verify_amount = get_val(w2, 'verify_sale_amount')
            w2_coupon_orders = get_val(w2, 'coupon_orders')
            w2_phone_clicks = get_val(w2, 'phone_clicks')

            w2_exposure_rate = calc_rate(w2_visit, w2_exposure)
            w2_order_rate = calc_rate(w2_order_users, w2_visit)
            w2_avg_price = calc_avg_price(w2_verify_discount, w2_verify_users)

            # 计算差值
            diff_verify_discount = round(w2_verify_discount - w1_verify_discount, 2)
            diff_exposure = w2_exposure - w1_exposure
            diff_visit = w2_visit - w1_visit
            diff_order_users = w2_order_users - w1_order_users
            diff_order_coupons = w2_order_coupons - w1_order_coupons
            diff_verify_users = w2_verify_users - w1_verify_users
            diff_verify_coupons = w2_verify_coupons - w1_verify_coupons
            diff_order_amount = round(w2_order_amount - w1_order_amount, 2)
            diff_verify_amount = round(w2_verify_amount - w1_verify_amount, 2)
            diff_coupon_orders = w2_coupon_orders - w1_coupon_orders
            diff_phone_clicks = w2_phone_clicks - w1_phone_clicks
            diff_avg_price = round(w2_avg_price - w1_avg_price, 2)

            # 差值百分比
            def calc_rate_diff(rate1, rate2):
                val1 = int(rate1.rstrip('%')) if rate1 != '0%' else 0
                val2 = int(rate2.rstrip('%')) if rate2 != '0%' else 0
                return f"{val2 - val1}%"

            diff_exposure_rate = calc_rate_diff(w1_exposure_rate, w2_exposure_rate)
            diff_order_rate = calc_rate_diff(w1_order_rate, w2_order_rate)

            # === 第 1-3 行：核销相关数据 ===
            # 行1: 第一周核销数据
            row1 = [
                shop_name, week1_period,
                round(w1_verify_discount, 2), w1_exposure, w1_visit, w1_exposure_rate,
                w1_order_users, w1_order_coupons, w1_order_rate,
                w1_verify_users, w1_verify_coupons,
                round(w1_order_amount, 2), round(w1_verify_amount, 2),
                w1_coupon_orders, w1_phone_clicks, w1_avg_price
            ]
            ws.append(row1)

            # 行2: 第二周核销数据
            row2 = [
                shop_name, week2_period,
                round(w2_verify_discount, 2), w2_exposure, w2_visit, w2_exposure_rate,
                w2_order_users, w2_order_coupons, w2_order_rate,
                w2_verify_users, w2_verify_coupons,
                round(w2_order_amount, 2), round(w2_verify_amount, 2),
                w2_coupon_orders, w2_phone_clicks, w2_avg_price
            ]
            ws.append(row2)

            # 行3: 差值
            row3 = [
                shop_name, '差值',
                diff_verify_discount, diff_exposure, diff_visit, diff_exposure_rate,
                diff_order_users, diff_order_coupons, diff_order_rate,
                diff_verify_users, diff_verify_coupons,
                diff_order_amount, diff_verify_amount,
                diff_coupon_orders, diff_phone_clicks, diff_avg_price
            ]
            ws.append(row3)

            # === 第 4 行：推广通表头（重复表头）===
            header_row = [
                '门店', '数据周期', '推广通花费', '推广通曝光', '推广通点击', '推广通点击均价',
                '推广通订单量', '推广通下单转化率', '推广通查看团购', '推广通查看电话',
                '在线咨询', '地址点击', '门店收藏', '收藏率', '新增好评数', '留评率'
            ]
            ws.append(header_row)

            # === 第 5-7 行：推广通数据 ===
            # 推广通相关数据
            w1_promo_cost = get_val(w1, 'promotion_cost')
            w1_promo_exposure = get_val(w1, 'promotion_exposure')
            w1_promo_clicks = get_val(w1, 'promotion_clicks')
            w1_promo_orders = get_val(w1, 'promotion_orders')
            w1_view_groupbuy = get_val(w1, 'view_groupbuy')
            w1_view_phone = get_val(w1, 'view_phone')
            w1_consult = get_val(w1, 'consult_users')
            w1_address = get_val(w1, 'address_clicks')
            w1_collect = get_val(w1, 'new_collect')
            w1_good_reviews = get_val(w1, 'new_good_reviews')
            w1_reviews = get_val(w1, 'new_reviews')

            w1_click_price = calc_avg_price(w1_promo_cost, w1_promo_clicks)
            w1_promo_rate = calc_rate(w1_promo_orders, w1_promo_clicks)
            w1_collect_rate = calc_rate(w1_collect, w1_visit)
            w1_review_rate = calc_rate(w1_good_reviews, w1_verify_users)

            w2_promo_cost = get_val(w2, 'promotion_cost')
            w2_promo_exposure = get_val(w2, 'promotion_exposure')
            w2_promo_clicks = get_val(w2, 'promotion_clicks')
            w2_promo_orders = get_val(w2, 'promotion_orders')
            w2_view_groupbuy = get_val(w2, 'view_groupbuy')
            w2_view_phone = get_val(w2, 'view_phone')
            w2_consult = get_val(w2, 'consult_users')
            w2_address = get_val(w2, 'address_clicks')
            w2_collect = get_val(w2, 'new_collect')
            w2_good_reviews = get_val(w2, 'new_good_reviews')
            w2_reviews = get_val(w2, 'new_reviews')

            w2_click_price = calc_avg_price(w2_promo_cost, w2_promo_clicks)
            w2_promo_rate = calc_rate(w2_promo_orders, w2_promo_clicks)
            w2_collect_rate = calc_rate(w2_collect, w2_visit)
            w2_review_rate = calc_rate(w2_good_reviews, w2_verify_users)

            # 差值
            diff_promo_cost = round(w2_promo_cost - w1_promo_cost, 2)
            diff_promo_exposure = w2_promo_exposure - w1_promo_exposure
            diff_promo_clicks = w2_promo_clicks - w1_promo_clicks
            diff_click_price = round(w2_click_price - w1_click_price, 2)
            diff_promo_orders = w2_promo_orders - w1_promo_orders
            diff_promo_rate = calc_rate_diff(w1_promo_rate, w2_promo_rate)
            diff_view_groupbuy = w2_view_groupbuy - w1_view_groupbuy
            diff_view_phone = w2_view_phone - w1_view_phone
            diff_consult = w2_consult - w1_consult
            diff_address = w2_address - w1_address
            diff_collect = w2_collect - w1_collect
            diff_collect_rate = calc_rate_diff(w1_collect_rate, w2_collect_rate)
            diff_good_reviews = w2_good_reviews - w1_good_reviews
            diff_review_rate = calc_rate_diff(w1_review_rate, w2_review_rate)

            # 行5: 第一周推广通数据
            row5 = [
                shop_name, week1_period,
                round(w1_promo_cost, 2), w1_promo_exposure, w1_promo_clicks, w1_click_price,
                w1_promo_orders, w1_promo_rate, w1_view_groupbuy, w1_view_phone,
                w1_consult, w1_address, w1_collect, w1_collect_rate,
                w1_good_reviews, w1_review_rate
            ]
            ws.append(row5)

            # 行6: 第二周推广通数据
            row6 = [
                shop_name, week2_period,
                round(w2_promo_cost, 2), w2_promo_exposure, w2_promo_clicks, w2_click_price,
                w2_promo_orders, w2_promo_rate, w2_view_groupbuy, w2_view_phone,
                w2_consult, w2_address, w2_collect, w2_collect_rate,
                w2_good_reviews, w2_review_rate
            ]
            ws.append(row6)

            # 行7: 推广通差值
            row7 = [
                shop_name, '差值',
                diff_promo_cost, diff_promo_exposure, diff_promo_clicks, diff_click_price,
                diff_promo_orders, diff_promo_rate, diff_view_groupbuy, diff_view_phone,
                diff_consult, diff_address, diff_collect, diff_collect_rate,
                diff_good_reviews, diff_review_rate
            ]
            ws.append(row7)

        # 应用样式
        # 设置列宽
        for i in range(1, 17):
            ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 15
        ws.column_dimensions['A'].width = 40  # 门店名称列

        # 设置边框和对齐
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=16):
            for cell in row:
                cell.border = thin_border
                cell.alignment = Alignment(horizontal='center', vertical='center')

                # 差值行用灰色背景
                if cell.column == 2 and cell.value == '差值':
                    for c in row:
                        c.fill = PatternFill(start_color="F0F0F0", end_color="F0F0F0", fill_type="solid")

                # 表头行（推广通）加粗
                if cell.column == 1 and cell.value == '门店' and '推广通' in str(ws.cell(cell.row, 3).value or ''):
                    for c in row:
                        c.font = Font(bold=True, size=10)
                        c.fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")

        # 保存文件
        if not output_filename:
            output_filename = f"周报 非餐 {week2_start.replace('-', '')}~{week2_end.replace('-', '')} {datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"

        wb.save(output_filename)
        print(f"✅ 周报生成成功: {output_filename}")
        return output_filename

    finally:
        cursor.close()
        conn.close()


# ==================== 核心功能：生成月报 ====================
def generate_monthly_report(month1_start, month1_end, month2_start, month2_end, output_filename=None):
    """
    生成月报（两个月对比）
    逻辑与周报完全相同，只是时间跨度从周变为月
    """
    # 直接复用周报的逻辑，只是参数名不同
    return generate_weekly_report(month1_start, month1_end, month2_start, month2_end, output_filename)


# ==================== 核心功能：生成自定义报表 ====================
def generate_custom_report(period1_start, period1_end, period2_start, period2_end, shop_ids=None, output_filename=None):
    """
    生成自定义报表（两个自定义时间段对比，支持筛选门店）

    参数:
        period1_start: str, 第一个时期开始日期
        period1_end: str, 第一个时期结束日期
        period2_start: str, 第二个时期开始日期
        period2_end: str, 第二个时期结束日期
        shop_ids: list, 门店ID列表，为空则查询所有门店
        output_filename: str, 输出文件名
    """
    # 1. 获取门店信息映射
    print("正在加载门店信息...")
    shop_mapping = get_shop_info_mapping()

    # 2. 从连接池获取连接
    conn = CONNECTION_POOL.get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # 构建 SQL，支持门店筛选
        shop_filter = ""
        if shop_ids:
            shop_filter = f"AND k.shop_id IN ({','.join(map(str, shop_ids))})"

        sql_period = f"""
        SELECT
            k.shop_id,
            k.shop_name,
            k.city,
            SUM(k.verify_after_discount) as verify_after_discount,
            SUM(k.exposure_users) as exposure_users,
            SUM(k.visit_users) as visit_users,
            SUM(k.order_users) as order_users,
            SUM(k.order_coupon_count) as order_coupon_count,
            SUM(k.verify_person_count) as verify_users,
            SUM(k.verify_coupon_count) as verify_coupon_count,
            SUM(k.order_sale_amount) as order_sale_amount,
            SUM(k.verify_sale_amount) as verify_sale_amount,
            SUM(k.coupon_pay_order_count) as coupon_orders,
            SUM(p.view_phone_count) as phone_clicks,
            SUM(k.promotion_cost) as promotion_cost,
            SUM(k.promotion_exposure_count) as promotion_exposure,
            SUM(k.promotion_click_count) as promotion_clicks,
            SUM(p.order_count) as promotion_orders,
            SUM(p.view_groupbuy_count) as view_groupbuy,
            SUM(p.view_phone_count) as view_phone,
            SUM(k.consult_users) as consult_users,
            SUM(p.view_address_count) as address_clicks,
            SUM(k.new_collect_users) as new_collect,
            SUM(k.new_good_review_count) as new_good_reviews,
            SUM(k.new_review_count) as new_reviews
        FROM kewen_daily_report k
        LEFT JOIN promotion_daily_report p
            ON k.shop_id = p.shop_id AND k.report_date = p.report_date
        WHERE k.report_date BETWEEN %s AND %s {shop_filter}
        GROUP BY k.shop_id, k.shop_name, k.city
        ORDER BY k.shop_id
        """

        # 获取两个时期的数据
        cursor.execute(sql_period, (period1_start, period1_end))
        period1_data = {row['shop_id']: row for row in cursor.fetchall()}

        cursor.execute(sql_period, (period2_start, period2_end))
        period2_data = {row['shop_id']: row for row in cursor.fetchall()}

        all_shop_ids = set(period1_data.keys()) | set(period2_data.keys())

        if not all_shop_ids:
            print("警告：没有找到数据")
            return None

        # 创建 Excel
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "自定义报表"

        # 格式化日期周期
        period1_str = f"{datetime.strptime(period1_start, '%Y-%m-%d').strftime('%Y.%m.%d')}-{datetime.strptime(period1_end, '%Y-%m-%d').strftime('%Y.%m.%d')}"
        period2_str = f"{datetime.strptime(period2_start, '%Y-%m-%d').strftime('%Y.%m.%d')}-{datetime.strptime(period2_end, '%Y-%m-%d').strftime('%Y.%m.%d')}"

        # 为每个门店生成数据（与周报类似，但增加额外字段）
        seq_num = 1
        for shop_id in sorted(all_shop_ids):
            p1 = period1_data.get(shop_id, {})
            p2 = period2_data.get(shop_id, {})
            shop_name = p2.get('shop_name') or p1.get('shop_name', '未知门店')

            # 从映射中获取运营、城市、销售
            shop_id_str = str(shop_id)
            shop_info = shop_mapping.get(shop_id_str, {})
            operator = shop_info.get('operator', '')
            sales = shop_info.get('sales', '')
            city = shop_info.get('city', '')

            def get_val(data, key, default=0):
                return data.get(key, default) if data else default

            def calc_rate(numerator, denominator):
                if denominator and denominator > 0:
                    return f"{int(round(numerator / denominator * 100))}%"
                return "0%"

            def calc_avg_price(total, count):
                if count and count > 0:
                    return round(total / count, 2)
                return 0

            # 计算所有指标（与周报相同）
            p1_verify_discount = get_val(p1, 'verify_after_discount')
            p1_exposure = get_val(p1, 'exposure_users')
            p1_visit = get_val(p1, 'visit_users')
            p1_order_users = get_val(p1, 'order_users')
            p1_order_coupons = get_val(p1, 'order_coupon_count')
            p1_verify_users = get_val(p1, 'verify_users')
            p1_verify_coupons = get_val(p1, 'verify_coupon_count')
            p1_order_amount = get_val(p1, 'order_sale_amount')
            p1_verify_amount = get_val(p1, 'verify_sale_amount')
            p1_coupon_orders = get_val(p1, 'coupon_orders')
            p1_phone_clicks = get_val(p1, 'phone_clicks')
            p1_exposure_rate = calc_rate(p1_visit, p1_exposure)
            p1_order_rate = calc_rate(p1_order_users, p1_visit)
            p1_avg_price = calc_avg_price(p1_verify_discount, p1_verify_users)

            p2_verify_discount = get_val(p2, 'verify_after_discount')
            p2_exposure = get_val(p2, 'exposure_users')
            p2_visit = get_val(p2, 'visit_users')
            p2_order_users = get_val(p2, 'order_users')
            p2_order_coupons = get_val(p2, 'order_coupon_count')
            p2_verify_users = get_val(p2, 'verify_users')
            p2_verify_coupons = get_val(p2, 'verify_coupon_count')
            p2_order_amount = get_val(p2, 'order_sale_amount')
            p2_verify_amount = get_val(p2, 'verify_sale_amount')
            p2_coupon_orders = get_val(p2, 'coupon_orders')
            p2_phone_clicks = get_val(p2, 'phone_clicks')
            p2_exposure_rate = calc_rate(p2_visit, p2_exposure)
            p2_order_rate = calc_rate(p2_order_users, p2_visit)
            p2_avg_price = calc_avg_price(p2_verify_discount, p2_verify_users)

            # 差值计算
            diff_verify_discount = round(p2_verify_discount - p1_verify_discount, 2)
            diff_exposure = p2_exposure - p1_exposure
            diff_visit = p2_visit - p1_visit

            def calc_rate_diff(rate1, rate2):
                val1 = int(rate1.rstrip('%')) if rate1 != '0%' else 0
                val2 = int(rate2.rstrip('%')) if rate2 != '0%' else 0
                return f"{val2 - val1}%"

            diff_exposure_rate = calc_rate_diff(p1_exposure_rate, p2_exposure_rate)
            diff_order_users = p2_order_users - p1_order_users
            diff_order_coupons = p2_order_coupons - p1_order_coupons
            diff_order_rate = calc_rate_diff(p1_order_rate, p2_order_rate)
            diff_verify_users = p2_verify_users - p1_verify_users
            diff_verify_coupons = p2_verify_coupons - p1_verify_coupons
            diff_order_amount = round(p2_order_amount - p1_order_amount, 2)
            diff_verify_amount = round(p2_verify_amount - p1_verify_amount, 2)
            diff_coupon_orders = p2_coupon_orders - p1_coupon_orders
            diff_phone_clicks = p2_phone_clicks - p1_phone_clicks
            diff_avg_price = round(p2_avg_price - p1_avg_price, 2)

            # 推广通数据
            p1_promo_cost = get_val(p1, 'promotion_cost')
            p1_promo_exposure = get_val(p1, 'promotion_exposure')
            p1_promo_clicks = get_val(p1, 'promotion_clicks')
            p1_promo_orders = get_val(p1, 'promotion_orders')
            p1_view_groupbuy = get_val(p1, 'view_groupbuy')
            p1_view_phone = get_val(p1, 'view_phone')
            p1_consult = get_val(p1, 'consult_users')
            p1_address = get_val(p1, 'address_clicks')
            p1_collect = get_val(p1, 'new_collect')
            p1_good_reviews = get_val(p1, 'new_good_reviews')
            p1_click_price = calc_avg_price(p1_promo_cost, p1_promo_clicks)
            p1_promo_rate = calc_rate(p1_promo_orders, p1_promo_clicks)
            p1_collect_rate = calc_rate(p1_collect, p1_visit)
            p1_review_rate = calc_rate(p1_good_reviews, p1_verify_users)

            p2_promo_cost = get_val(p2, 'promotion_cost')
            p2_promo_exposure = get_val(p2, 'promotion_exposure')
            p2_promo_clicks = get_val(p2, 'promotion_clicks')
            p2_promo_orders = get_val(p2, 'promotion_orders')
            p2_view_groupbuy = get_val(p2, 'view_groupbuy')
            p2_view_phone = get_val(p2, 'view_phone')
            p2_consult = get_val(p2, 'consult_users')
            p2_address = get_val(p2, 'address_clicks')
            p2_collect = get_val(p2, 'new_collect')
            p2_good_reviews = get_val(p2, 'new_good_reviews')
            p2_click_price = calc_avg_price(p2_promo_cost, p2_promo_clicks)
            p2_promo_rate = calc_rate(p2_promo_orders, p2_promo_clicks)
            p2_collect_rate = calc_rate(p2_collect, p2_visit)
            p2_review_rate = calc_rate(p2_good_reviews, p2_verify_users)

            diff_promo_cost = round(p2_promo_cost - p1_promo_cost, 2)
            diff_promo_exposure = p2_promo_exposure - p1_promo_exposure
            diff_promo_clicks = p2_promo_clicks - p1_promo_clicks
            diff_click_price = round(p2_click_price - p1_click_price, 2)
            diff_promo_orders = p2_promo_orders - p1_promo_orders
            diff_promo_rate = calc_rate_diff(p1_promo_rate, p2_promo_rate)
            diff_view_groupbuy = p2_view_groupbuy - p1_view_groupbuy
            diff_view_phone = p2_view_phone - p1_view_phone
            diff_consult = p2_consult - p1_consult
            diff_address = p2_address - p1_address
            diff_collect = p2_collect - p1_collect
            diff_collect_rate = calc_rate_diff(p1_collect_rate, p2_collect_rate)
            diff_good_reviews = p2_good_reviews - p1_good_reviews
            diff_review_rate = calc_rate_diff(p1_review_rate, p2_review_rate)

            # 第一行：时期1的核销数据（34列）
            row1 = [
                seq_num, operator, city, sales, shop_name, period1_str,
                round(p1_verify_discount, 2), p1_exposure, p1_visit, p1_exposure_rate,
                p1_order_users, p1_order_coupons, p1_order_rate,
                p1_verify_users, p1_verify_coupons,
                round(p1_order_amount, 2), round(p1_verify_amount, 2),
                p1_coupon_orders, p1_phone_clicks, p1_avg_price,
                round(p1_promo_cost, 2), p1_promo_exposure, p1_promo_clicks, p1_click_price,
                p1_promo_orders, p1_promo_rate, p1_view_groupbuy, p1_view_phone,
                p1_consult, p1_address, p1_collect, p1_collect_rate,
                p1_good_reviews, p1_review_rate
            ]
            ws.append(row1)

            # 第二行：时期2的数据
            row2 = [
                seq_num, operator, city, sales, shop_name, period2_str,
                round(p2_verify_discount, 2), p2_exposure, p2_visit, p2_exposure_rate,
                p2_order_users, p2_order_coupons, p2_order_rate,
                p2_verify_users, p2_verify_coupons,
                round(p2_order_amount, 2), round(p2_verify_amount, 2),
                p2_coupon_orders, p2_phone_clicks, p2_avg_price,
                round(p2_promo_cost, 2), p2_promo_exposure, p2_promo_clicks, p2_click_price,
                p2_promo_orders, p2_promo_rate, p2_view_groupbuy, p2_view_phone,
                p2_consult, p2_address, p2_collect, p2_collect_rate,
                p2_good_reviews, p2_review_rate
            ]
            ws.append(row2)

            # 第三行：差值
            row3 = [
                seq_num, operator, city, sales, shop_name, '差值',
                diff_verify_discount, diff_exposure, diff_visit, diff_exposure_rate,
                diff_order_users, diff_order_coupons, diff_order_rate,
                diff_verify_users, diff_verify_coupons,
                diff_order_amount, diff_verify_amount,
                diff_coupon_orders, diff_phone_clicks, diff_avg_price,
                diff_promo_cost, diff_promo_exposure, diff_promo_clicks, diff_click_price,
                diff_promo_orders, diff_promo_rate, diff_view_groupbuy, diff_view_phone,
                diff_consult, diff_address, diff_collect, diff_collect_rate,
                diff_good_reviews, diff_review_rate
            ]
            ws.append(row3)

            # 第四行：表头（重复）
            header = [
                '序号', '运营', '城市', '销售', '门店', '数据周期',
                '优惠后核销额', '曝光人数', '访问人数', '曝光访问转化率',
                '下单人数', '下单券数', '下单转化率', '核销人数', '核销券数',
                '下单售价金额', '核销售价金额', '优惠码订单', '电话点击', '客单价',
                '推广通花费', '推广通曝光', '推广通点击', '推广通点击均价',
                '推广通订单量', '推广通下单转化率', '推广通查看团购', '推广通查看电话',
                '在线咨询', '地址点击', '门店收藏', '收藏率', '新增好评数', '留评率'
            ]
            ws.append(header)

            seq_num += 1

        # 应用样式
        for i in range(1, 35):
            ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 12
        ws.column_dimensions['E'].width = 40  # 门店名称列

        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=34):
            for cell in row:
                cell.border = thin_border
                cell.alignment = Alignment(horizontal='center', vertical='center')

                # 差值行灰色背景
                if cell.column == 6 and cell.value == '差值':
                    for c in row:
                        c.fill = PatternFill(start_color="F0F0F0", end_color="F0F0F0", fill_type="solid")

                # 表头行加粗
                if cell.value == '序号' and ws.cell(cell.row, 2).value == '运营':
                    for c in row:
                        c.font = Font(bold=True, size=10)
                        c.fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")

        # 保存文件
        if not output_filename:
            shop_count = len(all_shop_ids)
            output_filename = f"自定义 {shop_count}家门店非餐 {period2_start.replace('-', '')}~{period2_end.replace('-', '')} {datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"

        wb.save(output_filename)
        print(f"✅ 自定义报表生成成功: {output_filename}")
        return output_filename

    finally:
        cursor.close()
        conn.close()


# ==================== 主程序示例 ====================
if __name__ == "__main__":
    print("=" * 60)
    print("江鑫数据报表生成系统")
    print("=" * 60)

    # 示例：生成日报
    print("\n【示例1】生成日报")
    try:
        generate_daily_report('2025-12-12')
    except Exception as e:
        print(f"❌ 日报生成失败: {e}")

    # 示例：生成周报
    print("\n【示例2】生成周报")
    try:
        generate_weekly_report(
            week1_start='2025-11-10',
            week1_end='2025-11-16',
            week2_start='2025-11-17',
            week2_end='2025-11-23'
        )
    except Exception as e:
        print(f"❌ 周报生成失败: {e}")

    # 示例：生成月报
    print("\n【示例3】生成月报")
    try:
        generate_monthly_report(
            month1_start='2025-09-01',
            month1_end='2025-09-30',
            month2_start='2025-10-01',
            month2_end='2025-10-31'
        )
    except Exception as e:
        print(f"❌ 月报生成失败: {e}")

    # 示例：生成自定义报表
    print("\n【示例4】生成自定义报表")
    try:
        generate_custom_report(
            period1_start='2025-10-25',
            period1_end='2025-11-09',
            period2_start='2025-11-10',
            period2_end='2025-11-25',
            shop_ids=None  # None表示所有门店，也可以传入 [shop_id1, shop_id2, ...]
        )
    except Exception as e:
        print(f"❌ 自定义报表生成失败: {e}")

    print("\n" + "=" * 60)
    print("所有报表生成完成！")
    print("=" * 60)
