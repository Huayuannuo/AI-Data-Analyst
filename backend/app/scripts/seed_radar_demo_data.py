"""
雷达行业 Demo 数据初始化脚本（适配比赛演示）

特点：
1. 仅清理并重建“雷达行业”相关数据，不影响其它行业
2. 三张核心表同时填充：industry_stats / company_data / policy_data
3. 提供 source/source_url/notes，方便流程展示和可追溯

用法：
    cd backend/app
    python3 scripts/seed_radar_demo_data.py
"""

import sys
import os
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import SessionLocal, engine, Base
from models.industry_data import IndustryStats, CompanyData, PolicyData


RADAR_INDUSTRY_NAME = "雷达"

SOURCE_ANCHORS = {
    "mmwave_china_scale": "https://www.chinabgao.com/info/1248635.html",
    "auto_mmwave_2024": "https://www.eefocus.com/article/1774960.html",
    "mmwave_forecast": "https://stock.finance.sina.com.cn/stock/go.php/vReport_Show/kind/search/rptid/730456751257/index.phtml",
    "policy_miit": "https://www.miit.gov.cn",
    "policy_mot": "https://www.mot.gov.cn",
    "policy_ndrc": "https://www.ndrc.gov.cn",
}


def seed_industry_stats(db):
    """插入行业统计数据（约 40+ 条）"""
    rows = []

    # 年度规模（亿元）- 演示口径，锚定公开研报区间
    annual_market_size = {
        2020: 52.0,
        2021: 69.0,
        2022: 79.0,
        2023: 92.0,
        2024: 108.0,
        2025: 129.0,
        2026: 152.0,
    }

    for y, v in annual_market_size.items():
        rows.append(
            IndustryStats(
                industry_name=RADAR_INDUSTRY_NAME,
                metric_name="市场规模",
                metric_value=v,
                unit="亿元",
                year=y,
                region="全国",
                source="公开研报口径汇总",
                source_url=SOURCE_ANCHORS["mmwave_china_scale"],
                notes="演示模拟数据（依据公开行业报告区间拟合）",
            )
        )

    # 同比
    years = sorted(annual_market_size.keys())
    for i in range(1, len(years)):
        prev_y, y = years[i - 1], years[i]
        growth = (annual_market_size[y] - annual_market_size[prev_y]) / annual_market_size[prev_y] * 100
        rows.append(
            IndustryStats(
                industry_name=RADAR_INDUSTRY_NAME,
                metric_name="同比增长率",
                metric_value=round(growth, 2),
                unit="%",
                year=y,
                region="全国",
                source="脚本计算",
                source_url=SOURCE_ANCHORS["mmwave_forecast"],
                notes=f"按市场规模({prev_y}->{y})计算",
            )
        )

    # 装配量（车载毫米波雷达，万颗）
    install_volume = {
        2021: 2000,
        2022: 2350,
        2023: 2780,
        2024: 3200,
        2025: 3650,
        2026: 4200,
    }
    for y, v in install_volume.items():
        rows.append(
            IndustryStats(
                industry_name=RADAR_INDUSTRY_NAME,
                metric_name="车载毫米波雷达装配量",
                metric_value=v,
                unit="万颗",
                year=y,
                region="全国",
                source="公开研报口径汇总",
                source_url=SOURCE_ANCHORS["auto_mmwave_2024"],
                notes="演示模拟数据",
            )
        )

    # 细分赛道（2024-2026）
    segment_template = {
        "车载毫米波雷达市场规模": {2024: 60.0, 2025: 74.0, 2026: 88.0},
        "低空安防雷达市场规模": {2024: 18.0, 2025: 24.0, 2026: 31.0},
        "海事监测雷达市场规模": {2024: 11.5, 2025: 13.8, 2026: 16.2},
        "气象雷达市场规模": {2024: 8.8, 2025: 10.2, 2026: 12.1},
    }
    for metric, yearly in segment_template.items():
        for y, v in yearly.items():
            rows.append(
                IndustryStats(
                    industry_name=RADAR_INDUSTRY_NAME,
                    metric_name=metric,
                    metric_value=v,
                    unit="亿元",
                    year=y,
                    region="全国",
                    source="公开信息测算",
                    source_url=SOURCE_ANCHORS["mmwave_china_scale"],
                    notes="演示模拟数据",
                )
            )

    # 区域结构（2025）
    region_2025 = {
        "华东地区": 43.0,
        "华南地区": 26.0,
        "华北地区": 22.0,
        "西南地区": 18.0,
        "华中地区": 12.0,
        "东北地区": 8.0,
    }
    for region, v in region_2025.items():
        rows.append(
            IndustryStats(
                industry_name=RADAR_INDUSTRY_NAME,
                metric_name="市场规模",
                metric_value=v,
                unit="亿元",
                year=2025,
                region=region,
                source="区域口径测算",
                source_url=SOURCE_ANCHORS["mmwave_forecast"],
                notes="演示模拟数据",
            )
        )

    # 2025 季度行业营收（亿元）
    quarter_revenue_2025 = {1: 28.4, 2: 31.2, 3: 33.5, 4: 36.1}
    for q, v in quarter_revenue_2025.items():
        rows.append(
            IndustryStats(
                industry_name=RADAR_INDUSTRY_NAME,
                metric_name="行业营收",
                metric_value=v,
                unit="亿元",
                year=2025,
                quarter=q,
                region="全国",
                source="行业口径估算",
                source_url=SOURCE_ANCHORS["auto_mmwave_2024"],
                notes="演示模拟数据",
            )
        )

    db.add_all(rows)
    db.commit()
    print(f"✓ 插入 {len(rows)} 条行业统计数据")


def seed_company_data(db):
    """插入企业季度数据（约 30+ 条）"""
    companies = [
        {
            "company_name": "纳睿雷达",
            "stock_code": "688522.SH",
            "sub_industry": "气象雷达",
            "base_revenue_q3_2025": 4.8,
            "net_margin": 0.18,
            "gross_margin": 42.0,
            "market_share_2025": 6.2,
            "employees": 850,
        },
        {
            "company_name": "国睿科技",
            "stock_code": "600562.SH",
            "sub_industry": "军工雷达/气象雷达",
            "base_revenue_q3_2025": 18.6,
            "net_margin": 0.15,
            "gross_margin": 36.5,
            "market_share_2025": 9.8,
            "employees": 4200,
        },
        {
            "company_name": "四创电子",
            "stock_code": "600990.SH",
            "sub_industry": "气象雷达/安防雷达",
            "base_revenue_q3_2025": 11.2,
            "net_margin": 0.09,
            "gross_margin": 31.8,
            "market_share_2025": 7.1,
            "employees": 3600,
        },
        {
            "company_name": "森思泰克",
            "stock_code": "未上市",
            "sub_industry": "车载毫米波雷达",
            "base_revenue_q3_2025": 6.4,
            "net_margin": 0.07,
            "gross_margin": 28.2,
            "market_share_2025": 4.5,
            "employees": 1200,
        },
        {
            "company_name": "承泰科技",
            "stock_code": "未上市",
            "sub_industry": "车载毫米波雷达",
            "base_revenue_q3_2025": 5.1,
            "net_margin": 0.05,
            "gross_margin": 25.6,
            "market_share_2025": 3.9,
            "employees": 980,
        },
        {
            "company_name": "楚航科技",
            "stock_code": "未上市",
            "sub_industry": "4D成像雷达",
            "base_revenue_q3_2025": 3.7,
            "net_margin": -0.04,
            "gross_margin": 22.4,
            "market_share_2025": 2.6,
            "employees": 760,
        },
        {
            "company_name": "华为智能汽车解决方案BU",
            "stock_code": "未上市",
            "sub_industry": "智能驾驶雷达融合",
            "base_revenue_q3_2025": 9.8,
            "net_margin": 0.12,
            "gross_margin": 39.0,
            "market_share_2025": 5.5,
            "employees": 6500,
        },
        {
            "company_name": "博世中国智能驾驶",
            "stock_code": "未上市",
            "sub_industry": "车载毫米波雷达",
            "base_revenue_q3_2025": 13.5,
            "net_margin": 0.14,
            "gross_margin": 34.7,
            "market_share_2025": 10.2,
            "employees": 5000,
        },
    ]

    # 2024Q4 + 2025Q1~Q4
    quarter_multipliers = {
        (2024, 4): 0.86,
        (2025, 1): 0.88,
        (2025, 2): 0.95,
        (2025, 3): 1.00,
        (2025, 4): 1.08,
    }

    rows = []
    for c in companies:
        for (year, quarter), m in quarter_multipliers.items():
            revenue = round(c["base_revenue_q3_2025"] * m, 2)
            net_profit = round(revenue * c["net_margin"], 2)
            market_share = round(c["market_share_2025"] * (0.93 if year == 2024 else 1.0), 2)

            rows.append(
                CompanyData(
                    company_name=c["company_name"],
                    stock_code=c["stock_code"],
                    industry=RADAR_INDUSTRY_NAME,
                    sub_industry=c["sub_industry"],
                    revenue=revenue,
                    net_profit=net_profit,
                    gross_margin=c["gross_margin"],
                    market_cap=round(max(revenue * 9.5, 12.0), 2) if c["stock_code"] != "未上市" else None,
                    employees=c["employees"],
                    market_share=market_share,
                    year=year,
                    quarter=quarter,
                    data_source="公开财报口径+演示模拟",
                    extra_data={
                        "source_anchor": SOURCE_ANCHORS["auto_mmwave_2024"],
                        "note": "演示模拟数据，按公开口径做季度插值",
                    },
                )
            )

    db.add_all(rows)
    db.commit()
    print(f"✓ 插入 {len(rows)} 条企业数据")


def seed_policy_data(db):
    """插入政策数据（雷达/智能网联/低空相关）"""
    rows = [
        PolicyData(
            policy_name="智能汽车创新发展战略",
            policy_number="发改产业〔2020〕202号",
            department="国家发展改革委等11部门",
            level="国家级",
            publish_date=date(2020, 2, 24),
            effective_date=date(2020, 2, 24),
            category="发展战略",
            industry=RADAR_INDUSTRY_NAME,
            summary="推进车载感知体系建设，支持毫米波雷达等核心传感器产业化应用。",
            key_points=["传感器国产化", "车规级雷达应用", "智能网联汽车"],
            full_text_url=SOURCE_ANCHORS["policy_ndrc"],
            impact_level="重大",
            affected_entities=["主机厂", "传感器厂商", "Tier1"],
        ),
        PolicyData(
            policy_name="数字交通发展规划纲要",
            policy_number="交规划发〔2019〕89号",
            department="交通运输部",
            level="国家级",
            publish_date=date(2019, 7, 25),
            effective_date=date(2019, 7, 25),
            category="发展规划",
            industry=RADAR_INDUSTRY_NAME,
            summary="推动交通基础设施数字化升级，强化道路感知网络与雷达等设施协同。",
            key_points=["道路感知", "交通数字化", "基础设施升级"],
            full_text_url=SOURCE_ANCHORS["policy_mot"],
            impact_level="重大",
            affected_entities=["高速运营方", "城市交管", "设备供应商"],
        ),
        PolicyData(
            policy_name="车联网（智能网联汽车）产业发展行动计划",
            policy_number="工信部联科〔2018〕283号",
            department="工业和信息化部",
            level="国家级",
            publish_date=date(2018, 12, 27),
            effective_date=date(2018, 12, 27),
            category="行动计划",
            industry=RADAR_INDUSTRY_NAME,
            summary="提升智能网联汽车渗透率，带动车载雷达需求增长。",
            key_points=["L2/L3渗透", "雷达装配率提升", "产业链协同"],
            full_text_url=SOURCE_ANCHORS["policy_miit"],
            impact_level="重大",
            affected_entities=["整车厂", "雷达供应商"],
        ),
        PolicyData(
            policy_name="关于开展智能网联汽车准入和上路通行试点工作的通知",
            policy_number="工信厅联通装〔2023〕217号",
            department="工业和信息化部等四部门",
            level="国家级",
            publish_date=date(2023, 11, 17),
            effective_date=date(2023, 11, 17),
            category="试点通知",
            industry=RADAR_INDUSTRY_NAME,
            summary="推动智能网联汽车准入和上路试点，加速雷达等感知器件装车落地。",
            key_points=["准入试点", "上路通行", "感知冗余"],
            full_text_url=SOURCE_ANCHORS["policy_miit"],
            impact_level="重大",
            affected_entities=["整车厂", "自动驾驶方案商", "传感器企业"],
        ),
        PolicyData(
            policy_name="低空经济发展行动部署（示范）",
            policy_number="示范口径",
            department="地方发改/工信部门",
            level="省级",
            publish_date=date(2024, 5, 10),
            effective_date=date(2024, 5, 10),
            category="实施方案",
            industry=RADAR_INDUSTRY_NAME,
            summary="低空安防与空域管理场景扩容，带动低空监视雷达需求。",
            key_points=["低空感知", "空域管理", "安防雷达"],
            full_text_url=SOURCE_ANCHORS["policy_ndrc"],
            impact_level="一般",
            affected_entities=["低空运营商", "安防系统集成商"],
        ),
    ]

    db.add_all(rows)
    db.commit()
    print(f"✓ 插入 {len(rows)} 条政策数据")


def cleanup_radar_only(db):
    """仅清理雷达行业相关数据，不动其它行业"""
    deleted_stats = db.query(IndustryStats).filter(IndustryStats.industry_name == RADAR_INDUSTRY_NAME).delete()
    deleted_companies = db.query(CompanyData).filter(CompanyData.industry == RADAR_INDUSTRY_NAME).delete()
    deleted_policies = db.query(PolicyData).filter(PolicyData.industry == RADAR_INDUSTRY_NAME).delete()
    db.commit()
    print(f"✓ 已清理雷达数据: industry_stats={deleted_stats}, company_data={deleted_companies}, policy_data={deleted_policies}")


def main():
    print("=" * 68)
    print("雷达行业 Demo 数据初始化")
    print("=" * 68)

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        cleanup_radar_only(db)
        seed_industry_stats(db)
        seed_company_data(db)
        seed_policy_data(db)

        stats_count = db.query(IndustryStats).filter(IndustryStats.industry_name == RADAR_INDUSTRY_NAME).count()
        company_count = db.query(CompanyData).filter(CompanyData.industry == RADAR_INDUSTRY_NAME).count()
        policy_count = db.query(PolicyData).filter(PolicyData.industry == RADAR_INDUSTRY_NAME).count()
        print("-" * 68)
        print(f"完成：industry_stats={stats_count}, company_data={company_count}, policy_data={policy_count}")
        print("=" * 68)

    except Exception as e:
        db.rollback()
        print(f"初始化失败: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()

