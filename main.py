import yaml
import os
import threading
import json
import base64
import re  # 用于 Markdown 正则清洗
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from utils import send_email, logger, LOG_FILENAME

# --- 全局配置 ---
DEBUG_MODE = True  
tracker_lock = threading.Lock()

def load_config():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"配置文件读取失败: {e}")
        return {"funds": [], "global": {"base_invest_amount": 1000, "max_daily_invest": 5000}}

def clean_markdown(text):
    """
    强效清洗 AI 回复中可能夹带的 Markdown 格式
    """
    if not text:
        return ""
    # 1. 移除 ```html ... ``` 或 ```markdown ... ``` 块标签
    text = re.sub(r'```(?:html|markdown)?', '', text)
    # 2. 移除常见的 Markdown 加粗和斜体标记 (**text** -> text)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'__(.*?)__', r'\1', text)
    # 3. 移除多余的 * 或 - 列表标记（仅针对行首）
    text = re.sub(r'^\s*[\*\-]\s+', '', text, flags=re.MULTILINE)
    return text.strip()

def calculate_position_v13(tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, strategy_type, fund_name):
    """
    V13 核心算分逻辑 (含 CIO 一票否决权 & 移动端逻辑适配)
    """
    base_score = tech.get('quant_score', 50)
    
    try:
        ai_adj_int = int(ai_adj)
    except:
        logger.warning(f"⚠️ {fund_name} AI调整值类型错误 ({ai_adj}), 重置为0")
        ai_adj_int = 0

    # 1. 初始计算
    tactical_score = max(0, min(100, base_score + ai_adj_int))
    
    # 2. CIO 一票否决权
    override_reason = ""
    original_score = tactical_score
    
    if ai_decision == "REJECT":
        tactical_score = 0 
        override_reason = "⛔ CIO指令:REJECT (强制否决)"
    elif ai_decision == "HOLD":
        if tactical_score >= 60:
            tactical_score = 59
            override_reason = "⏸️ CIO指令:HOLD (强制观望)"
            
    if override_reason:
        logger.warning(f"⚠️ [CIO介入 {fund_name}] 原分{original_score} -> {override_reason} -> 修正后: {tactical_score}")

    # 3. 记录状态
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj_int
    tech['valuation_desc'] = val_desc
    cro_signal = tech.get('tech_cro_signal', 'PASS')
    
    tactical_mult = 0
    reasons = []

    # 4. 定档
    if tactical_score >= 85: tactical_mult = 2.0; reasons.append("战术:极强")
    elif tactical_score >= 70: tactical_mult = 1.0; reasons.append("战术:走强")
    elif tactical_score >= 60: tactical_mult = 0.5; reasons.append("战术:企稳")
    elif tactical_score <= 25: tactical_mult = -1.0; reasons.append("战术:破位")

    # 5. 结合估值系数
    final_mult = tactical_mult
    if tactical_mult > 0:
        if val_mult < 0.5: final_mult = 0; reasons.append(f"战略:高估刹车")
        elif val_mult > 1.0: final_mult *= val_mult; reasons.append(f"战略:低估加倍")
    elif tactical_mult < 0:
        if val_mult > 1.2: final_mult = 0; reasons.append(f"战略:底部锁仓")
        elif val_mult < 0.8: final_mult *= 1.5; reasons.append("战略:高估止损")
    else:
        if val_mult >= 1.5 and strategy_type in ['core', 'dividend']:
            final_mult = 0.5; reasons.append(f"战略:左侧定投")

    # 6. 风控
    if cro_signal == "VETO":
        if final_mult > 0:
            final_mult = 0
            reasons.append(f"🛡️风控:否决买入")
            logger.info(f"🚫 [风控拦截 {fund_name}] 触发: {tech.get('tech_cro_comment')}")
    
    # 7. 锁仓规则
    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"规则:锁仓({held_days}天)")

    # 8. 计算最终金额
    final_amt = 0; is_sell = False; sell_val = 0; label = "观望"
    if final_mult > 0:
        amt = int(base_amt * final_mult)
        final_amt = max(0, min(amt, int(max_daily)))
        label = "买入"
    elif final_mult < 0:
        is_sell = True
        sell_ratio = min(abs(final_mult), 1.0)
        sell_val = pos['shares'] * tech.get('price', 0) * sell_ratio
        label = "卖出"

    if reasons: tech['quant_reasons'] = reasons
    return final_amt, label, is_sell, sell_val

def render_html_report_v13(all_news, results, cio_html, advisor_html):
    """
    生成完整的 HTML 邮件报告 (V15.20 移动端适配 & Markdown 清洗版)
    """
    # --- 样式定义 ---
    COLOR_GOLD = "#fab005" 
    COLOR_RED = "#fa5252"  
    COLOR_GREEN = "#51cf66" 
    COLOR_TEXT_MAIN = "#e9ecef"
    COLOR_TEXT_SUB = "#adb5bd"
    COLOR_BG_MAIN = "#0f1215" 
    COLOR_BG_CARD = "#16191d" 
    
    # 强力清洗 AI 生成的内容 (去除 ```html, **, 列表符等)
    cio_html = clean_markdown(cio_html)
    advisor_html = clean_markdown(advisor_html)

    news_html = ""
    if isinstance(all_news, list):
        for news in all_news:
            # 兼容字典或纯字符串格式
            title = news.get('title', str(news)) if isinstance(news, dict) else str(news)
            news_html += f"""<div style="font-size:11px;color:{COLOR_TEXT_SUB};margin-bottom:5px;border-bottom:1px solid #25282c;padding-bottom:3px;"><span style="color:{COLOR_GOLD};margin-right:4px;">●</span>{title}</div>"""
    
    rows = ""
    for r in results:
        tech = r.get('tech', {})
        risk = tech.get('risk_factors', {})
        final_score = tech.get('final_score', 0)
        ai_adj = int(tech.get('ai_adjustment', 0))
        cro_signal = tech.get('tech_cro_signal', 'PASS')
        cro_comment = tech.get('tech_cro_comment', '无')
        
        # 动态风控颜色
        cro_style = f"color:{COLOR_RED};font-weight:bold;" if cro_signal == "VETO" else f"color:{COLOR_GREEN};font-weight:bold;"
        
        # 盈亏计算
        profit_html = ""
        if r.get('pos_shares', 0) > 0:
            p_val = (tech.get('price', 0) - r.get('pos_cost', 0)) * r.get('pos_shares', 0)
            p_color = COLOR_RED if p_val > 0 else COLOR_GREEN 
            profit_html = f"""<div style="font-size:12px;margin-bottom:8px;background:rgba(0,0,0,0.2);padding:4px 8px;border-radius:3px;display:flex;justify-content:space-between;border:1px solid #333;"><span style="color:{COLOR_TEXT_SUB};">持有盈亏:</span><span style="color:{p_color};font-weight:bold;">{p_val:+.1f}元</span></div>"""
        
        # --- [修改处] 操作标签视觉优化 ---
        act_bg = ""
        act_border = ""
        act_text = ""
        act_content = ""
        
        if r['amount'] > 0:
            # 买入样式
            act_bg = "rgba(250, 82, 82, 0.15)"
            act_border = COLOR_RED
            act_text = COLOR_RED
            act_content = f"⚡ 买入 {r['amount']:,}"
        elif r.get('is_sell'):
            # 卖出样式
            act_bg = "rgba(81, 207, 102, 0.15)"
            act_border = COLOR_GREEN
            act_text = COLOR_GREEN
            act_content = f"💰 卖出 {int(r.get('sell_value',0)):,}"
        else:
            # 观望样式
            act_bg = "rgba(255, 255, 255, 0.05)"
            act_border = "#495057"
            act_text = COLOR_TEXT_SUB
            act_content = "☕ 观望"

        # 组装增强版操作徽章
        act_html = f"""
        <span style="
            display:inline-block;
            background:{act_bg};
            color:{act_text};
            border:1px solid {act_border};
            padding:3px 10px;
            font-size:13px;
            font-weight:bold;
            border-radius:4px;
            min-width:60px;
            text-align:center;
        ">{act_content}</span>
        """
        # --- [修改结束] ---
        
        # 理由标签
        reasons = " ".join([f"<span style='border:1px solid #444;background:rgba(255,255,255,0.05);padding:1px 4px;font-size:9px;border-radius:3px;color:{COLOR_TEXT_SUB};margin-right:3px;'>{x}</span>" for x in tech.get('quant_reasons', [])])
        
        # 投委会部分 (需清洗 Markdown)
        ai_data = r.get('ai_analysis', {})
        bull_say = clean_markdown(ai_data.get('bull_view', '无'))
        bear_say = clean_markdown(ai_data.get('bear_view', '无'))
        chairman = clean_markdown(ai_data.get('chairman_conclusion') or ai_data.get('comment', '无'))

        committee_html = ""
        if bull_say != '无':
            committee_html = f"""
            <div style="margin-top:12px;border-top:1px solid #333;padding-top:10px;">
                <div class="debate-box">
                    <div class="debate-item" style="border-left:2px solid {COLOR_GREEN}; background:rgba(81, 207, 102, 0.05);">
                        <div style="color:{COLOR_GREEN};font-size:11px;font-weight:bold;">🦊 CGO</div>
                        <div style="color:#c0ebc9;font-size:11px;">"{bull_say}"</div>
                    </div>
                    <div class="debate-item" style="border-left:2px solid {COLOR_RED}; background:rgba(250, 82, 82, 0.05);">
                        <div style="color:{COLOR_RED};font-size:11px;font-weight:bold;">🐻 CRO</div>
                        <div style="color:#ffc9c9;font-size:11px;">"{bear_say}"</div>
                    </div>
                </div>
                <div style="background:rgba(250, 176, 5, 0.05);padding:10px;border-radius:4px;border:1px solid rgba(250, 176, 5, 0.2);margin-top:8px;">
                    <div style="color:{COLOR_GOLD};font-size:12px;font-weight:bold;margin-bottom:4px;">⚖️ CIO 终审 (修正: {ai_adj:+d})</div>
                    <div style="color:{COLOR_TEXT_MAIN};font-size:12px;">{chairman}</div>
                </div>
            </div>"""

        rows += f"""<div class="card" style="border-left:3px solid {COLOR_GOLD};">
            <div style="display:flex;justify-content:space-between;margin-bottom:10px;align-items:center;">
                <span style="font-size:16px;font-weight:bold;color:{COLOR_TEXT_MAIN};">{r['name']}</span>
                {act_html}
            </div>
            <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                 <span style="color:{COLOR_GOLD};font-weight:bold;font-size:18px;">{final_score}分</span>
                 <div style="font-size:11px;color:{cro_style};padding-top:4px;">🛡️ {cro_comment}</div>
            </div>
            {profit_html}
            <div class="tech-grid">
                <span>RSI: {tech.get('rsi','-')}</span>
                <span>Trend: {tech.get('macd',{}).get('trend','-')}</span>
                <span>VR: {risk.get('vol_ratio', 1.0)}</span>
                <span>Val: {tech.get('valuation_desc', 'N/A')}</span>
            </div>
            <div style="margin-top:8px;">{reasons}</div>
            {committee_html}
        </div>"""

    # --- Logo 智能处理 (Base64 嵌入) ---
    logo_path = "logo.png"
    alt_logo_path = "Gemini_Generated_Image_d7oeird7oeird7oe.jpg"
    logo_src = "https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png" # 兜底链接
    
    target_logo = logo_path if os.path.exists(logo_path) else (alt_logo_path if os.path.exists(alt_logo_path) else None)
    
    if target_logo:
        try:
            with open(target_logo, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
                mime = "image/png" if target_logo.endswith('png') else "image/jpeg"
                logo_src = f"data:{mime};base64,{b64}"
                logger.info(f"🎨 Logo 已通过 Base64 嵌入: {target_logo}")
        except Exception as e:
            logger.error(f"Logo 嵌入失败: {e}")

    # --- 移动端响应式 HTML 结构 ---
    return f"""<!DOCTYPE html><html><head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ background: {COLOR_BG_MAIN}; color: {COLOR_TEXT_MAIN}; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 10px; }}
        .main-container {{ max-width: 600px; margin: 0 auto; background: #0a0c0e; border: 1px solid #2c3e50; padding: 15px; border-radius: 8px; }}
        .card {{ background: {COLOR_BG_CARD}; margin-bottom: 15px; padding: 15px; border-radius: 4px; box-shadow: 0 4px 10px rgba(0,0,0,0.5); }}
        .tech-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 5px; font-size: 11px; color: {COLOR_TEXT_SUB}; }}
        .debate-box {{ display: flex; gap: 10px; }}
        .debate-item {{ flex: 1; padding: 8px; border-radius: 4px; }}
        /* 移动端核心适配 */
        @media (max-width: 480px) {{
            .debate-box {{ flex-direction: column; }}
            .tech-grid {{ grid-template-columns: 1fr; }}
            .main-container {{ padding: 10px; border: none; }}
        }}
        /* 强制覆盖 AI 生成内容的背景色，防止白底 */
        .cio-content, .advisor-content {{ line-height: 1.6; font-size: 13px; color: #eee !important; }}
        .cio-content *, .advisor-content * {{ background: transparent !important; color: inherit !important; }}
    </style></head><body>
    <div class="main-container">
        <div style="text-align:center; padding-bottom:20px; border-bottom:1px solid #222;">
            <img src="{logo_src}" style="width:200px; max-width:80%; display:block; margin:0 auto;">
            <div style="font-size:10px; color:{COLOR_GOLD}; letter-spacing:2px; margin-top:10px;">MAGPIE SENSES THE WIND | V15.20</div>
        </div>
        <div class="card" style="margin-top:20px;">
            <div style="color:{COLOR_GOLD}; font-weight:bold; border-bottom:1px solid #333; padding-bottom:5px; margin-bottom:10px;">📡 全球舆情雷达</div>
            {news_html}
        </div>
        <div class="card" style="border-left:3px solid {COLOR_RED};">
            <div style="color:{COLOR_RED}; font-weight:bold; margin-bottom:10px;">🛑 CIO 战略审计</div>
            <div class="cio-content">{cio_html}</div>
        </div>
        <div class="card" style="border-left:3px solid {COLOR_GOLD};">
            <div style="color:{COLOR_GOLD}; font-weight:bold; margin-bottom:10px;">🐦 鹊知风·实战复盘</div>
            <div class="advisor-content">{advisor_html}</div>
        </div>
        {rows}
        <div style="text-align:center; color:#444; font-size:10px; margin-top:30px;">EST. 2026 | POWERED BY AI</div>
    </div></body></html>"""

def process_single_fund(fund, config, fetcher, tracker, val_engine, analyst, market_context, base_amt, max_daily):
    """
    单个基金处理函数 (恢复了详细的日志记录功能，用于支持 CIO 报告)
    """
    res = None
    cio_log = ""
    used_news = []
    
    try:
        logger.info(f"Analyzing {fund['name']}...")
        
        data = fetcher.get_fund_history(fund['code'])
        if data is None or data.empty: 
            return None, "", []

        tech = TechnicalAnalyzer.calculate_indicators(data)
        if not tech: return None, "", []
        
        try:
            val_mult, val_desc = val_engine.get_valuation_status(fund.get('index_name'), fund.get('strategy_type'))
        except:
            val_mult, val_desc = 1.0, "估值异常"

        with tracker_lock: pos = tracker.get_position(fund['code'])

        ai_adj = 0; ai_res = {}
        should_run_ai = True

        if analyst and should_run_ai:
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            fuse_level = 3 if cro_signal == 'VETO' else (1 if cro_signal == 'WARN' else 0)
            
            risk_payload = {
                "fuse_level": fuse_level,
                "risk_msg": tech.get('tech_cro_comment', '常规监控')
            }
            
            try:
                ai_res = analyst.analyze_fund_v5(fund['name'], tech, None, market_context, risk_payload, fund.get('strategy_type', 'core'))
                ai_adj = ai_res.get('adjustment', 0)
            except Exception as e:
                logger.error(f"AI Analysis Failed: {e}")
                ai_res = {"bull_view": "Error", "bear_view": "Error", "comment": "Offline", "adjustment": 0}

        ai_decision = ai_res.get('decision', 'PASS') 
        
        amt, lbl, is_sell, s_val = calculate_position_v13(
            tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'), fund['name']
        )
        
        with tracker_lock:
            tracker.record_signal(fund['code'], lbl)
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

        bull = ai_res.get('bull_view') or ai_res.get('bull_say', '无')
        bear = ai_res.get('bear_view') or ai_res.get('bear_say', '无')
        if bull != '无':
            logger.info(f"🗣️ [投委会 {fund['name']}] CGO:{bull[:20]}... | CRO:{bear[:20]}...")

        # 恢复详细的日志记录，以便 CIO 报告使用
        reason_str = ",".join(tech.get('quant_reasons', []))
        cio_log = f"标的:{fund['name']} | 决策:{lbl} (分:{tech['final_score']} AI:{ai_adj}) | 理由:{reason_str}"

        res = {
            "name": fund['name'], "code": fund['code'], 
            "amount": amt, "sell_value": s_val, "position_type": lbl, "is_sell": is_sell, 
            "tech": tech, "ai_analysis": ai_res, "history": tracker.get_signal_history(fund['code']),
            "pos_cost": pos.get('cost', 0), "pos_shares": pos.get('shares', 0)
        }
    except Exception as e:
        logger.error(f"Process Error {fund['name']}: {e}")
        return None, "", []
    return res, cio_log, used_news

def main():
    config = load_config()
    fetcher = DataFetcher()
    tracker = PortfolioTracker()
    val_engine = ValuationEngine()
    
    logger.info(f">>> [V15.20] Startup | LOCAL_MODE=True | Mobile Responsive = ON")
    tracker.confirm_trades()
    try:
        analyst = NewsAnalyst()
    except Exception:
        analyst = None

    logger.info("📖 正在构建全天候舆情上下文...")
    market_context = analyst.get_market_context() if analyst else "无新闻数据"
    logger.info(f"🌍 舆情上下文长度: {len(market_context)} 字符")
    
    # 修复：恢复新闻列表解析逻辑，否则邮件新闻栏为空
    all_news_seen = []
    if market_context and market_context != "今日暂无重大新闻。":
        for line in market_context.split('\n'):
            try:
                if line.strip().startswith('['):
                    all_news_seen.append(line.strip())
            except Exception:
                pass

    results = []; cio_lines = [f"【宏观环境】: (见独立审计报告)\n"]
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_fund = {executor.submit(
            process_single_fund, 
            fund, config, fetcher, tracker, val_engine, analyst, market_context, 
            config['global']['base_invest_amount'], config['global']['max_daily_invest']
        ): fund for fund in config.get('funds', [])}
        
        for future in as_completed(future_to_fund):
            try:
                res, log, _ = future.result()
                if res: 
                    results.append(res)
                    cio_lines.append(log)
            except Exception as e: logger.error(f"Thread Error: {e}")

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        full_report = "\n".join(cio_lines)
        
        # AI 总结
        cio_html = analyst.review_report(full_report, market_context) if analyst else "<p>CIO Missing</p>"
        advisor_html = analyst.advisor_review(full_report, market_context) if analyst else "<p>Advisor Offline</p>"
        
        # 渲染邮件 (传入完整的新闻列表)
        html = render_html_report_v13(all_news_seen, results, cio_html, advisor_html) 
        
        send_email("🕊️ 鹊知风 V15.20 洞察微澜，御风而行", html, attachment_path=LOG_FILENAME)

if __name__ == "__main__": main()
