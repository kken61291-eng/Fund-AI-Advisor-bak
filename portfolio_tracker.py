import json
import os
import threading
from datetime import datetime
from utils import logger

class PortfolioTracker:
    def __init__(self, filepath='portfolio.json'):
        self.filepath = filepath
        self.lock = threading.Lock()
        self._load_portfolio()

    def _load_portfolio(self):
        if not os.path.exists(self.filepath):
            self.portfolio = {}
            self._save_portfolio()
        else:
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self.portfolio = json.load(f)
                
                # [V14.12 修复] 数据自愈：自动补全旧版数据的缺失字段
                dirty = False
                for code, pos in self.portfolio.items():
                    if 'shares' not in pos: 
                        pos['shares'] = 0
                        dirty = True
                    if 'cost' not in pos: 
                        pos['cost'] = 0.0
                        dirty = True
                    if 'held_days' not in pos: 
                        pos['held_days'] = 0
                        dirty = True
                    if 'history' not in pos: 
                        pos['history'] = []
                        dirty = True
                
                if dirty:
                    self._save_portfolio()
                    logger.info("🔧 检测到旧版账本数据，已自动补全缺失字段 (Self-Healing).")

            except Exception as e:
                logger.error(f"账本加载失败: {e}, 重置为空账本")
                self.portfolio = {}

    def _save_portfolio(self):
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(self.portfolio, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"账本保存失败: {e}")

    def get_position(self, code):
        if code not in self.portfolio:
            return {'shares': 0, 'cost': 0.0, 'held_days': 0}
        pos = self.portfolio[code]
        # 双重保险
        return {
            'shares': pos.get('shares', 0),
            'cost': pos.get('cost', 0.0),
            'held_days': pos.get('held_days', 0)
        }

    def add_trade(self, code, name, amount_or_value, price, is_sell=False):
        if price <= 0: return

        if code not in self.portfolio:
            self.portfolio[code] = {
                "name": name,
                "shares": 0,
                "cost": 0.0,
                "held_days": 0,
                "history": []
            }
        
        pos = self.portfolio[code]
        # 运行时防御
        if 'shares' not in pos: pos['shares'] = 0
        if 'cost' not in pos: pos['cost'] = 0.0
        if 'held_days' not in pos: pos['held_days'] = 0
        if 'history' not in pos: pos['history'] = []

        shares_change = amount_or_value / price
        
        record = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "price": round(price, 3),
            "s": "S" if is_sell else "B"
        }

        if is_sell:
            real_sell_shares = min(pos['shares'], shares_change)
            pos['shares'] = max(0, pos['shares'] - real_sell_shares)
            record['amt'] = -int(real_sell_shares * price)
            
            if pos['shares'] == 0:
                pos['cost'] = 0.0 
                pos['held_days'] = 0
        else:
            old_value = pos['shares'] * pos['cost']
            new_invest = shares_change * price
            total_shares = pos['shares'] + shares_change
            
            if total_shares > 0:
                new_cost = (old_value + new_invest) / total_shares
                pos['cost'] = round(new_cost, 4)
            
            pos['shares'] = total_shares
            record['amt'] = int(amount_or_value)
            if pos['held_days'] == 0:
                pos['held_days'] = 1

        pos['history'].append(record)
        if len(pos['history']) > 10:
            pos['history'] = pos['history'][-10:]

        self._save_portfolio()
        logger.info(f"⚖️ 账本更新 {name}: {'卖出' if is_sell else '买入'} | 最新成本: {pos.get('cost',0):.3f}")

    def record_signal(self, code, signal):
        pass 

    def get_signal_history(self, code):
        if code in self.portfolio:
            return self.portfolio[code].get('history', [])
        return []
        
    def confirm_trades(self):
        # [V14.12 修复] 使用 .get() 安全访问
        for code, pos in self.portfolio.items():
            if pos.get('shares', 0) > 0:
                pos['held_days'] = pos.get('held_days', 0) + 1
        self._save_portfolio()
