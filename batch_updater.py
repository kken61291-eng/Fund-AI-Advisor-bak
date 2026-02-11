import yaml
import time
import os
from data_fetcher import DataFetcher
from utils import logger

def load_config():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"配置文件读取失败: {e}")
        return {"funds": []}

def main():
    logger.info(">>> [Batch Updater] 开始全量数据更新任务...")
    
    config = load_config()
    fetcher = DataFetcher()
    funds = config.get('funds', [])
    
    total = len(funds)
    success_count = 0
    
    for i, fund in enumerate(funds):
        code = fund['code']
        name = fund['name']
        
        logger.info(f"🔄 ({i+1}/{total}) 正在更新: {name} ({code})...")
        
        # 1. 执行下载并保存
        success = fetcher.update_cache(code)
        
        if success:
            success_count += 1
        
        # 2. [关键] 强制休眠 60秒 (除最后一个外)
        # 这就是您要求的"每个板块获取后隔1分钟"
        if i < total - 1:
            logger.info("⏳ 休眠 60秒 以规避反爬...")
            time.sleep(60)
            
    logger.info(f"<<< [Batch Updater] 任务结束。成功: {success_count}/{total}")

if __name__ == "__main__":
    main()
