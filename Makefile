PYTHON = conda run -n isochrone python

## 清库重建表
db-reset:
	$(PYTHON) -c "\
from src.settings import load_config; load_config(); \
from src.models import suumo_engine, init_suumo_db; \
from sqlalchemy import text; \
conn = suumo_engine.connect(); \
conn.execute(text('DROP TABLE IF EXISTS mansions, kodates, price_history, crawl_logs, crawl_cycles, listings CASCADE')); \
conn.commit(); conn.close(); \
print('Old tables dropped.'); \
init_suumo_db(); \
"

## 东京 中古公寓 30条（带详情）
test-mansion:
	$(PYTHON) scripts/crawl_suumo.py --type mansion --prefecture 13 --max-items 30

## 东京 中古一户建 30条（带详情）
test-kodate:
	$(PYTHON) scripts/crawl_suumo.py --type kodate --prefecture 13 --max-items 30

## 神奈川 中古公寓 30条
test-mansion-kanagawa:
	$(PYTHON) scripts/crawl_suumo.py --type mansion --prefecture 14 --max-items 30

## 神奈川 中古一户建 30条
test-kodate-kanagawa:
	$(PYTHON) scripts/crawl_suumo.py --type kodate --prefecture 14 --max-items 30

## 埼玉 中古公寓 30条
test-mansion-saitama:
	$(PYTHON) scripts/crawl_suumo.py --type mansion --prefecture 11 --max-items 30

## 埼玉 中古一户建 30条
test-kodate-saitama:
	$(PYTHON) scripts/crawl_suumo.py --type kodate --prefecture 11 --max-items 30

## 千叶 中古公寓 30条
test-mansion-chiba:
	$(PYTHON) scripts/crawl_suumo.py --type mansion --prefecture 12 --max-items 30

## 千叶 中古一户建 30条
test-kodate-chiba:
	$(PYTHON) scripts/crawl_suumo.py --type kodate --prefecture 12 --max-items 30

.PHONY: db-reset test-mansion test-kodate \
        test-mansion-kanagawa test-kodate-kanagawa \
        test-mansion-saitama test-kodate-saitama \
        test-mansion-chiba test-kodate-chiba
