from common import CONFIG
from pathlib import Path
from .util import datafeed,sector


if __name__ == '__main__':
    test = False
    sector = sector.Sector()
    sectors_list = sector.get_up_sector(sectorlist=['concept', 'l3'], ret='today')
    stocklist = sector.get_list_in_sector(sectorlist=sectors_list)
    sector.get_L2Vol(stocklist, '20220301', '20220305')
    if test:
        sectors_list = sector.get_up_sector(sectorlist=['concept', 'l3'], ret='today')
        sectorslist_df = sector.get_up_sector(sectorlist=['concept', 'l3'], ret='daily')
        sector.update_block(block_code='ZFBK', update_list=sectors_list)
        stocklist = sector.get_list_in_sector(sectorlist=sectors_list)
        stocklist_df = sector.get_daily_list_in_sector(sectorlist_df=sectorslist_df)