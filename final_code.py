import requests
import json
import random
import time
import pyodbc
import pandas as pd
from urllib.parse import quote_plus

# 数据库配置
DATABASE = 'daily'
SERVER = 'xxxxxxxxxs.com'
PORT = 1234
USERNAME = 'xxxxx'
PASSWORD = 'xxxxxxxxxxxxx'
DRIVER = '{ODBC Driver 17 for SQL Server}'


class ExpediaScraper:
    def __init__(self):
        self.session = requests.Session()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
        ]
        self.proxy_pool = [
            "http://127.0.0.1:xxxx",  # 替换为实际代理
        ]
        self.graphql_url = "https://www.expedia.com/graphql"
        self._init_headers()

    def _init_headers(self):
        """初始化动态请求头"""
        self.headers = {
            'authority': 'www.expedia.com',
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'client-info': 'lotus-home-ui,fd8d67e776b8ea96e71ded1c46dbd8c4b388e75d,us-west-2',
            'content-type': 'application/json',
            'cookie': 'tpid=v.1,1; linfo=v.4,|0|0|255|1|0||||||||2052|0|0||0|0|0|-1|-1; currency=USD; CRQS=t|1`s|1`l|zh_CN`c|USD; EG_SESSIONTOKEN=ClAKJgokYmYzMzEwZGUtMzgyNi00Mzc0LTk3ZDMtZTk2NWQzNjEwNDQ2EiYKJDk3YmNkNWExLTJiODItNDZjNS1hOGM5LWY4NTZiYjY5NjRhZQ.qqooIalSVjlJY8Sn.n9mAgz369u4X8_omYzvA_IikNtvL76GOJWH2aoA_vHgrP-lfyq7o9nAaPCA5uUX_L9dFSwSAe2xhHk3poMDA-xHuVxyrndA91jz29W7f8LhbY9TzngnRqiryvmge-6fskG9SMYjPcj0sPoGyg4jiseO62b0CaGI5eD_DDluXdA8la-JXOGs5sxLSamdUYmTvS8lJeDpbNQzSd-e6CcsILDXFTsZA__wb1q3IUFF2BGM.k42l_kcOOrt3EE6ftrkjIg; MC1=GUID=b0d60ce88f5c443288f2f7550ae989e2; DUAID=b0d60ce8-8f5c-4432-88f2-f7550ae989e2; eg_adblock=e=0; ttd_TDID=7645e6ba-f023-43b0-bfd2-77a82e27ee1d; _gcl_au=1.1.426610557.1742355330; xdid=0eb67ebe-3ef8-4e0c-bb7a-b06a709de929|1742355341|expedia.com; OptanonAlertBoxClosed=2025-03-19T04:12:30.871Z; OIP=ccpa|1`ts|1742357550; AMCV_C00802BE5330A8350A490D4C%40AdobeOrg=1585540135%7CMCIDTS%7C20167%7CMCMID%7C91640691881792333638208621083389108338%7CMCAID%7CNONE%7CMCAAMLH-1742963242%7C11%7CMCAAMB-1742963242%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCOPTOUT-1742365642s%7CNONE%7CvVersion%7C4.4.0; _fbp=fb.1.1742362691468.21309899264002745; _abck=C933CEEFBC135B30D34702A32913A0F9~-1~YAAQpXw2Fwt/UquVAQAADcNasQ00xm7FOrgZXqcGcW8uOfJ55Bh8jSfI3kO49TIBrYgaAxbv7Sm9t3gW9g5vEmDIdzWHCwqftFzGFX1DXDFrC37WXX9cF0azkLpqjS8BtR8r0C99A18jnkECaNrw8YkmXkvUZfTsZPp9nSMQrkcvyeD8R11Yiuzp6G+ZJH1L2j9hjCXvLoLroXSZZOGAxfnxCEQQTtpUw/I9cxZFL1qyJkoTDFEPa196iFWymjoHE4cSVzQhxe2s9+CFKzuIJr0OhH9ZH97pBMEBQjWQ1DlB/8xyHCdOduo0KABt0G9DrHTI2OpdZHcEx053UuBYinuF9QbXFv54JqaAuIp+kVjxRTC7RMwCtzY0yhMI0c8OgFxclVXVue878Nvh6HkD1Yo+iTIfXoOYGiCgnIZp+CnBbN55V2PwUgHxkfJofx2/1TxFUo2QBFXrVg0168Isn62z2bM=~-1~-1~-1; iEAPID=0; CRQSS=e|0; eg_ppid=f5c6fa23-70a0-4738-a396-408a7e351951; NavActions=acctWasOpened; session_id=436a7dc1-9cdd-3f22-8a80-60d140c8bf9e; page_name=page.Hotel-Search; pageVisited=true; aspp=v.1,0|||||||||||||; QSI_HistorySession=https%3A%2F%2Fwww.expedia.com%2FHotel-Search%3Fdestination%3D%25E9%2598%25BF%25E9%2587%258C%25E5%25B1%25B1%252C%2520%25E5%2598%2589%25E4%25B9%2589%25E5%258E%25BF%252C%2520%25E5%258F%25B0%25E6%25B9%25BE%26latLong%3D23.435467%252C120.780968%26flexibility%3D0_DAY%26d1%3D2025-04-02%26startDate%3D2025-04-02%26d2%3D2025-04-03%26endDate%3D2025-04-03%26adults%3D2%26rooms%3D1%26regionId%3D6051171%26isInvalidatedDate%3Dfalse%26theme%3D%26userIntent%3D%26semdtl%3D%26useRewards%3Dfalse%26sort%3DRECOMMENDED~1742443153239%7Chttps%3A%2F%2Fwww.expedia.com%2FHotel-Search%3Fdestination%3D%25E5%258D%258E%25E6%25B3%25B0%25E7%2591%259E%25E8%258B%2591%25E5%259E%25A6%25E4%25B8%2581%25E5%25AE%25BE%25E9%25A6%2586%252C%2520%25E6%2581%2592%25E6%2598%25A5%252C%2520%25E5%25B1%258F%25E4%25B8%259C%25E5%258E%25BF%252C%2520%25E5%258F%25B0%25E6%25B9%25BE%26regionId%3D6047453%26latLong%3D21.9562%252C120.813003%26propertyId%3D1343463%26selected%3D1343463%26flexibility%3D0_DAY%26d1%3D2025-04-02%26startDate%3D2025-04-02%26d2%3D2025-04-03%26endDate%3D2025-04-03%26adults%3D2%26rooms%3D1%26isInvalidatedDate%3Dfalse%26theme%3D%26userIntent%3D%26semdtl%3D%26useRewards%3Dfalse%26sort%3DRECOMMENDED~1742443739037%7Chttps%3A%2F%2Fwww.expedia.com%2Fcn%2FHengchun-Hotels-Gloria-Manor.h1343463.Hotel-Information%3Fchkin%3D2025-04-02%26chkout%3D2025-04-03%26x_pwa%3D1%26rfrr%3DHSR%26pwa_ts%3D1742443676182%26referrerUrl%3DaHR0cHM6Ly93d3cuZXhwZWRpYS5jb20vSG90ZWwtU2VhcmNo%26useRewards%3Dfalse%26rm1%3Da2%26regionId%3D6047453%26destination%3D%25E6%2581%2592%25E6%2598%25A5%252C%2B%25E5%25B1%258F%25E4%25B8%259C%25E5%258E%25BF%252C%2B%25E5%258F%25B0%25E6%25B9%25BE%26destType%3DMARKET%26neighborhoodId%3D553248635976007544%26selected%3D1343463%26latLong%3D22.000828%252C120.744766%26sort%3DRECOMMENDED%26top_dp%3D179%26top_cur%3DUSD%26userIntent%3D%26selectedRoomType%3D202413134%26selectedRatePlan%3D212279759%26searchId%3D868c75dd-bfc5-423f-a590-13b7ddfc5015~1742443756824; HMS=51d56810-a1ae-34be-9b85-728563f073bd; bm_ss=ab8e18ef4e; __gads=ID=77d219e04244ac2c:T=1742355328:RT=1742449447:S=ALNI_MZiFko1QGh2YYmXyxwMIPDKz1b3Yg; __gpi=UID=00001069bdeebcb0:T=1742355328:RT=1742449447:S=ALNI_MYfUFPBjF0B5VWVk9TDv6yc0i8RAg; __eoi=ID=f1231ef301ebf956:T=1742355328:RT=1742449447:S=AA-AfjYJJDVsUjivB1iYcQs8Dp0-; bm_mi=60556A3E72B889C939B1E87604AB2975~YAAQC6InFxvC3J6VAQAADpkUshsvrjY5bsE0ni465b05YWvyRiC3/XpgmrcDeNr+uoPsUixmiBqGYBmjmC7dcLSo6dSHe2nuHZE2GTEH2MOy7bpcSQzas5xmfnlr61U4zHz6wBRsjdHwTzvMeeXL0Tgmtu5drRXJ4YeNj72Ye3p6nY8KX9Ab91pzesr3EdTgziPvG4IOC0KUc4ANEQO4JfwlCpdbYe498vCiXoqI5EL8AeBCIlNB0c3Xb71CqpMgTwLttdThGRjSR0nKQib7aFTnUDY27bGObquGU9l3pdx0AEIeBcPACalrIXSoXk7D8w==~1; ak_bmsc=565F15E46AA97694BECCB0459C2441B7~000000000000000000000000000000~YAAQC6InF+DC3J6VAQAAZaIUshsweA1GCKJSbebr+mu0AufUz470UVFqBUZqEFGFXodkUjV9P3mw2DSNtzGh060ahoQksQN/s+QHEOrU1oWQjpYC4UTTKG7QkROEm5Nqs7VIvcOqbeQWAjmpEd188gKzEqWwqyU2WqApz9QkjLaHoHtaaWATL00VSsyywXqArFMRp1GisXwgKrU2IkuW/DU6s63hqvsyC89ES1ya6UWk5rgu8o0c7ACgOnfqEmLssnQTPTYmztva0uzbtDMroTcJ2945nzqtNGhBdbFcZt5SS2Lo/73BI/d5xuzB/GsC2b848seZw58SiMQgypCpzfyUvlnYgrQlDEFk0SGuWzzTwbKjW94SnxRcq9RabsC8L4AfvrF/Fw/Mi+RfTl3pCvUoQeFQ+hoYqu4RqnZnhsx0HBkSsI7+kXPGuwJmcIe00aPin2BbGJOM2JUq2n3jVY5JqvfLDorug4RsyXAJG9km; JSESSIONID=52B9B9EEA6689BC44BA418BEFE03D7A7; bm_so=A1FAF141721E035549BCE57D1927CA576159F0AED41679FA008C0C79C9E542EB~YAAQC6InF8TQ3J6VAQAAzl8VsgMqTzdFSdIDWMIqrjyKL598OCGJtzHxW+j+FlDAq3+jOBXuJPucj2ganHbQdCDLahk7Cm6QgC4bNC08ydHu78ULxoii0RLLZL+Hel8PvhxTsRlAWC447A2uMj2frY1Pyx/x85jwClnmbqBQoErowsfpYKMdvPorjV40cIyBJ1mBuMtLG6hdT182KiYfzHPH3CODcyLviOviwrcvKLxaAcffvL6802N8XGzfIZ1PtaqlkqYNw5RlfB0+HlW7UXCR8Xlry0Ax7zuLZb5N2J61Bkpu5yG0iWIimHwD1AaRSso1LRtxB+o/sEF462TdZ3YkxSIEfP3Bi6sbEioHNntiaqk1ciSQZiUX8ccXE6XKQSEnyVqv8x4eWKMmAQfKg+u/kZpA5Wkyi36MhqgREvmjBGk7Rl4Y9UdG+ELm1yyq29JZ1mkFRYgV0Gqws3rO; _uetsid=3607b1f0047311f0a2186dd25aa31bca; _uetvid=3607cdd0047311f0840031b6cc64aedc; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Mar+20+2025+13%3A45%3A07+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202305.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=bc1a625f-f063-4aac-9481-9417c6edeea5&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CSPD_BG%3A1%2CC0004%3A1&geolocation=%3B&AwaitingReconsent=false; bm_lso=A1FAF141721E035549BCE57D1927CA576159F0AED41679FA008C0C79C9E542EB~YAAQC6InF8TQ3J6VAQAAzl8VsgMqTzdFSdIDWMIqrjyKL598OCGJtzHxW+j+FlDAq3+jOBXuJPucj2ganHbQdCDLahk7Cm6QgC4bNC08ydHu78ULxoii0RLLZL+Hel8PvhxTsRlAWC447A2uMj2frY1Pyx/x85jwClnmbqBQoErowsfpYKMdvPorjV40cIyBJ1mBuMtLG6hdT182KiYfzHPH3CODcyLviOviwrcvKLxaAcffvL6802N8XGzfIZ1PtaqlkqYNw5RlfB0+HlW7UXCR8Xlry0Ax7zuLZb5N2J61Bkpu5yG0iWIimHwD1AaRSso1LRtxB+o/sEF462TdZ3YkxSIEfP3Bi6sbEioHNntiaqk1ciSQZiUX8ccXE6XKQSEnyVqv8x4eWKMmAQfKg+u/kZpA5Wkyi36MhqgREvmjBGk7Rl4Y9UdG+ELm1yyq29JZ1mkFRYgV0Gqws3rO^1742449530153; bm_s=YAAQC6InF3ca3Z6VAQAAl/MYsgNMCYq1PQ8G+PvqYZ009OKMtQ5rZuQR9lljmZYh/Gn1uZM6YL+Cxk4lFqOEMkcqDub0+eD6jOFaqsDaw1qgKnv6E7OVrS2FN0zswxTJtWn4LtPAnNuXiYYK3eXdTBYTLTrFu7trvriJsS98wQ5z+ypwAVQjWpEfQX9BRQEQg2BD2uLmy2WDOMyZCGECsawgG0rlJJurHIxCbHv2tNvNNJpkJdba6I1A0XjDd4gszjGzVbKaNuo/U3Q36MEUykV7C/Bc1iF3ffP90gmngheX0K9gHobLvIqyMOYzlMCRK1YjjbPlkutCKRfFP/4s0fePhtXzAIEQ8MWeoclrOcin4cCZAa4kq0wPJQ5D9EksARGZWhDwhgZT14ziM1/lcNAGsIJ/ZJkRD+wdgq2KvbQQ3KK6T6g8/9XkHUrc0RCIvIMNtevSCse15z/o1gLpDgtMu4Q=; bm_sv=B1B60180322253C67EF98126B2E5D74B~YAAQr3w2F4ZoKK2VAQAA/Y0ashs2PYJNZIzXiIPzryIJVrE5JieOPuQxRT7GrpKDo/JgA+3c4ct1aOTpL/GYlQxfV4yTa1zmMZvvmx/Rqdh4P9F2JstH3U2Qy7yysSJsf4EQWpTt6T1Psgr46s1+giM5rK7ypgczPRPoOLfgpc5t9n6UKixvW8yZ7w8JVvWhCdQfyX9qzaCyigluPSZ0nS4EPCNciF29QsPGsnxeS5+Cr782oaSEacfNmL2i8ySeVUM=~1; bm_sz=123116E13BE3CC07CF4C2AFF36A0510C~YAAQaD0xF3yey6+VAQAAH8ssshtpWIom12VSVaBcahw7belR/wg0RQvASVoXAChYb2K16lh/OhqYGbx+b/Cn+cy/4gPD3plJ0W8sfAIYIfenVY+ZnMu4lWCeZQqP4OhefIVPMEk5aLbWHfR/icn/O0N3y+rsQ6IZi1qyu5CpaIUOojlTIfM/BM16FmDTM3kLswU0XS2oUQYWipkBhdxUN1JAHMEA9TrqLLN8QVBFR35ubg1IyPwtjWghwKC+oQ2NuMvi2V3fAwErmg3SSaSpMLtK8MAgjuloJFfXEqbF21HEcNpOo/RHuoJdAZlDO11k83Cc07lzX55qCIXe5d38ZotKxJrnTD1ub+/7SOlIOHdvUwBlVWR76yleKU+xEiOr1WuOX89HnGvGOFoOnDyPTOuDeZ5YoZnbmQsJqx5hFURMx1bv+tBYBuWyTHJps507FixUuEd6H1cl6yaaX6Sm0hsFCkuhJi8XxJTbwN0sUhwLeiR2vdPStvrw2D4+oANGZVrLwAwmjXBMbOL6Bv+zZ5rWA7fs4puzGyFiFD2Qy8KWrKANioCc6dX9lREXiiGlznK6l/cYpHw8+U/HasPb~4535876~3359027; cesc=%7B%22lpe%22%3A%5B%221e9e64cd-0f3e-4dd0-8e2d-18f8342d8110%22%2C1742451057291%5D%2C%22marketingClick%22%3A%5B%22false%22%2C1742451057291%5D%2C%22lmc%22%3A%5B%22DIRECT.WEB%22%2C1742451057291%5D%2C%22hitNumber%22%3A%5B%2221%22%2C1742451057291%5D%2C%22amc%22%3A%5B%22SEO.B.BING.COM%22%2C1742451057291%5D%2C%22visitNumber%22%3A%5B%226%22%2C1742449447796%5D%2C%22ape%22%3A%5B%228d7d4f17-d026-4ca1-bb6c-cbe71cafb1aa%22%2C1742451057291%5D%2C%22cidVisit%22%3A%5B%22SEO.B.bing.com%22%2C1742451057291%5D%2C%22entryPage%22%3A%5B%22page.Hotel-Search%22%2C1742451057291%5D%2C%22cid%22%3A%5B%22SEO.B.bing.com%22%2C1742449449227%5D%7D; _dd_s=rum=0&expire=1742451958063; DUAID=b0d60ce8-8f5c-4432-88f2-f7550ae989e2; EG_SESSIONTOKEN=ClAKJgokYmYzMzEwZGUtMzgyNi00Mzc0LTk3ZDMtZTk2NWQzNjEwNDQ2EiYKJDk3YmNkNWExLTJiODItNDZjNS1hOGM5LWY4NTZiYjY5NjRhZQ.qqooIalSVjlJY8Sn.n9mAgz369u4X8_omYzvA_IikNtvL76GOJWH2aoA_vHgrP-lfyq7o9nAaPCA5uUX_L9dFSwSAe2xhHk3poMDA-xHuVxyrndA91jz29W7f8LhbY9TzngnRqiryvmge-6fskG9SMYjPcj0sPoGyg4jiseO62b0CaGI5eD_DDluXdA8la-JXOGs5sxLSamdUYmTvS8lJeDpbNQzSd-e6CcsILDXFTsZA__wb1q3IUFF2BGM.k42l_kcOOrt3EE6ftrkjIg; HMS=51d56810-a1ae-34be-9b85-728563f073bd; MC1=GUID=b0d60ce88f5c443288f2f7550ae989e2; cesc=%7B%22lpe%22%3A%5B%221e9e64cd-0f3e-4dd0-8e2d-18f8342d8110%22%2C1742451260738%5D%2C%22marketingClick%22%3A%5B%22false%22%2C1742451260738%5D%2C%22lmc%22%3A%5B%22DIRECT.WEB%22%2C1742451260738%5D%2C%22hitNumber%22%3A%5B%2222%22%2C1742451260738%5D%2C%22amc%22%3A%5B%22SEO.B.BING.COM%22%2C1742451260738%5D%2C%22visitNumber%22%3A%5B%226%22%2C1742449447796%5D%2C%22ape%22%3A%5B%228d7d4f17-d026-4ca1-bb6c-cbe71cafb1aa%22%2C1742451260738%5D%2C%22cidVisit%22%3A%5B%22SEO.B.bing.com%22%2C1742451260738%5D%2C%22entryPage%22%3A%5B%22page.Hotel-Search%22%2C1742451260738%5D%2C%22cid%22%3A%5B%22SEO.B.bing.com%22%2C1742449449227%5D%7D',
            'ctx-view-id': '11170b6e-560d-4dd6-be62-fd20fec62d7e',
            'origin': 'https://www.expedia.com',
            'referer': 'https://www.expedia.com/cn/?pwaDialog=search-location-dialog-destination_form_field-3',
            'sec-ch-ua': '"Microsoft Edge";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.50',
            'x-hcom-origin-id': 'Homepage,U,10',
            'x-page-id': 'Homepage,U,10'
        }

    def _rotate_identity(self):
        """更换请求特征"""
        new_headers = {
            'user-agent': random.choice(self.user_agents),
            'x-page-id': f'Homepage,U,{random.randint(10, 99)}',
            'ctx-view-id': self._generate_uuid()
        }
        self.headers.update(new_headers)

    def _generate_uuid(self):
        """生成UUIDv4格式的随机字符串"""
        return ''.join(random.choices('0123456789abcdef', k=32))

    def _get_proxy(self):
        """获取随机代理"""
        return {'http': random.choice(self.proxy_pool)} if self.proxy_pool else None

    def _construct_payload(self, hotel_name, city):
        """构建GraphQL请求体"""
        return json.dumps([{
            "operationName": "SearchLocationSuggestions",
            "variables": {
                "context": {
                    "siteId": random.randint(1, 5),
                    "locale": "zh_CN",
                    "eapid": 0,
                    "tpid": 1,
                    "currency": "USD",
                    "device": {"type": "DESKTOP"},
                    "identity": {
                        # "duaid": self._generate_uuid(),
                        "duaid": 'b0d60ce8-8f5c-4432-88f2-f7550ae989e2',
                        "authState": "ANONYMOUS"
                    },
                    "privacyTrackingState": "CAN_TRACK"
                },
                "searchLocationCriteria": {
                    "searchTerm": f"{hotel_name} {city}",
                    "lineOfBusiness": "HOTELS",
                    "isDestination": True,
                    "isGroundTransfersAirport": False
                }
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "d59763ac1db048fe52c47398da71f8da6894467cbdd78d703f0138579a3d8ee2"
                }
            }
        }])

    def search_hotels(self, hotel_name, city):
        """执行酒店搜索"""
        for attempt in range(3):
            try:
                self._rotate_identity()
                time.sleep(random.uniform(1, 3))

                response = self.session.post(
                    self.graphql_url,
                    headers=self.headers,
                    data=self._construct_payload(hotel_name, city),
                    proxies=self._get_proxy(),
                    timeout=(3, 10),
                    verify=False
                )

                if response.status_code == 403:
                    raise Exception(f"触发风控限制，状态码：{response.status_code}")

                if response.status_code != 200:
                    raise Exception(f"请求失败，状态码：{response.status_code}")

                return self._parse_response(response.json())

            except Exception as e:
                print(f"尝试 {attempt + 1}/3 失败: {str(e)}")
                if attempt == 2:
                    return None

    def _parse_response(self, data):
        """解析API响应"""
        try:
            items = data[0]['data']['searchLocation']['itemsGroups'][0]['items']
            results = []
            for item in items:
                # 增加有效性检查：确保propertyId存在且为有效数值
                if 'propertyId' in item and item['propertyId'] not in (None, 'none', ''):
                    # 进一步验证propertyId是否为数字（防止字符串形式的无效值）
                    try:
                        property_id = int(item['propertyId'])
                        hotel_id = f"h{property_id}"
                        results.append({
                            'url': f"https://www.expedia.com/hotel/h{property_id}",
                            'hotel_id': hotel_id,
                            'name': item.get('primaryText', ''),
                            'location': item.get('locationFullName', '')
                        })
                    except (ValueError, TypeError):
                        print(f"无效的propertyId: {item['propertyId']}, 已跳过")
            return results
        except KeyError as e:
            print(f"解析错误，响应结构可能已变更: {str(e)}")
            print("建议检查：1. GraphQL哈希值 2. 操作名称 3. 代理质量")
            return []


def get_db_connection():
    """创建数据库连接"""
    conn_str = f"DRIVER={DRIVER};SERVER={SERVER},{PORT};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print(f"数据库连接失败: {str(e)}")
        raise


def insert_keyword_to_db(city, name):
    """插入搜索关键词（存在则跳过）"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 
                    FROM aily 
                    WHERE city=? AND name=? AND platform='Expedia'
                )
                BEGIN
                    INSERT INTO daily
                    (city, name, platform, status) 
                    VALUES (?, ?, 'Expedia', 0)
                END
            """, (city, name, city, name))
            conn.commit()
    except pyodbc.Error as e:
        print(f"数据库插入失败: {str(e)}")
        raise


def update_status(city, name, status):
    """更新任务状态"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE daily
                SET status = ?
                WHERE city = ? AND name = ? AND platform='Expedia'
            """, (status, city, name))
            conn.commit()
    except pyodbc.Error as e:
        print(f"状态更新失败: {str(e)}")
        raise


def insert_hotel_info(task_city, result):
    """插入酒店详情"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO daily
                (task_city_name, city_name, task_hotel_name, hotel_name, hotel_id,platform,create_time,url) 
                VALUES (?, ?, ?, ?, ?, 'expedia',GETDATE(),?)
            """, (
                task_city['city'],
                result['location'],
                task_city['name'],
                result['name'],
                result['hotel_id'],
                result['url']


            ))
            conn.commit()
    except pyodbc.Error as e:
        print(f"酒店插入失败: {str(e)}")
        raise


def process_excel_data(file_path):
    """处理Excel数据"""
    try:
        df = pd.read_excel(file_path, header=None, names=['city', 'name'])
        print(f"成功读取 {len(df)} 条数据")
        return df[['city', 'name']].apply(lambda x: x.str.strip()).to_dict('records')
    except Exception as e:
        print(f"Excel处理失败: {str(e)}")
        return []


def main():
    scraper = ExpediaScraper()
    tasks = process_excel_data("new_insert.xlsx")

    if not tasks:
        return

    for task in tasks:
        city = task['city']
        name = task['name']
        print(f"\n处理任务: {city} - {name}")

        try:
            # 插入初始任务
            insert_keyword_to_db(city, name)

            # 执行搜索
            results = scraper.search_hotels(name, city)

            if results:
                for result in results:
                    insert_hotel_info(task, result)
                update_status(city, name, 2)
                print(f"成功找到 {len(results)} 个结果")
            else:
                update_status(city, name, 404)
                print("未找到有效结果")

        except Exception as e:
            print(f"处理异常: {str(e)}")
            update_status(city, name, 500)
            continue

        # 随机延迟防止请求过快
        time.sleep(random.uniform( 5,10))


if __name__ == "__main__":
    main()
