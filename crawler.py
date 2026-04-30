"""
TheStar Nation 뉴스 크롤러 (Google News RSS 버전)
- Google News RSS 수집 → Claude 번역 + 분류 → Google Sheets 저장
"""

import os
import base64
import tempfile
from datetime import datetime

import pytz
import feedparser
import gspread
from google.oauth2.service_account import Credentials
from anthropic import Anthropic

RSS_URL    = "https://news.google.com/rss/search?q=site:thestar.com.my+nation&hl=en-MY&gl=MY&ceid=MY:en"
SHEET_NAME = "TheStar Nation News"
TZ         = pytz.timezone("Asia/Kuala_Lumpur")
MAX_ITEMS  = 20

CATEGORIES = ["정치", "경제", "사회", "범죄", "보건", "교육", "환경", "외교", "기타"]

_GCREDS_B64 = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAidHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMSIsCiAgInByaXZhdGVfa2V5X2lkIjogIjhjODliMzRjMThiZDFkN2ZkNzllMWUxOTNiZDNiYjE5NzMwZjNhMzYiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRQy9sT0tIdTZCOG9iN3FcbnU4V1lTZDI4U0hjSTluWk5KTlBuSmRhcjdDdXdMSVdnOTRGc3Rza3BMNUxidy9iQ01IOFFTNmdDTlFTY1VnTDhcbmRWSTF5Rkh4ejk1cTBYaUdPSzVneDRPSmFGMWkwSDZoZHdpNWhpVHF6eHU3NkEwL3d2V0tWWHRRM2diSVQvTlRcbk4yNnBEcEp6bDFCcm1rVlJVRmdvVjFkL0xWRjJ0d0hUZ3pXaTU2bit6bHF5dDZqZjQxbXVuckMwU3R0YjM5UTJcbndqc1BrWXQvYXVNbFFUTDJaa1dNRTN3d3E5WGRmekV3Z3NqSTRqOERPeTF5U2lnQ0oxQTZibWh6b2lLUUNJblBcbkQyT3hlT2hheWJ3S1JXTlFlNHF4UjVLYmhOa3F5ZHU1TzJtNDZSM0Z4ZXRZSmxuMWovbjVtRjI5dkxVRU5IdDVcbk9LcmI0WmJOQWdNQkFBRUNnZ0VBS1BrZVhxR2ozK3ZXMFV5UjVMRk5qUzVoV09jZkkzNFFITlEwbm9YT3pTbWtcbmRhbmhnZjd3dEZDQ28rSThGTWw2NzJFQzRGLzI2YmpIZnpkWE92M3ArcUcybzVsRG9jOGZDajg1VkVxZ0NGbG5cbmI4QnR1d0hqeE4xQkJQWEsyWnRvV0tyU0NoaEdFcEs0eTFBa2FGOGZ0cjJDbTFTOTVQZGVOMWlBczhnZEwxcHlcbm1FTVdwSkJteW1HT3VSeGhaZWI5Nk5QRWZKMk5nNktnMVNhd3NIajlwZE5sbllJQVVlWmVVY1RuNUlnR1ZOYURcbk1Hb3Y4M01DNERwQ0tXNUVOb3ZiUlVsbHM0Q1ZkNnJXNUpvZjhBZ1NJU3RkR2xxNnFEVmVGV3RXZ29RQXVQanRcbkxOTWtScHJoUTlBaWN1eFdIS3BYSVlXdnlaUjF6SFVWL3owbmVCamE2UUtCZ1FENDBGblhzZmV1YkI0cUpsZEVcbjF1ZkdBbmJkZXZCb3AxUVE4OXNmTGhTYXV5SnhWYXpodFdaekRHaGg1U2MyMElGWnJUakgxNlV6UTg1MEt2cjFcbnhHTFo2R2g0RUs1cytvOTlIRGZ2RkFIMGtEUzJhMGRHejhnMGdjUGxKZzJlYnBaczd6ZWxtQkd3Z3hpWkpNWXJcbnpVWUpiQWZETS9laHVicC9vV2RoOWEzZHhRS0JnUURGSFdDQUNKM1hPTEZQbmlhTUFIYmV6V09oamtxdHladGRcbnBFQ1JIUG9TczNPUVNFbVQzS0MzQlpSbG42SFkxbTNmRHJNbWZ3cFBiVmR0NlVERHdPYWJRZjBMWHA5bW55M3Rcbk1xeTA2c0tLcUFvYk1TbDY3NXlCV0E3MnpGRmVDQ014WTJ2MWNuVk5saFZ5Wm1SMVI0UjNxdEJ5T1RnQzkvRDJcbjFsSldpSjR0YVFLQmdCSytWMVpKWU5neDZ0L1AvWmtBKzNyT0tyZ3FqN3ZDaHpHenZSa1BOcHdsWnNYVTUyNGxcblpTOWJpdTE4L3NSQlZzMHpvR2hsbFZ0VVMxUXkvSzdRK1lWaTNhUFYxZDM5emh0bHFjMEgyOUhabnk3eXkrN0lcbnJsTk9SenlXN2tXMkhQemQxSHBVQjFrZHR4ZXFUb2QwTWtkNWJPaEduT3dBK3N5c2t3WXRKTWpsQW9HQkFNSzRcbmU0emZkaldYSHFuZFBJMFBKRjZESFVvS1M4R1VLTFAvdjN6YTJEdERKUVZDTTlVcW5XVlAwTUgzU1NYdzVnYjBcbitZc3M4cDEyRVdsVmhCSWM3SEl4Wk8wblkyWTRGMkY3cnRybUVwcHcxOUwyNU5nNS9peFVOaXdkdUwrZ1lFdTRcbndIeHV2bXlQdDg0VVVtYjg2R0ppenBvWDVqb2dmdDBJNmJ2c0ovNEpBb0dCQUwreWVTMFFTWFBXVndMTm8rK1lcblU5a1JLVEpJTXJERUVva2pRRlJUWi9aRXl3em5sR29pc3BNYlV6QmUyR01RSk1RQjgwaUN2Q1dodXBsdmFvOXVcbkowVGlBMnN5UlB0QmtOdzBwZE81RGhwRC9GSGFCY1pvdkZIU3EwUzdld0w1TEVneEdmQUloSVozbVk0dGtrMVRcbnJKbEcrc0dUSFhiZlJJM1hsQlgreXNWb1xuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogImNsYXVkZS1jb2RlLWFwaUB0cnVzdHktc2xhdGUtNDk0MDE0LWExLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwMzY3OTg1MDc5NTUxOTQ1OTQ5NyIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvY2xhdWRlLWNvZGUtYXBpJTQwdHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMS5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo="


def get_sheet():
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    tmp.write(base64.b64decode(_GCREDS_B64).decode("utf-8"))
    tmp.close()
    creds = Credentials.from_service_account_file(tmp.name, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    os.unlink(tmp.name)
    gc = gspread.authorize(creds)
    try:
        sh = gc.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)
    ws = sh.sheet1
    if not ws.get_all_values() or ws.cell(1, 1).value != "날짜":
        ws.insert_row(
            ["날짜", "분류", "제목 (EN)", "제목 (KR)", "요약 (KR)", "링크", "수집시각"],
            index=1
        )
    return ws


def already_saved(ws, link):
    return link in ws.col_values(6)  # 링크가 6번째 열로 이동


def fetch_articles():
    feed = feedparser.parse(RSS_URL)
    articles = []
    for entry in feed.entries[:MAX_ITEMS]:
        title = entry.get("title", "").replace(" - The Star", "").strip()
        articles.append({
            "title":          title,
            "summary":        entry.get("summary", title),
            "url":            entry.get("link", ""),
            "published_date": entry.get("published", ""),
        })
    return articles


def translate_and_classify(client, articles):
    """번역 + 분류를 한 번의 API 호출로 처리"""
    if not articles:
        return []

    cat_list = ", ".join(CATEGORIES)
    lines = []
    for i, a in enumerate(articles, 1):
        lines.append(f"[{i}] TITLE: {a['title'][:200]}")
        lines.append(f"[{i}] SUMMARY: {a['summary'][:400]}")

    prompt = (
        f"For each article: translate TITLE and SUMMARY to Korean, and classify into one of: {cat_list}\n\n"
        "Return ONLY in this exact format (no extra text):\n"
        "[1] CATEGORY: 정치\n[1] TITLE: 한국어제목\n[1] SUMMARY: 한국어요약\n[2] CATEGORY: ...\n\n"
        + "\n".join(lines)
    )

    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    results = [{"category": "기타", "title_kr": "", "summary_kr": ""} for _ in articles]
    for line in resp.content[0].text.strip().splitlines():
        line = line.strip()
        for i in range(1, len(articles) + 1):
            if line.startswith(f"[{i}] CATEGORY:"):
                results[i-1]["category"] = line[len(f"[{i}] CATEGORY:"):].strip()
            elif line.startswith(f"[{i}] TITLE:"):
                results[i-1]["title_kr"] = line[len(f"[{i}] TITLE:"):].strip()
            elif line.startswith(f"[{i}] SUMMARY:"):
                results[i-1]["summary_kr"] = line[len(f"[{i}] SUMMARY:"):].strip()
    return results


def run():
    now = datetime.now(TZ)
    collected_at = now.strftime("%Y-%m-%d %H:%M MYT")
    print(f"[{collected_at}] 크롤링 시작")

    ws = get_sheet()
    articles = fetch_articles()
    print(f"  전체 기사: {len(articles)}개")

    new_articles = [a for a in articles if not already_saved(ws, a["url"])]
    print(f"  신규 기사: {len(new_articles)}개")

    if not new_articles:
        print("새 기사 없음")
        return

    results = translate_and_classify(Anthropic(), new_articles)
    rows = []
    for article, r in zip(new_articles, results):
        rows.append([
            article["published_date"],
            r["category"],
            article["title"],
            r["title_kr"],
            r["summary_kr"],
            article["url"],
            collected_at,
        ])

    ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"✓ {len(rows)}개 기사 저장 완료!")
    for r in results:
        print(f"  [{r['category']}] {r['title_kr'][:40]}")


if __name__ == "__main__":
    run()
