"""
TheStar Nation 뉴스 크롤러 (Firecrawl 버전)
- Firecrawl로 Nation 뉴스 수집 → Claude로 한국어 번역 → Google Sheets 저장
- 환경변수: FIRECRAWL_API_KEY
"""

import os
import time
import base64
import tempfile
from datetime import datetime

import pytz
import gspread
from google.oauth2.service_account import Credentials
from anthropic import Anthropic
from firecrawl import FirecrawlApp

# ── 설정 ──────────────────────────────────────────────
NATION_URL  = "https://www.thestar.com.my/news/nation"
SHEET_NAME  = "TheStar Nation News"
TZ          = pytz.timezone("Asia/Kuala_Lumpur")

# Google 서비스 계정 credentials (base64)
_GCREDS_B64 = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAidHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMSIsCiAgInByaXZhdGVfa2V5X2lkIjogIjhjODliMzRjMThiZDFkN2ZkNzllMWUxOTNiZDNiYjE5NzMwZjNhMzYiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRQy9sT0tIdTZCOG9iN3FcbnU4V1lTZDI4U0hjSTluWk5KTlBuSmRhcjdDdXdMSVdnOTRGc3Rza3BMNUxidy9iQ01IOFFTNmdDTlFTY1VnTDhcbmRWSTF5Rkh4ejk1cTBYaUdPSzVneDRPSmFGMWkwSDZoZHdpNWhpVHF6eHU3NkEwL3d2V0tWWHRRM2diSVQvTlRcbk4yNnBEcEp6bDFCcm1rVlJVRmdvVjFkL0xWRjJ0d0hUZ3pXaTU2bit6bHF5dDZqZjQxbXVuckMwU3R0YjM5UTJcbndqc1BrWXQvYXVNbFFUTDJaa1dNRTN3d3E5WGRmekV3Z3NqSTRqOERPeTF5U2lnQ0oxQTZibWh6b2lLUUNJblBcbkQyT3hlT2hheWJ3S1JXTlFlNHF4UjVLYmhOa3F5ZHU1TzJtNDZSM0Z4ZXRZSmxuMWovbjVtRjI5dkxVRU5IdDVcbk9LcmI0WmJOQWdNQkFBRUNnZ0VBS1BrZVhxR2ozK3ZXMFV5UjVMRk5qUzVoV09jZkkzNFFITlEwbm9YT3pTbWtcbmRhbmhnZjd3dEZDQ28rSThGTWw2NzJFQzRGLzI2YmpIZnpkWE92M3ArcUcybzVsRG9jOGZDajg1VkVxZ0NGbG5cbmI4QnR1d0hqeE4xQkJQWEsyWnRvV0tyU0NoaEdFcEs0eTFBa2FGOGZ0cjJDbTFTOTVQZGVOMWlBczhnZEwxcHlcbm1FTVdwSkJteW1HT3VSeGhaZWI5Nk5QRWZKMk5nNktnMVNhd3NIajlwZE5sbllJQVVlWmVVY1RuNUlnR1ZOYURcbk1Hb3Y4M01DNERwQ0tXNUVOb3ZiUlVsbHM0Q1ZkNnJXNUpvZjhBZ1NJU3RkR2xxNnFEVmVGV3RXZ29RQXVQanRcbkxOTWtScHJoUTlBaWN1eFdIS3BYSVlXdnlaUjF6SFVWL3owbmVCamE2UUtCZ1FENDBGblhzZmV1YkI0cUpsZEVcbjF1ZkdBbmJkZXZCb3AxUVE4OXNmTGhTYXV5SnhWYXpodFdaekRHaGg1U2MyMElGWnJUakgxNlV6UTg1MEt2cjFcbnhHTFo2R2g0RUs1cytvOTlIRGZ2RkFIMGtEUzJhMGRHejhnMGdjUGxKZzJlYnBaczd6ZWxtQkd3Z3hpWkpNWXJcbnpVWUpiQWZETS9laHVicC9vV2RoOWEzZHhRS0JnUURGSFdDQUNKM1hPTEZQbmlhTUFIYmV6V09oamtxdHladGRcbnBFQ1JIUG9TczNPUVNFbVQzS0MzQlpSbG42SFkxbTNmRHJNbWZ3cFBiVmR0NlVERHdPYWJRZjBMWHA5bW55M3Rcbk1xeTA2c0tLcUFvYk1TbDY3NXlCV0E3MnpGRmVDQ014WTJ2MWNuVk5saFZ5Wm1SMVI0UjNxdEJ5T1RnQzkvRDJcbjFsSldpSjR0YVFLQmdCSytWMVpKWU5neDZ0L1AvWmtBKzNyT0tyZ3FqN3ZDaHpHenZSa1BOcHdsWnNYVTUyNGxcblpTOWJpdTE4L3NSQlZzMHpvR2hsbFZ0VVMxUXkvSzdRK1lWaTNhUFYxZDM5emh0bHFjMEgyOUhabnk3eXkrN0lcbnJsTk9SenlXN2tXMkhQemQxSHBVQjFrZHR4ZXFUb2QwTWtkNWJPaEduT3dBK3N5c2t3WXRKTWpsQW9HQkFNSzRcbmU0emZkaldYSHFuZFBJMFBKRjZESFVvS1M4R1VLTFAvdjN6YTJEdERKUVZDTTlVcW5XVlAwTUgzU1NYdzVnYjBcbitZc3M4cDEyRVdsVmhCSWM3SEl4Wk8wblkyWTRGMkY3cnRybUVwcHcxOUwyNU5nNS9peFVOaXdkdUwrZ1lFdTRcbndIeHV2bXlQdDg0VVVtYjg2R0ppenBvWDVqb2dmdDBJNmJ2c0ovNEpBb0dCQUwreWVTMFFTWFBXVndMTm8rK1lcblU5a1JLVEpJTXJERUVva2pRRlJUWi9aRXl3em5sR29pc3BNYlV6QmUyR01RSk1RQjgwaUN2Q1dodXBsdmFvOXVcbkowVGlBMnN5UlB0QmtOdzBwZE81RGhwRC9GSGFCY1pvdkZIU3EwUzdld0w1TEVneEdmQUloSVozbVk0dGtrMVRcbnJKbEcrc0dUSFhiZlJJM1hsQlgreXNWb1xuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogImNsYXVkZS1jb2RlLWFwaUB0cnVzdHktc2xhdGUtNDk0MDE0LWExLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwMzY3OTg1MDc5NTUxOTQ1OTQ5NyIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvY2xhdWRlLWNvZGUtYXBpJTQwdHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMS5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo="
# ─────────────────────────────────────────────────────


def get_credentials_file() -> str:
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    tmp.write(base64.b64decode(_GCREDS_B64).decode("utf-8"))
    tmp.close()
    return tmp.name


def get_or_create_sheet(creds_file: str):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    gc    = gspread.authorize(creds)
    try:
        sh = gc.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)

    ws = sh.sheet1
    if ws.row_count == 0 or ws.cell(1, 1).value != "날짜":
        ws.insert_row(
            ["날짜", "제목 (EN)", "제목 (KR)", "요약 (KR)", "링크", "수집시각"],
            index=1,
        )
    return ws


def already_saved(ws, link: str) -> bool:
    return link in ws.col_values(5)


def translate_batch(client: Anthropic, articles: list) -> list:
    """제목+요약을 한 번의 API 호출로 전부 번역 (속도 최적화)"""
    if not articles:
        return []

    # 번호 매겨서 한꺼번에 보내기
    lines = []
    for i, a in enumerate(articles, 1):
        lines.append(f"[{i}] TITLE: {a.get('title', '')[:200]}")
        lines.append(f"[{i}] SUMMARY: {a.get('summary', '')[:400]}")

    prompt = (
        "Translate each TITLE and SUMMARY to Korean. "
        "Return ONLY in this exact format, nothing else:\n"
        "[1] TITLE: 한국어제목\n[1] SUMMARY: 한국어요약\n[2] TITLE: ...\n\n"
        + "\n".join(lines)
    )

    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    # 파싱
    results = [{"title_kr": "", "summary_kr": ""} for _ in articles]
    for line in resp.content[0].text.strip().splitlines():
        line = line.strip()
        for i in range(1, len(articles) + 1):
            if line.startswith(f"[{i}] TITLE:"):
                results[i - 1]["title_kr"] = line[len(f"[{i}] TITLE:"):].strip()
            elif line.startswith(f"[{i}] SUMMARY:"):
                results[i - 1]["summary_kr"] = line[len(f"[{i}] SUMMARY:"):].strip()
    return results


def fetch_articles() -> list:
    app = FirecrawlApp(api_key=os.environ["FIRECRAWL_API_KEY"])
    result = app.scrape_url(
        NATION_URL,
        formats=["json"],
        json_options={
            "prompt": (
                "Extract all Nation news articles. "
                "For each article get: title, summary, url, published_date."
            )
        },
        only_main_content=True,
    )
    return result.json.get("articles", []) if result.json else []


def run():
    now          = datetime.now(TZ)
    collected_at = now.strftime("%Y-%m-%d %H:%M MYT")
    print(f"[{collected_at}] 크롤링 시작")

    creds_file = get_credentials_file()
    anthropic  = Anthropic()
    ws         = get_or_create_sheet(creds_file)

    articles = fetch_articles()
    print(f"  수집된 기사: {len(articles)}개")

    # 중복 제거
    new_articles = [a for a in articles if not already_saved(ws, a.get("url", ""))]
    print(f"  신규 기사: {len(new_articles)}개")

    if not new_articles:
        print("새 기사 없음")
        os.unlink(creds_file)
        return

    # 한 번에 전체 번역
    translations = translate_batch(anthropic, new_articles)

    rows = []
    for article, tr in zip(new_articles, translations):
        pub_date = article.get("published_date", now.strftime("%Y-%m-%d"))
        rows.append([
            pub_date,
            article.get("title", "").strip(),
            tr["title_kr"],
            tr["summary_kr"],
            article.get("url", ""),
            collected_at,
        ])

    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"✓ {len(rows)}개 기사 저장 완료")
    else:
        print("새 기사 없음")

    os.unlink(creds_file)


if __name__ == "__main__":
    run()
