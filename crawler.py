"""
TheStar Nation 뉴스 크롤러 (Firecrawl 버전)
- Firecrawl로 Nation 뉴스 수집 → Claude로 한국어 번역 → Google Sheets 저장
- 환경변수:
    FIRECRAWL_API_KEY     : Firecrawl API 키
    GOOGLE_CREDENTIALS_BASE64 : 서비스 계정 JSON을 base64 인코딩한 값
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
NATION_URL   = "https://www.thestar.com.my/news/nation"
SHEET_NAME   = "TheStar Nation News"
TZ           = pytz.timezone("Asia/Kuala_Lumpur")
# ─────────────────────────────────────────────────────


def get_credentials_file() -> str:
    b64 = os.environ.get("GOOGLE_CREDENTIALS_BASE64", "")
    if not b64:
        local = os.path.join(os.path.dirname(__file__), "google_credentials.json")
        if os.path.exists(local):
            return local
        raise RuntimeError("GOOGLE_CREDENTIALS_BASE64 환경변수 또는 google_credentials.json 필요")
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    tmp.write(base64.b64decode(b64).decode("utf-8"))
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
    return link in ws.col_values(5)  # E열 = 링크


def translate(client: Anthropic, text: str) -> str:
    if not text or not text.strip():
        return ""
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": (
                "Translate the following English text to Korean. "
                "Return only the Korean translation, no explanation.\n\n"
                + text[:800]
            ),
        }],
    )
    return resp.content[0].text.strip()


def fetch_articles() -> list[dict]:
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

    rows = []
    for article in articles:
        link = article.get("url", "")
        if already_saved(ws, link):
            print(f"    skip (중복): {link}")
            continue

        title_en   = article.get("title", "").strip()
        summary_en = article.get("summary", "").strip()
        pub_date   = article.get("published_date", now.strftime("%Y-%m-%d"))

        title_kr   = translate(anthropic, title_en)
        summary_kr = translate(anthropic, summary_en)

        rows.append([pub_date, title_en, title_kr, summary_kr, link, collected_at])
        time.sleep(0.3)

    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"✓ {len(rows)}개 기사 저장 완료")
    else:
        print("새 기사 없음 (모두 중복)")

    if "tmp" in creds_file:
        os.unlink(creds_file)


if __name__ == "__main__":
    run()
