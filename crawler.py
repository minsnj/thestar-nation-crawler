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
_GCREDS_B64 = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAidHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMSIsCiAgInByaXZhdGVfa2V5X2lkIjogIjY1Y2NjYzU5YmY2NjliMGM0MmQzM2I0YzYyZDljN2Q3MmIwZDE0ODYiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2QUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktZd2dnU2lBZ0VBQW9JQkFRRHdlNDRvK1ZhQmcvMjJcbk8vc3g0eHFzOU95Z3VjVXlqaFZCNlRKSTN1S1lsWVNWRHdGeEdzWlNkNWg4TUdYcHdZNkVuN3RJKys5R085T3BcbnlsUHF3UVk3N0NMcnMvUlNxTFlHTVdmSVU5WnpxY0pxbWtOWW44bDRYVmhvdjVEeG5mSVV5SFZMSFRtajE0eE1cbk5Ick10bHFXaUEwc0ltWGFBZ1lScGlGSkJNNTZPUW1sRUVjVjgzRWRqbkdvRnRkeUpmT05aT3A3Q01IK1Vwck9cbmdvR0tBeUhSMFFHME9rNkhjSDdmS0RMc0s1Qmt6MHpzQzYxelRrNTFvSHN4NDZlY1czYU5CSHVyT0ZBUXVYQzlcbkxxbkZuME12YUhJTDZ5QWIrcitXb0svTWdDeU1JRzJZRk5WWTdhNngya3VHN0wxY0NmZ1JmMENHNGtmWEptVWhcbnFMUmxRbEFuQWdNQkFBRUNnZ0VBRXBsYnUxWXl3ZWhBSFJ5eWZDbnJ0dWk5SWJQUnlVRXRQN1hBeW9ZQ2ZIbERcbjV5QzJwSUFhU3BLRUJwd3ZwYVB2V1M2SmE1eExPemF4VTNadk1HbDhDVWU2dzY2eXAyOUk5TEwrMWRnMkdTZjRcbnFMU3U5THhzUytveG01aGFwaC9vY3l4Z2RmakNxOUNsN0lHUDVjNmlDYUVTeVJFTlU0NXpMYlpDNk1UVEVLZG1cbkNFSXFVdFZta3JBcEdBS2VrNHR1bjhBdXNpSFlaUUJ0YW1IZVFzQlU3QXFlWjdBcmMvUkwxRk9kemswTHpxTnpcbisvQmoxeXdsVWdOKzIrVWRSaXRSTkZwMGkvVUJiMjVkdVIzRkFVTVNVWTBtOWNLN1h0bG1SaThLTjhoa3BwbkhcbnZyT0g4UTQ2a0djdW5JS2NxNkFqOVE1ZS9ZWGZQWVZBSTNmT1NSQXdZUUtCZ1FENGNkMlpIL2F1ejhuSTdKQXdcblg0VWJIQzVZTzNsVXpCMm1xRHJFeEdmanhRSGxiRHU1WHhrOC9yM1BqTVg3YnNwak1DdmI3UFhwbDBLbTl3SjNcbng0L3NUdWh1QnJ3YTZIbVQ4QzVkeEV6aXdqYmVEcFdCdDEyOW0vR1hWTFNyeDFXLytCWldOc2FsU2hQbXhsTUtcbjlvT2M3NEprMnRucTY0Z1A5VHR6UnhvWHFRS0JnUUQzeTdSaVdsY3lCUGZpTXoyWFBUaTUra2ZKeWhhNUxxbmZcbktsSy92TUUwdk81ZVFyOFd5TXVpakV6STVRYitxNEpXNUZ5TDh2Zm01Y3pkaFRGVStRZGZCcHA1em0yWGE2c3VcblJ1SEFVZHZYc082L3lIUDNhQXJpOUtob2tDenN2QjNUODZ0VXBRNXhTQ3U1dURqTXR4b29vaDltOExZaU9wNk5cblRPTnJCeUxMVHdLQmdGQlBkb09tS25iTjRudVp4TzV1SWpmbVB6RDBZTDlCa0NBc1ozcnR4bXVCWnFDRUFUWm1cbkFHR1FNMGxoUnlxRTROVjVYK1FpMEVkblJ2dDBBNDgycWxhSUYyaGhzdks3elhrOS9hNy80cDYyaTBmeXpPOTJcbmw4M1FHQ09FRlRjbUk1ai9tRERjV2hCYVJ0NmxvM2g2d1lhOGdaa2FpYkQvM0NiVWJoaFFscVdwQW9HQUQ4Z3Jcbmp2QU9DYU9EWGliQlQzYVl2RStTYnVtdUZORkNCSEtmbnBLWkE1RGM1YWYrbjZiZVFWWUtOZWxRVHJ3QnF5TVVcbk9kMlpxRjBPZFRWY2RQci9XekFDZXF1SkUxSEtMMEpZY25WRU9US05vaTFVSlhlODZjT0hUbEdRYXV0NFF0Mk1cbkZmSUZQM1hIUXliV2gwTktLVGhhSTZaRkUxMEVhQjl6aEllSjBjVUNnWUJxUEtvbExZcE92UzNrRG5DcE5La21cbkRuM2ZKR1doeHZ5YzBNOUpMaWhyMEFLUXlYZFY1ZXN6dHJLR0Y2MDNCSms3QWJ0ZzcyYzRSYWdCUkZlVk5QeWhcbjB1VlR0dEx5L0sxL01qUDZ4eWNRRkxNa2tLalZGQ1FXVTVQSnJMMFlrUkg0bjlpOVRqT1RQUlc4NUU1R1VCQjlcblMvMEdvRzcxVm1SMkhGejJoaUYwcHc9PVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogImNsYXVkZS1jb2RlLWFwaUB0cnVzdHktc2xhdGUtNDk0MDE0LWExLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwNjg1NDgwNDk2OTE5NzE0ODM2OCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvY2xhdWRlLWNvZGUtYXBpJTQwdHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMS5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo="
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
