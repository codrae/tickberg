"""slides.md → tickberg-final.pptx 생성기.

실행: .venv/bin/python docs/presentation/build_pptx.py

slides.md 의 `---` 구분 33 슬라이드를 16:9 다크/테크 톤 .pptx 로 변환.
- 표지(1)·Q&A(33) 는 전용 히어로/클로징 레이아웃
- 본문 슬라이드: 좌측 accent stripe + 제목 rule + 인용구 callout + 불릿 + 코드 + footer
- **시각자료**: , **Speaker Note:** → 발표자 노트
- SCREENSHOTS 매핑 슬라이드 → dashboard/screenshots/ 이미지 임베드

.pptx 와 스크린샷은 .gitignore — 이 스크립트만 커밋 (재현 가능 빌드).
"""
from __future__ import annotations

import re
from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN
from pptx.util import Emu, Inches, Pt

ROOT = Path(__file__).resolve().parent.parent.parent
SLIDES_MD = ROOT / "docs/presentation/slides.md"
SHOTS_DIR = ROOT / "dashboard/screenshots"
OUT_PPTX = ROOT / "docs/presentation/tickberg-final.pptx"

# 슬라이드 번호 → 임베드할 스크린샷 (dashboard/screenshots/ 기준)
SCREENSHOTS: dict[int, list[str]] = {
    14: ["health_check.png"],
    15: ["검증쿼리1.png", "검증쿼리2.png", "검증쿼리3.png"],
    17: ["kpi 대시보드.png"],
    18: ["운영탭 대시보드.png"],
    25: ["grafana.png"],
}

# --- 팔레트 (다크/테크) ---
BG0 = RGBColor(0x0B, 0x0F, 0x1A)       # 배경 (deep navy)
BG1 = RGBColor(0x12, 0x18, 0x28)       # 배경 그라데이션 끝
PANEL = RGBColor(0x16, 0x1B, 0x29)     # 코드/콜아웃 패널
PANEL_BAR = RGBColor(0x23, 0x2B, 0x3D) # 코드 상단 바
ACCENT = RGBColor(0x4D, 0x9F, 0xFF)    # 강조 (blue)
ACCENT_DK = RGBColor(0x2A, 0x4A, 0x7A) # 강조 어두운 톤
TITLE = RGBColor(0xE6, 0xED, 0xF3)     # 제목 (near white)
QUOTE = RGBColor(0x9D, 0xB2, 0xCE)     # 인용구
BODY = RGBColor(0xB8, 0xC4, 0xD4)      # 본문
CODE = RGBColor(0x8D, 0xD0, 0xFF)      # 코드
DIM = RGBColor(0x55, 0x60, 0x73)       # footer/번호
FONT = "Apple SD Gothic Neo"
MONO = "Menlo"

SW, SH = Inches(13.333), Inches(7.5)
TOTAL = 33


def parse_slides(md: str) -> list[dict]:
    slides: list[dict] = []
    cur: dict | None = None
    mode = None
    in_code = False
    code_buf: list[str] = []
    for raw in md.splitlines():
        line = raw.rstrip()
        m = re.match(r"^## (.+)", line)
        if m:
            if cur:
                slides.append(cur)
            cur = {"title": m.group(1).strip(), "quote": [], "bullets": [],
                   "code": [], "visual": "", "note": []}
            mode = None
            in_code = False
            continue
        if cur is None:
            continue
        if line.strip().startswith("```"):
            if in_code:
                cur["code"].append("\n".join(code_buf))
                code_buf = []
            in_code = not in_code
            continue
        if in_code:
            code_buf.append(raw)
            continue
        if line == "---":
            mode = None
            continue
        if line.startswith("> "):
            cur["quote"].append(line[2:].strip())
        elif line.startswith("- ") and mode != "note":
            cur["bullets"].append(line[2:].strip())
        elif line.startswith("**시각자료**:"):
            cur["visual"] = line.replace("**시각자료**:", "").strip()
        elif line.startswith("**Speaker Note:**"):
            mode = "note"
        elif mode == "note" and line.strip():
            cur["note"].append(line.strip())
    if cur:
        slides.append(cur)
    return slides


def _rect(slide, shape, l, t, w, h, color, *, line=False):
    r = slide.shapes.add_shape(shape, l, t, w, h)
    r.fill.solid()
    r.fill.fore_color.rgb = color
    if line:
        r.line.color.rgb = color
    else:
        r.line.fill.background()
    r.shadow.inherit = False
    return r


def _bg(slide):
    """배경 — deep navy 그라데이션."""
    r = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, SW, SH)
    r.line.fill.background()
    r.shadow.inherit = False
    try:
        r.fill.gradient()
        stops = r.fill.gradient_stops
        stops[0].color.rgb = BG0
        stops[0].position = 0.0
        stops[1].color.rgb = BG1
        stops[1].position = 1.0
        r.fill.gradient_angle = 60.0
    except Exception:  # noqa: BLE001 — 일부 환경 gradient 미지원 시 solid fallback
        r.fill.solid()
        r.fill.fore_color.rgb = BG0
    return r


def _box(slide, l, t, w, h):
    tb = slide.shapes.add_textbox(l, t, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    return tb, tf


def _run(p, text, *, size, color, bold=False, italic=False, font=FONT):
    r = p.add_run()
    r.text = text
    r.font.size = Pt(size)
    r.font.color.rgb = color
    r.font.bold = bold
    r.font.italic = italic
    r.font.name = font
    return r


def _footer(slide, idx):
    _, ff = _box(slide, Inches(0.7), Inches(7.04), Inches(8.0), Inches(0.35))
    fp = ff.paragraphs[0]
    _run(fp, "tickberg", size=9, color=ACCENT, bold=True)
    _run(fp, "  ·  Real-time Korean Stock Tick Lakehouse", size=9, color=DIM)
    _, nf = _box(slide, Inches(11.6), Inches(7.04), Inches(1.1), Inches(0.35))
    npar = nf.paragraphs[0]
    npar.alignment = PP_ALIGN.RIGHT
    _run(npar, f"{idx:02d} / {TOTAL}", size=9, color=DIM)


def _code_panel(slide, l, t, w, code_text):
    n_lines = code_text.count("\n") + 1
    bar_h = Inches(0.16)
    body_h = min(Inches(0.205 * n_lines + 0.24), Inches(2.5))
    # 터미널 상단 바
    bar = _rect(slide, MSO_SHAPE.ROUNDED_RECTANGLE, l, t, w, bar_h + Inches(0.05),
                PANEL_BAR)
    bf = bar.text_frame
    bf.margin_left = Inches(0.12)
    bf.margin_top = bf.margin_bottom = Emu(0)
    _run(bf.paragraphs[0], "● ● ●", size=7, color=DIM)
    # 코드 본문
    panel = _rect(slide, MSO_SHAPE.RECTANGLE, l, t + bar_h, w, body_h, PANEL)
    ctf = panel.text_frame
    ctf.word_wrap = True
    ctf.margin_left = ctf.margin_right = Inches(0.16)
    ctf.margin_top = ctf.margin_bottom = Inches(0.1)
    _run(ctf.paragraphs[0], code_text, size=10, color=CODE, font=MONO)
    return t + bar_h + body_h


def render_cover(slide, s):
    _bg(slide)
    # 중앙 accent rule
    _rect(slide, MSO_SHAPE.RECTANGLE, Inches(5.27), Inches(2.5),
          Inches(2.8), Inches(0.045), ACCENT)
    _, tf = _box(slide, Inches(1.0), Inches(2.7), Inches(11.33), Inches(1.5))
    tp = tf.paragraphs[0]
    tp.alignment = PP_ALIGN.CENTER
    _run(tp, "tickberg", size=66, color=TITLE, bold=True)
    # subtitle (quote)
    _, sf = _box(slide, Inches(1.0), Inches(4.15), Inches(11.33), Inches(0.7))
    sp = sf.paragraphs[0]
    sp.alignment = PP_ALIGN.CENTER
    sub = s["quote"][0] if s["quote"] else ""
    sub = sub.replace("tickberg — ", "")
    _run(sp, sub, size=19, color=ACCENT)
    # meta 정보 (bullets)
    _, mf = _box(slide, Inches(1.0), Inches(5.05), Inches(11.33), Inches(1.6))
    for i, b in enumerate(s["bullets"]):
        mp = mf.paragraphs[0] if i == 0 else mf.add_paragraph()
        mp.alignment = PP_ALIGN.CENTER
        mp.space_after = Pt(6)
        _run(mp, b, size=13, color=QUOTE)


def render_closing(slide, s):
    _bg(slide)
    _rect(slide, MSO_SHAPE.RECTANGLE, Inches(6.07), Inches(1.5),
          Inches(1.2), Inches(0.045), ACCENT)
    _, tf = _box(slide, Inches(1.0), Inches(1.7), Inches(11.33), Inches(1.3))
    tp = tf.paragraphs[0]
    tp.alignment = PP_ALIGN.CENTER
    _run(tp, "Q & A", size=54, color=TITLE, bold=True)
    if s["quote"]:
        _, qf = _box(slide, Inches(1.0), Inches(3.0), Inches(11.33), Inches(0.6))
        qp = qf.paragraphs[0]
        qp.alignment = PP_ALIGN.CENTER
        _run(qp, s["quote"][0], size=15, color=ACCENT, italic=True)
    # 예상 질문 목록
    _, bf = _box(slide, Inches(2.6), Inches(3.8), Inches(8.13), Inches(2.8))
    for i, b in enumerate(s["bullets"]):
        bp = bf.paragraphs[0] if i == 0 else bf.add_paragraph()
        bp.space_after = Pt(9)
        _run(bp, "▸  ", size=14, color=ACCENT, bold=True)
        _run(bp, b, size=14, color=BODY)
    _footer(slide, TOTAL)


def render_content(slide, idx, s):
    _bg(slide)
    # 좌측 accent stripe
    _rect(slide, MSO_SHAPE.RECTANGLE, 0, 0, Inches(0.1), SH, ACCENT)
    has_shot = idx in SCREENSHOTS

    # 제목
    _, tf = _box(slide, Inches(0.7), Inches(0.42), Inches(11.9), Inches(0.95))
    _run(tf.paragraphs[0], s["title"], size=27, color=TITLE, bold=True)
    # 제목 아래 accent rule
    _rect(slide, MSO_SHAPE.RECTANGLE, Inches(0.72), Inches(1.18),
          Inches(0.62), Inches(0.04), ACCENT)

    y = Inches(1.42)
    # 인용구 callout — 좌측 accent bar + 텍스트
    if s["quote"]:
        qtext = "  ".join(s["quote"])
        _rect(slide, MSO_SHAPE.RECTANGLE, Inches(0.72), y,
              Inches(0.045), Inches(0.62), ACCENT)
        _, qf = _box(slide, Inches(0.95), y - Inches(0.04),
                     Inches(11.6), Inches(0.72))
        _run(qf.paragraphs[0], qtext, size=15, color=QUOTE, italic=True)
        y += Inches(0.92)

    body_w = Inches(5.7) if has_shot else Inches(11.9)

    # 불릿
    if s["bullets"]:
        _, bf = _box(slide, Inches(0.72), y, body_w, Inches(4.6))
        for i, b in enumerate(s["bullets"]):
            p = bf.paragraphs[0] if i == 0 else bf.add_paragraph()
            p.space_after = Pt(9)
            p.line_spacing = 1.08
            _run(p, "▪  ", size=13, color=ACCENT, bold=True)
            _run(p, b, size=14, color=BODY)

    # 코드블록
    if s["code"]:
        code_top = y if not s["bullets"] else Inches(5.0)
        _code_panel(slide, Inches(0.72), code_top, body_w,
                    "\n\n".join(s["code"]))

    # 스크린샷
    if has_shot:
        files = [SHOTS_DIR / f for f in SCREENSHOTS[idx] if (SHOTS_DIR / f).exists()]
        sl, sw = Inches(6.7), Inches(6.0)
        if len(files) == 1:
            # accent 테두리 느낌 — 살짝 큰 어두운 박스 위에 이미지
            _rect(slide, MSO_SHAPE.RECTANGLE, sl - Inches(0.04), y - Inches(0.04),
                  sw + Inches(0.08), Inches(4.7), ACCENT_DK)
            slide.shapes.add_picture(str(files[0]), sl, y, width=sw)
        elif files:
            each_h = Inches(4.5 / len(files))
            for i, f in enumerate(files):
                slide.shapes.add_picture(
                    str(f), sl, Emu(int(y) + int(each_h) * i),
                    height=Emu(int(each_h) - Inches(0.12)))

    _footer(slide, idx)


def build():
    slides_data = parse_slides(SLIDES_MD.read_text(encoding="utf-8"))
    prs = Presentation()
    prs.slide_width, prs.slide_height = SW, SH
    blank = prs.slide_layouts[6]

    for idx, s in enumerate(slides_data, start=1):
        slide = prs.slides.add_slide(blank)
        if idx == 1:
            render_cover(slide, s)
        elif idx == TOTAL:
            render_closing(slide, s)
        else:
            render_content(slide, idx, s)
        # 발표자 노트
        notes = slide.notes_slide.notes_text_frame
        parts = []
        if s["visual"]:
            parts.append(f"[시각자료] {s['visual']}")
        if s["note"]:
            parts.append("\n".join(s["note"]))
        notes.text = "\n\n".join(parts)

    prs.save(OUT_PPTX)
    print(f"saved {OUT_PPTX} ({len(slides_data)} slides)")


if __name__ == "__main__":
    build()
