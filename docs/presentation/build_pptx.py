"""slides.md → tickberg-final.pptx 생성기.

실행: .venv/bin/python docs/presentation/build_pptx.py

slides.md 의 `---` 구분 33 슬라이드를 파싱해 16:9 다크/테크 톤 .pptx 로 변환.
- 제목 / 인용구(>) / 불릿(-) / 코드블록(```) → 슬라이드 본문
- **시각자료**: , **Speaker Note:** → 발표자 노트
- SCREENSHOTS 매핑된 슬라이드 → dashboard/screenshots/ 이미지 임베드

.pptx 와 스크린샷은 .gitignore — 이 스크립트만 커밋 (재현 가능 빌드).
"""
from __future__ import annotations

import re
from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

ROOT = Path(__file__).resolve().parent.parent.parent
SLIDES_MD = ROOT / "docs/presentation/slides.md"
SHOTS_DIR = ROOT / "dashboard/screenshots"
OUT_PPTX = ROOT / "docs/presentation/tickberg-final.pptx"

# 슬라이드 번호 → 임베드할 스크린샷 파일 (dashboard/screenshots/ 기준)
SCREENSHOTS: dict[int, list[str]] = {
    14: ["health_check.png"],
    15: ["검증쿼리1.png", "검증쿼리2.png", "검증쿼리3.png"],
    17: ["kpi 대시보드.png"],
    18: ["운영탭 대시보드.png"],
    25: ["grafana.png"],
}

# --- 디자인 (다크/테크 톤) ---
BG = RGBColor(0x0D, 0x11, 0x17)        # GitHub dark
PANEL = RGBColor(0x16, 0x1B, 0x22)     # 코드블록 배경
ACCENT = RGBColor(0x58, 0xA6, 0xFF)    # 제목 강조 (blue)
QUOTE = RGBColor(0x8B, 0x94, 0x9E)     # 인용구 (gray)
BODY = RGBColor(0xC9, 0xD1, 0xD9)      # 본문 (light gray)
CODE = RGBColor(0x79, 0xC0, 0xFF)      # 코드 (light blue)
DIM = RGBColor(0x6E, 0x76, 0x81)       # 슬라이드 번호 등
FONT = "Apple SD Gothic Neo"           # 한글 글꼴 (Mac)
MONO = "Menlo"

SW, SH = Inches(13.333), Inches(7.5)


def parse_slides(md: str) -> list[dict]:
    """slides.md 를 슬라이드 dict 리스트로 파싱."""
    slides: list[dict] = []
    cur: dict | None = None
    mode = None          # None | 'note'
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


def _bg(slide):
    r = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, SW, SH)
    r.fill.solid()
    r.fill.fore_color.rgb = BG
    r.line.fill.background()
    r.shadow.inherit = False
    return r


def _box(slide, left, top, width, height):
    tb = slide.shapes.add_textbox(left, top, width, height)
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


def build():
    slides_data = parse_slides(SLIDES_MD.read_text(encoding="utf-8"))
    prs = Presentation()
    prs.slide_width, prs.slide_height = SW, SH
    blank = prs.slide_layouts[6]

    for idx, s in enumerate(slides_data, start=1):
        slide = prs.slides.add_slide(blank)
        _bg(slide)
        has_shot = idx in SCREENSHOTS

        # --- 제목 ---
        _, tf = _box(slide, Inches(0.6), Inches(0.4), Inches(12.1), Inches(1.0))
        p = tf.paragraphs[0]
        _run(p, s["title"], size=30, color=ACCENT, bold=True)

        # --- 인용구 ---
        y = Inches(1.45)
        if s["quote"]:
            _, qf = _box(slide, Inches(0.6), y, Inches(12.1), Inches(0.8))
            qp = qf.paragraphs[0]
            _run(qp, "  ".join(s["quote"]), size=16, color=QUOTE, italic=True)
            y = Inches(2.2)

        # --- 본문 영역: 스크린샷 있으면 좌 45% / 우 이미지 ---
        body_w = Inches(5.6) if has_shot else Inches(12.1)

        # --- 불릿 ---
        if s["bullets"]:
            _, bf = _box(slide, Inches(0.6), y, body_w, Inches(4.4))
            for i, b in enumerate(s["bullets"]):
                p = bf.paragraphs[0] if i == 0 else bf.add_paragraph()
                p.space_after = Pt(8)
                _run(p, "• ", size=15, color=ACCENT, bold=True)
                _run(p, b, size=15, color=BODY)

        # --- 코드블록 ---
        if s["code"]:
            code_top = y if not s["bullets"] else Inches(4.9)
            code_text = "\n\n".join(s["code"])
            n_lines = code_text.count("\n") + 1
            ch = min(Inches(0.22 * n_lines + 0.3), Inches(2.4))
            panel = slide.shapes.add_shape(
                MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.6), code_top, body_w, ch)
            panel.fill.solid()
            panel.fill.fore_color.rgb = PANEL
            panel.line.fill.background()
            panel.shadow.inherit = False
            ctf = panel.text_frame
            ctf.word_wrap = True
            ctf.margin_left = ctf.margin_right = Inches(0.15)
            ctf.margin_top = ctf.margin_bottom = Inches(0.1)
            cp = ctf.paragraphs[0]
            _run(cp, code_text, size=10.5, color=CODE, font=MONO)

        # --- 스크린샷 ---
        if has_shot:
            shot_left, shot_top, shot_w = Inches(6.5), y, Inches(6.2)
            files = [SHOTS_DIR / f for f in SCREENSHOTS[idx]]
            files = [f for f in files if f.exists()]
            if len(files) == 1:
                slide.shapes.add_picture(str(files[0]), shot_left, shot_top,
                                         width=shot_w)
            elif files:
                # 여러 장 → 세로 스택
                each_h = Inches(4.4 / len(files))
                for i, f in enumerate(files):
                    slide.shapes.add_picture(
                        str(f), shot_left, Emu(int(y) + int(each_h) * i),
                        height=Emu(int(each_h) - Inches(0.1)))

        # --- 슬라이드 번호 ---
        _, nf = _box(slide, Inches(12.4), Inches(7.05), Inches(0.8), Inches(0.35))
        np_ = nf.paragraphs[0]
        np_.alignment = PP_ALIGN.RIGHT
        _run(np_, f"{idx}/33", size=10, color=DIM)

        # --- 발표자 노트 (시각자료 + Speaker Note) ---
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
