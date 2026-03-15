#!/usr/bin/env python3
"""Generate an expanded 8-Week Modern Data Engineering Roadmap PDF (~90 pages).

Includes:
- Week overview pages (existing)
- Deep-dive pages with code examples and ASCII diagrams per week
- Datasets + Books + Capstone pages
- Cookbook appendix: cheat sheets for 10 tools
"""

from fpdf import FPDF
from pygments import lex
from pygments.lexers import get_lexer_by_name
from pygments.token import Token

# Paths to system fonts
_ARIAL_REGULAR = "/System/Library/Fonts/Supplemental/Arial.ttf"
_ARIAL_BOLD = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
_ARIAL_ITALIC = "/System/Library/Fonts/Supplemental/Arial Italic.ttf"
_ARIAL_BOLD_ITALIC = "/System/Library/Fonts/Supplemental/Arial Bold Italic.ttf"
_ARIAL_UNICODE = "/Library/Fonts/Arial Unicode.ttf"
_ANDALE_MONO = "/System/Library/Fonts/Supplemental/Andale Mono.ttf"

# Font family names
F = "arial"
MONO = "mono"

# Syntax highlighting: token type -> RGB (VS Code-inspired light theme)
_TOKEN_COLORS = {
    Token.Keyword:              (0, 0, 200),       # blue
    Token.Keyword.Namespace:    (0, 0, 200),
    Token.Keyword.Constant:     (0, 0, 200),
    Token.Keyword.Type:         (0, 0, 200),
    Token.Name.Builtin:         (0, 130, 120),      # teal
    Token.Name.Function:        (120, 80, 0),        # dark yellow/brown
    Token.Name.Function.Magic:  (120, 80, 0),
    Token.Name.Decorator:       (175, 80, 0),        # orange
    Token.Name.Class:           (0, 130, 120),
    Token.Literal.String:       (163, 21, 21),       # red
    Token.Literal.String.Doc:   (163, 21, 21),
    Token.Literal.String.Interpol: (0, 0, 200),
    Token.Literal.String.Affix: (0, 0, 200),
    Token.Literal.Number:       (9, 134, 88),        # green
    Token.Literal.Number.Integer: (9, 134, 88),
    Token.Literal.Number.Float: (9, 134, 88),
    Token.Comment:              (0, 128, 0),         # green
    Token.Comment.Single:       (0, 128, 0),
    Token.Comment.Hashbang:     (0, 128, 0),
    Token.Operator.Word:        (0, 0, 200),         # blue (and, or, not, in)
}
_DEFAULT_TOKEN_COLOR = (30, 30, 30)

# Map language labels to Pygments lexer names
_LANG_TO_LEXER = {
    "python":     "python",
    "pyspark":    "python",
    "airflow":    "python",
    "sql":        "sql",
    "yaml":       "yaml",
    "hcl":        "hcl",
    "bash":       "bash",
    "dockerfile": "docker",
    "docker":     "bash",
    "toml":       "toml",
    "jinja":      "jinja",
    "git":        "bash",
    "data":       "python",
    "architecture": None,
    "project":    None,
}


def _get_token_color(token_type):
    """Walk token type hierarchy to find a matching color."""
    t = token_type
    while t:
        if t in _TOKEN_COLORS:
            return _TOKEN_COLORS[t]
        t = t.parent
    return _DEFAULT_TOKEN_COLOR


def _detect_lexer(language):
    """Try to resolve a Pygments lexer from the language label."""
    if not language:
        return None
    # Check exact match first (e.g. "Dockerfile" → docker lexer)
    low = language.lower().strip()
    if low == "dockerfile":
        try:
            return get_lexer_by_name("docker")
        except Exception:
            return None
    # Normalise: "Python — app.py" -> "python"
    lang = low.split()[0].rstrip("—-:,")
    if lang in _LANG_TO_LEXER:
        name = _LANG_TO_LEXER[lang]
        if name is None:
            return None
        try:
            return get_lexer_by_name(name)
        except Exception:
            return None
    # fallback: try the raw string
    try:
        return get_lexer_by_name(lang)
    except Exception:
        return None


class RoadmapPDF(FPDF):
    """Custom PDF with header/footer for the DE roadmap."""

    def _register_fonts(self):
        self.add_font(F, "", _ARIAL_REGULAR)
        self.add_font(F, "B", _ARIAL_BOLD)
        self.add_font(F, "I", _ARIAL_ITALIC)
        self.add_font(F, "BI", _ARIAL_BOLD_ITALIC)
        self.add_font("arialuni", "", _ARIAL_UNICODE)
        self.add_font(MONO, "", _ANDALE_MONO)

    def header(self):
        if self.page_no() > 1:
            self.set_font(F, "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 8, "Modern Data Engineering Roadmap \u2014 8-Week Sprint", align="C")
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font(F, "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    # ── helpers ──────────────────────────────────────────────────

    def section_title(self, title, r=25, g=100, b=200):
        self.set_font(F, "B", 16)
        self.set_text_color(r, g, b)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(r, g, b)
        self.set_line_width(0.6)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def sub_heading(self, text):
        self.set_font(F, "B", 12)
        self.set_text_color(50, 50, 50)
        self.cell(0, 8, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font(F, "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bullet(self, text, indent=10):
        x = self.get_x()
        self.set_font(F, "", 10)
        self.set_text_color(40, 40, 40)
        self.set_x(x + indent)
        self.cell(4, 5.5, "\u2022")
        self.multi_cell(0, 5.5, text)
        self.ln(0.5)

    def link_bullet(self, label, url, indent=10):
        x = self.get_x()
        self.set_x(x + indent)
        self.set_font(F, "", 10)
        self.set_text_color(40, 40, 40)
        self.cell(4, 5.5, chr(8226))
        self.set_font(F, "B", 10)
        self.cell(self.get_string_width(label) + 2, 5.5, label)
        self.set_font(F, "", 9)
        self.set_text_color(30, 100, 200)
        self.cell(0, 5.5, url, link=url, new_x="LMARGIN", new_y="NEXT", markdown=True)
        self.set_text_color(40, 40, 40)
        self.ln(0.5)

    def goal_box(self, goal_text):
        self.ln(2)
        self.set_fill_color(235, 245, 255)
        self.set_draw_color(25, 100, 200)
        self.set_line_width(0.4)
        x, y = self.get_x(), self.get_y()
        w = self.w - self.l_margin - self.r_margin - 4
        lines = self.multi_cell(w, 5.5, goal_text, dry_run=True, output="LINES")
        h = max(len(lines) * 5.5 + 10, 18)
        self.rect(x, y, self.w - self.l_margin - self.r_margin, h, style="DF")
        self.set_xy(x + 2, y + 3)
        self.set_font(F, "B", 10)
        self.set_text_color(25, 100, 200)
        self.cell(self.get_string_width("GOAL: ") + 1, 5.5, "GOAL: ")
        self.set_font(F, "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(w - 2, 5.5, goal_text)
        self.set_y(y + h + 3)

    def deliverable_box(self, items):
        self.ln(2)
        self.set_fill_color(240, 255, 240)
        self.set_draw_color(34, 139, 34)
        self.set_line_width(0.4)
        x, y = self.get_x(), self.get_y()
        w = self.w - self.l_margin - self.r_margin
        total_lines = 1
        for item in items:
            total_lines += max(1, len(item) // 80 + 1)
        h = total_lines * 5.5 + 10
        self.rect(x, y, w, h, style="DF")
        self.set_xy(x + 2, y + 3)
        self.set_font(F, "B", 10)
        self.set_text_color(34, 139, 34)
        self.cell(0, 5.5, "DELIVERABLES", new_x="LMARGIN", new_y="NEXT")
        for item in items:
            self.set_x(x + 6)
            self.set_font("arialuni", "", 10)
            self.set_text_color(30, 30, 30)
            self.cell(4, 5.5, "\u2611")
            self.set_font(F, "", 10)
            self.multi_cell(w - 12, 5.5, " " + item)
        self.set_y(y + h + 3)

    # ── NEW helpers ──────────────────────────────────────────────

    def code_block(self, code, language=""):
        """Render a syntax-highlighted code block with monospace font on gray background."""
        self.ln(2)
        if language:
            self.set_font(F, "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 4, language, new_x="LMARGIN", new_y="NEXT")

        self.set_font(MONO, "", 8)
        self.set_fill_color(245, 245, 245)
        self.set_draw_color(200, 200, 200)
        self.set_line_width(0.3)
        line_h = 4.2
        w = self.w - self.l_margin - self.r_margin
        raw_lines = code.split("\n")
        h = len(raw_lines) * line_h + 4

        # page break check
        if self.get_y() + h > self.h - 20:
            self.add_page()

        lexer = _detect_lexer(language)
        if lexer is None:
            # no highlighting — plain monospace
            y_start = self.get_y()
            self.set_text_color(30, 30, 30)
            self.cell(w, 2, "", fill=True, new_x="LMARGIN", new_y="NEXT")
            for line in raw_lines:
                self.cell(w, line_h, "  " + line, fill=True, new_x="LMARGIN", new_y="NEXT")
            self.cell(w, 2, "", fill=True, new_x="LMARGIN", new_y="NEXT")
            self.rect(self.l_margin, y_start, w, self.get_y() - y_start, style="D")
            self.ln(2)
            return

        # tokenize with Pygments
        # split tokens into lines for per-line rendering
        all_lines = [[]]  # list of list of (token_type, text)
        for token_type, value in lex(code, lexer):
            parts = value.split("\n")
            for i, part in enumerate(parts):
                if part:
                    all_lines[-1].append((token_type, part))
                if i < len(parts) - 1:
                    all_lines.append([])

        y_start = self.get_y()
        # top padding
        self.cell(w, 2, "", fill=True, new_x="LMARGIN", new_y="NEXT")

        for line_tokens in all_lines:
            y = self.get_y()
            # check page break mid-block
            if y + line_h > self.h - 20:
                # close current block rect
                self.rect(self.l_margin, y_start, w, y - y_start, style="D")
                self.add_page()
                y = self.get_y()
                y_start = y
            # fill background for entire line
            self.set_fill_color(245, 245, 245)
            self.cell(w, line_h, "", fill=True)
            # render tokens
            self.set_xy(self.l_margin + 3, y)
            for tt, text in line_tokens:
                r, g, b = _get_token_color(tt)
                self.set_text_color(r, g, b)
                tw = self.get_string_width(text)
                self.cell(tw, line_h, text)
            self.set_xy(self.l_margin, y + line_h)

        # bottom padding
        self.set_fill_color(245, 245, 245)
        self.cell(w, 2, "", fill=True, new_x="LMARGIN", new_y="NEXT")
        # border
        self.rect(self.l_margin, y_start, w, self.get_y() - y_start, style="D")
        self.ln(2)

    def diagram_block(self, text):
        """Render an ASCII diagram: monospace on light blue background, centered."""
        self.ln(2)
        self.set_font(MONO, "", 8)
        self.set_text_color(20, 60, 120)
        self.set_fill_color(230, 242, 255)
        self.set_draw_color(100, 160, 220)
        self.set_line_width(0.4)
        lines = text.split("\n")
        line_h = 4.2
        w = self.w - self.l_margin - self.r_margin
        h = len(lines) * line_h + 8
        # page break check
        if self.get_y() + h > self.h - 20:
            self.add_page()
        y_start = self.get_y()
        # render top padding
        self.cell(w, 4, "", fill=True, new_x="LMARGIN", new_y="NEXT")
        # render each line with fill
        for line in lines:
            self.cell(w, line_h, "  " + line, fill=True, new_x="LMARGIN", new_y="NEXT")
        # render bottom padding
        self.cell(w, 4, "", fill=True, new_x="LMARGIN", new_y="NEXT")
        # draw border around the block
        y_end = self.get_y()
        self.rect(self.l_margin, y_start, w, y_end - y_start, style="D")
        self.ln(3)

    def tool_heading(self, name, description):
        """Tool name (bold) + one-line description."""
        self.set_font(F, "B", 11)
        self.set_text_color(25, 100, 200)
        self.cell(self.get_string_width(name) + 2, 7, name)
        self.set_font(F, "", 10)
        self.set_text_color(80, 80, 80)
        self.cell(0, 7, f" \u2014 {description}", new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def cheat_entry(self, command, description, language=""):
        """Two-column: command (mono, syntax-highlighted) + description (regular)."""
        x = self.get_x()
        cmd_w = 75
        desc_w = self.w - self.l_margin - self.r_margin - cmd_w - 2
        y_before = self.get_y()
        line_h = 4.2
        # measure command column height
        self.set_font(MONO, "", 8)
        cmd_lines = self.multi_cell(cmd_w, line_h, command, dry_run=True, output="LINES")
        cmd_h = len(cmd_lines) * line_h
        # measure description column height
        self.set_font(F, "", 9)
        desc_lines = self.multi_cell(desc_w, 4.5, description, dry_run=True, output="LINES")
        desc_h = len(desc_lines) * 4.5
        row_h = max(cmd_h, desc_h, 5)
        # check page break
        if y_before + row_h > self.h - 20:
            self.add_page()
            y_before = self.get_y()
        # draw command column with syntax highlighting
        lexer = _detect_lexer(language) if language else None
        if lexer is not None:
            # tokenize and render with colors
            all_lines = [[]]
            for token_type, value in lex(command, lexer):
                parts = value.split("\n")
                for i, part in enumerate(parts):
                    if part:
                        all_lines[-1].append((token_type, part))
                    if i < len(parts) - 1:
                        all_lines.append([])
            self.set_font(MONO, "", 8)
            cy = y_before
            for line_tokens in all_lines:
                self.set_xy(x, cy)
                for tt, text in line_tokens:
                    r, g, b = _get_token_color(tt)
                    self.set_text_color(r, g, b)
                    tw = self.get_string_width(text)
                    self.cell(tw, line_h, text)
                cy += line_h
        else:
            # plain mono fallback
            self.set_xy(x, y_before)
            self.set_font(MONO, "", 8)
            self.set_text_color(30, 30, 30)
            self.multi_cell(cmd_w, line_h, command)
        # draw description column
        self.set_xy(x + cmd_w + 2, y_before)
        self.set_font(F, "", 9)
        self.set_text_color(60, 60, 60)
        self.multi_cell(desc_w, 4.5, description)
        self.set_y(max(self.get_y(), y_before + row_h) + 1.5)

    def cheat_separator(self):
        self.set_draw_color(220, 220, 220)
        self.set_line_width(0.2)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2)

    def add_week_page(self, week_num, title, summary, topics, resources, goal, deliverables):
        self.add_page()
        self.set_font(F, "B", 22)
        self.set_text_color(25, 100, 200)
        self.cell(0, 12, f"WEEK {week_num}", new_x="LMARGIN", new_y="NEXT")
        self.set_font(F, "B", 15)
        self.set_text_color(60, 60, 60)
        self.cell(0, 9, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)
        self.set_draw_color(200, 200, 200)
        self.set_line_width(0.3)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)
        self.sub_heading("Summary")
        self.body_text(summary)
        self.sub_heading("Key Topics")
        for topic in topics:
            self.bullet(topic)
        self.ln(2)
        self.sub_heading("Resources")
        for label, url in resources:
            self.link_bullet(label, url)
        self.ln(1)
        self.goal_box(goal)
        self.deliverable_box(deliverables)


# ═══════════════════════════════════════════════════════════════
# DEEP-DIVE CONTENT: code examples + diagrams per week
# ═══════════════════════════════════════════════════════════════

def _week1_deep_dive(pdf):
    """Week 1 deep-dive: Python for Data Engineering."""
    pdf.add_page()
    pdf.section_title("Week 1 Deep-Dive: Python Code Examples")

    pdf.sub_heading("Project Structure")
    pdf.diagram_block(
        '  taxi-ingestion/\n'
        '  +-- pyproject.toml          # project metadata + deps\n'
        '  +-- src/\n'
        '  |   +-- taxi_ingestion/\n'
        '  |       +-- __init__.py\n'
        '  |       +-- app.py          # FastAPI application\n'
        '  |       +-- config.py       # pydantic-settings\n'
        '  |       +-- logging_config.py\n'
        '  |       +-- ingestion.py    # download logic\n'
        '  |       +-- models.py       # data contracts\n'
        '  +-- tests/\n'
        '  |   +-- test_app.py\n'
        '  |   +-- test_ingestion.py\n'
        '  |   +-- conftest.py\n'
        '  +-- data/\n'
        '  |   +-- raw/                # downloaded files land here\n'
        '  +-- .env                    # local overrides\n'
        '  +-- Dockerfile'
    )

    pdf.tool_heading("pyproject.toml", "Modern Python project configuration")
    pdf.code_block(
        '[build-system]\n'
        'requires = ["hatchling"]\n'
        'build-backend = "hatchling.build"\n'
        '\n'
        '[project]\n'
        'name = "taxi-ingestion"\n'
        'version = "0.1.0"\n'
        'requires-python = ">=3.11"\n'
        'dependencies = [\n'
        '    "httpx>=0.27",\n'
        '    "fastapi>=0.110",\n'
        '    "uvicorn[standard]>=0.29",\n'
        '    "pydantic-settings>=2.2",\n'
        ']\n'
        '\n'
        '[project.optional-dependencies]\n'
        'dev = ["pytest>=8.0", "ruff>=0.4", "pytest-cov"]\n'
        '\n'
        '[project.scripts]\n'
        'taxi-ingest = "taxi_ingestion.app:main"\n'
        '\n'
        '[tool.ruff]\n'
        'line-length = 100\n'
        'target-version = "py311"',
        "pyproject.toml",
    )

    pdf.tool_heading("Structured Logging", "Replace print() with proper logging")
    pdf.code_block(
        'import logging\n'
        'import json\n'
        'import sys\n'
        '\n'
        'class JSONFormatter(logging.Formatter):\n'
        '    def format(self, record):\n'
        '        return json.dumps({\n'
        '            "ts": self.formatTime(record),\n'
        '            "level": record.levelname,\n'
        '            "module": record.module,\n'
        '            "msg": record.getMessage(),\n'
        '        })\n'
        '\n'
        'def get_logger(name: str) -> logging.Logger:\n'
        '    logger = logging.getLogger(name)\n'
        '    handler = logging.StreamHandler(sys.stdout)\n'
        '    handler.setFormatter(JSONFormatter())\n'
        '    logger.addHandler(handler)\n'
        '    logger.setLevel(logging.INFO)\n'
        '    return logger\n'
        '\n'
        'log = get_logger("ingestion")\n'
        'log.info("Pipeline started", extra={"run_id": "abc123"})',
        "Python — logging_config.py",
    )

    pdf.add_page()
    pdf.tool_heading("FastAPI Ingestion Endpoint", "HTTP API for triggering data pulls")
    pdf.code_block(
        'from fastapi import FastAPI, BackgroundTasks, HTTPException\n'
        'from pydantic import BaseModel, field_validator\n'
        'import httpx\n'
        'from pathlib import Path\n'
        'from datetime import datetime\n'
        '\n'
        'app = FastAPI(title="Taxi Data Ingestion")\n'
        '\n'
        'class IngestRequest(BaseModel):\n'
        '    year: int\n'
        '    month: int\n'
        '\n'
        '    @field_validator("month")\n'
        '    @classmethod\n'
        '    def valid_month(cls, v):\n'
        '        if not 1 <= v <= 12:\n'
        '            raise ValueError("month must be 1-12")\n'
        '        return v\n'
        '\n'
        'async def download_parquet(year: int, month: int):\n'
        '    url = (\n'
        '        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"\n'
        '        f"yellow_tripdata_{year}-{month:02d}.parquet"\n'
        '    )\n'
        '    out = Path(f"data/raw/{year}_{month:02d}.parquet")\n'
        '    out.parent.mkdir(parents=True, exist_ok=True)\n'
        '    async with httpx.AsyncClient(timeout=120) as c:\n'
        '        resp = await c.get(url)\n'
        '        resp.raise_for_status()\n'
        '        out.write_bytes(resp.content)\n'
        '\n'
        '@app.post("/ingest")\n'
        'async def ingest(req: IngestRequest, bg: BackgroundTasks):\n'
        '    bg.add_task(download_parquet, req.year, req.month)\n'
        '    return {"status": "accepted", "period": f"{req.year}-{req.month:02d}"}\n'
        '\n'
        '@app.get("/health")\n'
        'async def health():\n'
        '    return {"status": "ok"}',
        "Python — app.py",
    )

    pdf.tool_heading("Data Contracts with Dataclasses", "Typed records for pipeline data")
    pdf.code_block(
        'from dataclasses import dataclass, asdict\n'
        'from datetime import datetime\n'
        'from typing import Optional\n'
        '\n'
        '@dataclass(frozen=True)\n'
        'class TaxiTrip:\n'
        '    pickup_datetime: datetime\n'
        '    dropoff_datetime: datetime\n'
        '    passenger_count: int\n'
        '    trip_distance: float\n'
        '    fare_amount: float\n'
        '    tip_amount: float\n'
        '    total_amount: float\n'
        '    payment_type: Optional[str] = None\n'
        '\n'
        '    def to_dict(self) -> dict:\n'
        '        return asdict(self)\n'
        '\n'
        '    @property\n'
        '    def duration_minutes(self) -> float:\n'
        '        delta = self.dropoff_datetime - self.pickup_datetime\n'
        '        return delta.total_seconds() / 60',
        "Python — models.py",
    )

    pdf.add_page()
    pdf.tool_heading("httpx Ingestion with Retry", "Resilient HTTP downloads")
    pdf.code_block(
        'import httpx\n'
        'from tenacity import retry, stop_after_attempt, wait_exponential\n'
        'from pathlib import Path\n'
        'from logging_config import get_logger\n'
        '\n'
        'log = get_logger("ingestion")\n'
        '\n'
        '@retry(\n'
        '    stop=stop_after_attempt(3),\n'
        '    wait=wait_exponential(multiplier=1, min=2, max=30),\n'
        '    reraise=True,\n'
        ')\n'
        'def download_file(url: str, dest: Path) -> Path:\n'
        '    log.info(f"Downloading {url}")\n'
        '    dest.parent.mkdir(parents=True, exist_ok=True)\n'
        '    with httpx.Client(timeout=120, follow_redirects=True) as c:\n'
        '        with c.stream("GET", url) as resp:\n'
        '            resp.raise_for_status()\n'
        '            with open(dest, "wb") as f:\n'
        '                for chunk in resp.iter_bytes(chunk_size=8192):\n'
        '                    f.write(chunk)\n'
        '    log.info(f"Saved {dest} ({dest.stat().st_size:,} bytes)")\n'
        '    return dest',
        "Python — ingestion.py",
    )

    pdf.tool_heading("Pytest Tests", "Testing the ingestion logic")
    pdf.code_block(
        'import pytest\n'
        'from httpx import AsyncClient\n'
        'from unittest.mock import patch, AsyncMock\n'
        'from app import app\n'
        '\n'
        '@pytest.mark.anyio\n'
        'async def test_ingest_returns_accepted():\n'
        '    async with AsyncClient(app=app, base_url="http://test") as c:\n'
        '        resp = await c.post("/ingest",\n'
        '            json={"year": 2024, "month": 1})\n'
        '        assert resp.status_code == 200\n'
        '        data = resp.json()\n'
        '        assert data["status"] == "accepted"\n'
        '        assert data["period"] == "2024-01"\n'
        '\n'
        '@pytest.mark.anyio\n'
        'async def test_ingest_validates_month():\n'
        '    async with AsyncClient(app=app, base_url="http://test") as c:\n'
        '        resp = await c.post("/ingest",\n'
        '            json={"year": 2024, "month": 13})\n'
        '        assert resp.status_code == 422\n'
        '\n'
        '@pytest.mark.anyio\n'
        'async def test_health():\n'
        '    async with AsyncClient(app=app, base_url="http://test") as c:\n'
        '        resp = await c.get("/health")\n'
        '        assert resp.json()["status"] == "ok"',
        "Python — test_app.py",
    )

    pdf.tool_heading("Config with pydantic-settings", "Environment-driven configuration")
    pdf.code_block(
        'from pydantic_settings import BaseSettings\n'
        '\n'
        'class Settings(BaseSettings):\n'
        '    api_base_url: str = "https://d37ci6vzurychx.cloudfront.net"\n'
        '    data_dir: str = "data/raw"\n'
        '    log_level: str = "INFO"\n'
        '    max_retries: int = 3\n'
        '\n'
        '    model_config = {"env_prefix": "TAXI_"}\n'
        '\n'
        'settings = Settings()  # reads TAXI_API_BASE_URL, TAXI_DATA_DIR, etc.',
        "Python — config.py",
    )


def _week2_deep_dive(pdf):
    """Week 2 deep-dive: SQL + Data Modeling."""
    pdf.add_page()
    pdf.section_title("Week 2 Deep-Dive: SQL Code Examples")

    pdf.tool_heading("Window Functions", "Analytics beyond GROUP BY")
    pdf.code_block(
        '-- Rank trips by fare within each day\n'
        'SELECT\n'
        '    pickup_date,\n'
        '    trip_id,\n'
        '    fare_amount,\n'
        '    ROW_NUMBER() OVER (\n'
        '        PARTITION BY pickup_date\n'
        '        ORDER BY fare_amount DESC\n'
        '    ) AS fare_rank,\n'
        '    LAG(fare_amount) OVER (\n'
        '        PARTITION BY pickup_date\n'
        '        ORDER BY pickup_datetime\n'
        '    ) AS prev_fare,\n'
        '    SUM(fare_amount) OVER (\n'
        '        PARTITION BY pickup_date\n'
        '        ORDER BY pickup_datetime\n'
        '        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n'
        '    ) AS running_total\n'
        'FROM trips\n'
        'WHERE pickup_date = \'2024-01-15\';',
        "SQL — Window functions",
    )

    pdf.tool_heading("CTEs for Readable Queries", "Break complex logic into named steps")
    pdf.code_block(
        'WITH daily_stats AS (\n'
        '    SELECT\n'
        '        pickup_date,\n'
        '        COUNT(*) AS trip_count,\n'
        '        AVG(fare_amount) AS avg_fare,\n'
        '        PERCENTILE_CONT(0.5) WITHIN GROUP\n'
        '            (ORDER BY fare_amount) AS median_fare\n'
        '    FROM trips\n'
        '    GROUP BY pickup_date\n'
        '),\n'
        'ranked AS (\n'
        '    SELECT *,\n'
        '        RANK() OVER (ORDER BY trip_count DESC) AS busiest\n'
        '    FROM daily_stats\n'
        ')\n'
        'SELECT * FROM ranked WHERE busiest <= 10;',
        "SQL — CTEs",
    )

    pdf.add_page()
    pdf.tool_heading("Star Schema DDL", "Dimensional modeling in PostgreSQL")
    pdf.code_block(
        '-- Dimension: date\n'
        'CREATE TABLE dim_date (\n'
        '    date_key     INT PRIMARY KEY,\n'
        '    full_date    DATE NOT NULL,\n'
        '    year         SMALLINT,\n'
        '    quarter      SMALLINT,\n'
        '    month        SMALLINT,\n'
        '    day_of_week  VARCHAR(10),\n'
        '    is_weekend   BOOLEAN\n'
        ');\n'
        '\n'
        '-- Dimension: location\n'
        'CREATE TABLE dim_location (\n'
        '    location_key  SERIAL PRIMARY KEY,\n'
        '    zone_name     VARCHAR(100),\n'
        '    borough       VARCHAR(50),\n'
        '    service_zone  VARCHAR(50)\n'
        ');\n'
        '\n'
        '-- Fact: trips (grain = one taxi trip)\n'
        'CREATE TABLE fact_trips (\n'
        '    trip_key         BIGSERIAL PRIMARY KEY,\n'
        '    date_key         INT REFERENCES dim_date,\n'
        '    pickup_loc_key   INT REFERENCES dim_location,\n'
        '    dropoff_loc_key  INT REFERENCES dim_location,\n'
        '    passenger_count  SMALLINT,\n'
        '    trip_distance    NUMERIC(8,2),\n'
        '    fare_amount      NUMERIC(8,2),\n'
        '    tip_amount       NUMERIC(8,2),\n'
        '    total_amount     NUMERIC(8,2)\n'
        ');\n'
        '\n'
        'CREATE INDEX idx_fact_date ON fact_trips(date_key);\n'
        'CREATE INDEX idx_fact_pickup ON fact_trips(pickup_loc_key);',
        "SQL — Star schema DDL",
    )

    pdf.tool_heading("EXPLAIN ANALYZE", "Query performance analysis")
    pdf.code_block(
        'EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)\n'
        'SELECT\n'
        '    d.full_date,\n'
        '    l.borough,\n'
        '    COUNT(*) AS trips,\n'
        '    AVG(f.fare_amount) AS avg_fare\n'
        'FROM fact_trips f\n'
        'JOIN dim_date d ON f.date_key = d.date_key\n'
        'JOIN dim_location l ON f.pickup_loc_key = l.location_key\n'
        'WHERE d.year = 2024 AND d.quarter = 1\n'
        'GROUP BY d.full_date, l.borough\n'
        'ORDER BY trips DESC;',
        "SQL — EXPLAIN ANALYZE",
    )

    pdf.add_page()
    pdf.tool_heading("SCD Type 2", "Track dimension history with effective dates")
    pdf.code_block(
        '-- SCD Type 2: keep full history of dimension changes\n'
        'CREATE TABLE dim_location_scd2 (\n'
        '    location_sk    SERIAL PRIMARY KEY,   -- surrogate key\n'
        '    location_id    INT NOT NULL,          -- natural key\n'
        '    zone_name      VARCHAR(100),\n'
        '    borough        VARCHAR(50),\n'
        '    service_zone   VARCHAR(50),\n'
        '    effective_from DATE NOT NULL,\n'
        '    effective_to   DATE DEFAULT \'9999-12-31\',\n'
        '    is_current     BOOLEAN DEFAULT TRUE\n'
        ');\n'
        '\n'
        '-- Close old record and insert new one on change\n'
        'UPDATE dim_location_scd2\n'
        'SET effective_to = CURRENT_DATE - 1,\n'
        '    is_current = FALSE\n'
        'WHERE location_id = 42 AND is_current = TRUE;\n'
        '\n'
        'INSERT INTO dim_location_scd2\n'
        '    (location_id, zone_name, borough, service_zone, effective_from)\n'
        'VALUES (42, \'New Name\', \'Manhattan\', \'Yellow Zone\', CURRENT_DATE);',
        "SQL — SCD Type 2",
    )

    pdf.tool_heading("Materialized Views", "Pre-computed query results in PostgreSQL")
    pdf.code_block(
        'CREATE MATERIALIZED VIEW mv_daily_borough_stats AS\n'
        'SELECT\n'
        '    d.full_date,\n'
        '    l.borough,\n'
        '    COUNT(*)             AS trip_count,\n'
        '    ROUND(AVG(f.fare_amount), 2) AS avg_fare,\n'
        '    ROUND(AVG(f.trip_distance), 2) AS avg_distance\n'
        'FROM fact_trips f\n'
        'JOIN dim_date d ON f.date_key = d.date_key\n'
        'JOIN dim_location l ON f.pickup_loc_key = l.location_key\n'
        'GROUP BY d.full_date, l.borough;\n'
        '\n'
        'CREATE UNIQUE INDEX idx_mv_daily_borough\n'
        '    ON mv_daily_borough_stats (full_date, borough);\n'
        '\n'
        '-- Refresh without locking reads\n'
        'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_borough_stats;',
        "SQL — Materialized views",
    )

    pdf.tool_heading("JSONB Queries", "Semi-structured data in PostgreSQL")
    pdf.code_block(
        '-- Store flexible metadata as JSONB\n'
        'CREATE TABLE events (\n'
        '    id BIGSERIAL PRIMARY KEY,\n'
        '    event_type VARCHAR(50),\n'
        '    payload JSONB NOT NULL,\n'
        '    created_at TIMESTAMPTZ DEFAULT NOW()\n'
        ');\n'
        '\n'
        '-- Query nested fields\n'
        'SELECT\n'
        '    payload->>\'user_id\' AS user_id,\n'
        '    payload->\'location\'->>\'city\' AS city,\n'
        '    (payload->>\'amount\')::numeric AS amount\n'
        'FROM events\n'
        'WHERE payload @> \'{"status": "completed"}\'\n'
        '  AND created_at > NOW() - INTERVAL \'1 day\';\n'
        '\n'
        '-- GIN index for fast JSONB lookups\n'
        'CREATE INDEX idx_events_payload ON events USING GIN (payload);',
        "SQL — JSONB",
    )


def _week3_deep_dive(pdf):
    """Week 3 deep-dive: Docker."""
    pdf.add_page()
    pdf.section_title("Week 3 Deep-Dive: Docker Code Examples")

    pdf.tool_heading("Multi-Stage Dockerfile", "Slim production images")
    pdf.code_block(
        '# Stage 1: build dependencies\n'
        'FROM python:3.11-slim AS builder\n'
        'WORKDIR /app\n'
        'COPY pyproject.toml .\n'
        'RUN pip install --no-cache-dir --prefix=/install .\n'
        '\n'
        '# Stage 2: runtime\n'
        'FROM python:3.11-slim\n'
        'RUN useradd -m appuser\n'
        'COPY --from=builder /install /usr/local\n'
        'COPY src/ /app/src/\n'
        'WORKDIR /app\n'
        'USER appuser\n'
        'EXPOSE 8000\n'
        'CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000"]',
        "Dockerfile",
    )

    pdf.tool_heading("Docker Compose Stack", "Multi-container data platform")
    pdf.code_block(
        'services:\n'
        '  postgres:\n'
        '    image: postgres:16-alpine\n'
        '    environment:\n'
        '      POSTGRES_DB: warehouse\n'
        '      POSTGRES_USER: de_user\n'
        '      POSTGRES_PASSWORD: ${PG_PASSWORD}\n'
        '    volumes:\n'
        '      - pg_data:/var/lib/postgresql/data\n'
        '    ports: ["5432:5432"]\n'
        '    healthcheck:\n'
        '      test: ["CMD-SHELL", "pg_isready -U de_user"]\n'
        '      interval: 5s\n'
        '      retries: 5\n'
        '\n'
        '  pgadmin:\n'
        '    image: dpage/pgadmin4:latest\n'
        '    environment:\n'
        '      PGADMIN_DEFAULT_EMAIL: admin@local.dev\n'
        '      PGADMIN_DEFAULT_PASSWORD: admin\n'
        '    ports: ["8080:80"]\n'
        '    depends_on:\n'
        '      postgres: { condition: service_healthy }\n'
        '\n'
        '  ingestion:\n'
        '    build: .\n'
        '    environment:\n'
        '      DATABASE_URL: postgresql://de_user:${PG_PASSWORD}@postgres/warehouse\n'
        '    depends_on:\n'
        '      postgres: { condition: service_healthy }\n'
        '\n'
        'volumes:\n'
        '  pg_data:',
        "docker-compose.yml",
    )

    pdf.add_page()
    pdf.tool_heading(".dockerignore", "Keep images lean")
    pdf.code_block(
        '__pycache__/\n'
        '*.pyc\n'
        '.venv/\n'
        '.git/\n'
        '.env\n'
        'data/\n'
        '*.md\n'
        'tests/',
        ".dockerignore",
    )

    pdf.sub_heading("Architecture Diagram: Container Stack")
    pdf.diagram_block(
        '  +--------------------------------------------------+\n'
        '  |              Docker Compose Network               |\n'
        '  |                                                    |\n'
        '  |  +-----------+  +-----------+  +---------------+  |\n'
        '  |  | postgres  |  | pgadmin   |  |  ingestion    |  |\n'
        '  |  | :5432     |  | :80       |  |  :8000        |  |\n'
        '  |  |           |  |           |  |               |  |\n'
        '  |  | pg_data   |  | (web UI)  |  | FastAPI app   |  |\n'
        '  |  | volume    |  |           |  | downloads +   |  |\n'
        '  |  |           |  |           |  | loads data    |  |\n'
        '  |  +-----------+  +-----------+  +---------------+  |\n'
        '  |       ^               |               |            |\n'
        '  |       |     connects  |   writes to   |            |\n'
        '  |       +---------------+---------------+            |\n'
        '  +--------------------------------------------------+\n'
        '         :5432           :8080           :8000\n'
        '     (host ports exposed to localhost)'
    )

    pdf.add_page()
    pdf.tool_heading("Health Checks", "Ensure services are ready before dependents start")
    pdf.code_block(
        '# In docker-compose.yml\n'
        'services:\n'
        '  postgres:\n'
        '    healthcheck:\n'
        '      test: ["CMD-SHELL", "pg_isready -U de_user -d warehouse"]\n'
        '      interval: 5s\n'
        '      timeout: 3s\n'
        '      retries: 5\n'
        '      start_period: 10s\n'
        '\n'
        '  app:\n'
        '    depends_on:\n'
        '      postgres:\n'
        '        condition: service_healthy\n'
        '\n'
        '# In Dockerfile (HTTP health check)\n'
        'HEALTHCHECK --interval=30s --timeout=3s --retries=3 \\\n'
        '    CMD curl -f http://localhost:8000/health || exit 1',
        "YAML / Dockerfile — health checks",
    )

    pdf.tool_heading("Docker Networking", "How containers find each other")
    pdf.code_block(
        '# Custom network with explicit subnet\n'
        'networks:\n'
        '  data-net:\n'
        '    driver: bridge\n'
        '    ipam:\n'
        '      config:\n'
        '        - subnet: 172.28.0.0/16\n'
        '\n'
        'services:\n'
        '  postgres:\n'
        '    networks: [data-net]\n'
        '  app:\n'
        '    networks: [data-net]\n'
        '    # connect using service name as hostname:\n'
        '    # postgres://de_user:pass@postgres:5432/warehouse\n'
        '    # NOT localhost! Services use container names.',
        "YAML — docker-compose networking",
    )

    pdf.tool_heading("Volume Management", "Persist data across container restarts")
    pdf.code_block(
        '# Named volume (Docker manages location)\n'
        'volumes:\n'
        '  pg_data:\n'
        '\n'
        'services:\n'
        '  postgres:\n'
        '    volumes:\n'
        '      - pg_data:/var/lib/postgresql/data    # named volume\n'
        '      - ./init.sql:/docker-entrypoint-initdb.d/init.sql  # bind mount\n'
        '\n'
        '# Inspect volume\n'
        '# docker volume inspect pg_data\n'
        '\n'
        '# Backup a volume\n'
        '# docker run --rm -v pg_data:/data -v $(pwd):/backup \\\n'
        '#     alpine tar czf /backup/pg_backup.tar.gz /data',
        "YAML / Bash — volumes",
    )


def _week4_deep_dive(pdf):
    """Week 4 deep-dive: Airflow."""
    pdf.add_page()
    pdf.section_title("Week 4 Deep-Dive: Airflow Code Examples")

    pdf.tool_heading("Complete ETL DAG", "Download -> Transform -> Load pipeline")
    pdf.code_block(
        'from airflow import DAG\n'
        'from airflow.operators.python import PythonOperator\n'
        'from airflow.providers.postgres.operators.postgres import PostgresOperator\n'
        'from datetime import datetime, timedelta\n'
        'import pandas as pd\n'
        'import httpx\n'
        '\n'
        'default_args = {\n'
        '    "owner": "data-eng",\n'
        '    "retries": 3,\n'
        '    "retry_delay": timedelta(minutes=5),\n'
        '    "retry_exponential_backoff": True,\n'
        '    "email_on_failure": True,\n'
        '}\n'
        '\n'
        'def download_taxi_data(**ctx):\n'
        '    ds = ctx["ds"]  # e.g. "2024-01-01"\n'
        '    year, month = ds[:4], ds[5:7]\n'
        '    url = (f"https://d37ci6vzurychx.cloudfront.net/"\n'
        '           f"trip-data/yellow_tripdata_{year}-{month}.parquet")\n'
        '    path = f"/tmp/taxi_{year}_{month}.parquet"\n'
        '    resp = httpx.get(url, timeout=120)\n'
        '    resp.raise_for_status()\n'
        '    with open(path, "wb") as f:\n'
        '        f.write(resp.content)\n'
        '    return path\n'
        '\n'
        'def transform(**ctx):\n'
        '    path = ctx["ti"].xcom_pull(task_ids="download")\n'
        '    df = pd.read_parquet(path)\n'
        '    df = df.dropna(subset=["fare_amount"])\n'
        '    df = df[df["fare_amount"] > 0]\n'
        '    out = path.replace(".parquet", "_clean.parquet")\n'
        '    df.to_parquet(out, index=False)\n'
        '    return out',
        "Python — dags/taxi_etl.py (part 1)",
    )

    pdf.code_block(
        'with DAG(\n'
        '    dag_id="taxi_etl_monthly",\n'
        '    default_args=default_args,\n'
        '    schedule="@monthly",\n'
        '    start_date=datetime(2024, 1, 1),\n'
        '    catchup=True,\n'
        '    max_active_runs=2,\n'
        '    tags=["taxi", "etl"],\n'
        ') as dag:\n'
        '\n'
        '    download = PythonOperator(\n'
        '        task_id="download",\n'
        '        python_callable=download_taxi_data,\n'
        '    )\n'
        '\n'
        '    clean = PythonOperator(\n'
        '        task_id="transform",\n'
        '        python_callable=transform,\n'
        '    )\n'
        '\n'
        '    create_table = PostgresOperator(\n'
        '        task_id="create_table",\n'
        '        postgres_conn_id="warehouse",\n'
        '        sql="""\n'
        '            CREATE TABLE IF NOT EXISTS taxi_trips (\n'
        '                pickup_datetime TIMESTAMP,\n'
        '                dropoff_datetime TIMESTAMP,\n'
        '                passenger_count INT,\n'
        '                trip_distance FLOAT,\n'
        '                fare_amount FLOAT\n'
        '            );\n'
        '        """,\n'
        '    )\n'
        '\n'
        '    download >> clean >> create_table',
        "Python — dags/taxi_etl.py (part 2)",
    )

    pdf.add_page()
    pdf.sub_heading("Architecture Diagram: Airflow DAG Flow")
    pdf.diagram_block(
        '    +------------+     +-------------+     +--------------+\n'
        '    |  download  | --> |  transform   | --> | create_table |\n'
        '    |            |     |              |     |              |\n'
        '    | httpx GET  |     | pandas clean |     | PostgresOp   |\n'
        '    | .parquet   |     | drop nulls   |     | CREATE TABLE |\n'
        '    +------------+     +-------------+     +--------------+\n'
        '          |                   |                    |\n'
        '          v                   v                    v\n'
        '    XCom: file path     XCom: clean path     SQL executed\n'
        '\n'
        '    Schedule: @monthly    Retries: 3    Catchup: enabled\n'
        '    Max active runs: 2   Backfill: airflow dags backfill'
    )

    pdf.tool_heading("Airflow Connections", "Managing external credentials")
    pdf.code_block(
        '# Set via CLI (preferred for automation)\n'
        'airflow connections add warehouse \\\n'
        '    --conn-type postgres \\\n'
        '    --conn-host postgres \\\n'
        '    --conn-port 5432 \\\n'
        '    --conn-login de_user \\\n'
        '    --conn-password "${PG_PASSWORD}" \\\n'
        '    --conn-schema warehouse\n'
        '\n'
        '# Or via environment variable\n'
        'export AIRFLOW_CONN_WAREHOUSE=\\\n'
        '    "postgresql://de_user:pass@postgres:5432/warehouse"',
        "Bash — connection setup",
    )

    pdf.tool_heading("Backfill", "Re-run historical DAG runs")
    pdf.code_block(
        '# Backfill 6 months of taxi data\n'
        'airflow dags backfill taxi_etl_monthly \\\n'
        '    --start-date 2024-01-01 \\\n'
        '    --end-date 2024-06-30 \\\n'
        '    --reset-dagruns',
        "Bash",
    )

    pdf.add_page()
    pdf.tool_heading("XCom: Inter-Task Communication", "Pass data between tasks")
    pdf.code_block(
        '# Push a value (implicit via return)\n'
        'def extract(**ctx):\n'
        '    path = "/tmp/data_2024_01.parquet"\n'
        '    download(path)\n'
        '    return path  # auto-pushed to XCom\n'
        '\n'
        '# Pull a value in another task\n'
        'def transform(**ctx):\n'
        '    path = ctx["ti"].xcom_pull(task_ids="extract")\n'
        '    df = pd.read_parquet(path)\n'
        '    # ... transform ...\n'
        '\n'
        '# Push multiple values with explicit keys\n'
        'def extract_multi(**ctx):\n'
        '    ctx["ti"].xcom_push(key="row_count", value=10_000)\n'
        '    ctx["ti"].xcom_push(key="file_path", value="/tmp/data.parquet")\n'
        '\n'
        '# Pull specific key\n'
        'def load(**ctx):\n'
        '    count = ctx["ti"].xcom_pull(\n'
        '        task_ids="extract_multi", key="row_count")\n'
        '\n'
        '# WARNING: XCom is stored in metadata DB\n'
        '# Never pass large data (DataFrames, files)\n'
        '# Pass file paths or S3 URIs instead',
        "Python — XCom patterns",
    )

    pdf.tool_heading("Sensor: Wait for File", "Event-driven pipeline triggering")
    pdf.code_block(
        'from airflow.sensors.filesystem import FileSensor\n'
        'from airflow.operators.python import PythonOperator\n'
        '\n'
        'wait_for_file = FileSensor(\n'
        '    task_id="wait_for_data",\n'
        '    filepath="/data/incoming/trades_{{ ds }}.csv",\n'
        '    poke_interval=60,     # check every 60 seconds\n'
        '    timeout=3600,         # give up after 1 hour\n'
        '    mode="reschedule",    # free worker slot between checks\n'
        ')\n'
        '\n'
        'process = PythonOperator(\n'
        '    task_id="process_data",\n'
        '    python_callable=process_trades,\n'
        ')\n'
        '\n'
        'wait_for_file >> process',
        "Python — sensor DAG",
    )

    pdf.tool_heading("Custom Operator", "Encapsulate reusable pipeline logic")
    pdf.code_block(
        'from airflow.models import BaseOperator\n'
        'from airflow.utils.context import Context\n'
        'import httpx\n'
        'from pathlib import Path\n'
        '\n'
        'class DownloadOperator(BaseOperator):\n'
        '    """Download a file from a URL and save locally."""\n'
        '\n'
        '    template_fields = ("url", "dest_path")\n'
        '\n'
        '    def __init__(self, url: str, dest_path: str, **kwargs):\n'
        '        super().__init__(**kwargs)\n'
        '        self.url = url\n'
        '        self.dest_path = dest_path\n'
        '\n'
        '    def execute(self, context: Context):\n'
        '        self.log.info(f"Downloading {self.url}")\n'
        '        resp = httpx.get(self.url, timeout=120)\n'
        '        resp.raise_for_status()\n'
        '        Path(self.dest_path).write_bytes(resp.content)\n'
        '        self.log.info(f"Saved to {self.dest_path}")\n'
        '        return self.dest_path',
        "Python — custom operator",
    )


def _week5_deep_dive(pdf):
    """Week 5 deep-dive: dbt."""
    pdf.add_page()
    pdf.section_title("Week 5 Deep-Dive: dbt Code Examples")

    pdf.tool_heading("dbt_project.yml", "Project-level configuration")
    pdf.code_block(
        'name: taxi_analytics\n'
        'version: "1.0.0"\n'
        'profile: taxi\n'
        '\n'
        'model-paths: ["models"]\n'
        'test-paths: ["tests"]\n'
        'macro-paths: ["macros"]\n'
        'seed-paths: ["seeds"]\n'
        '\n'
        'models:\n'
        '  taxi_analytics:\n'
        '    staging:\n'
        '      +materialized: view\n'
        '      +schema: staging\n'
        '    intermediate:\n'
        '      +materialized: ephemeral\n'
        '    marts:\n'
        '      +materialized: table\n'
        '      +schema: analytics',
        "YAML — dbt_project.yml",
    )

    pdf.tool_heading("Staging Model", "Clean raw data with consistent naming")
    pdf.code_block(
        '-- models/staging/stg_taxi_trips.sql\n'
        'WITH source AS (\n'
        '    SELECT * FROM {{ source("raw", "taxi_trips") }}\n'
        '),\n'
        'renamed AS (\n'
        '    SELECT\n'
        '        tpep_pickup_datetime   AS pickup_at,\n'
        '        tpep_dropoff_datetime  AS dropoff_at,\n'
        '        passenger_count,\n'
        '        trip_distance,\n'
        '        fare_amount,\n'
        '        tip_amount,\n'
        '        total_amount,\n'
        '        payment_type,\n'
        '        pulocationid           AS pickup_zone_id,\n'
        '        dolocationid           AS dropoff_zone_id\n'
        '    FROM source\n'
        '    WHERE fare_amount > 0\n'
        '      AND trip_distance > 0\n'
        ')\n'
        'SELECT * FROM renamed',
        "SQL — stg_taxi_trips.sql",
    )

    pdf.tool_heading("Mart Model", "Business-ready aggregation")
    pdf.code_block(
        '-- models/marts/fct_daily_trips.sql\n'
        'WITH trips AS (\n'
        '    SELECT * FROM {{ ref("stg_taxi_trips") }}\n'
        ')\n'
        'SELECT\n'
        '    DATE_TRUNC(\'day\', pickup_at) AS trip_date,\n'
        '    pickup_zone_id,\n'
        '    COUNT(*)               AS total_trips,\n'
        '    AVG(fare_amount)       AS avg_fare,\n'
        '    AVG(trip_distance)     AS avg_distance,\n'
        '    SUM(tip_amount)        AS total_tips,\n'
        '    AVG(tip_amount / NULLIF(fare_amount, 0))\n'
        '                           AS avg_tip_pct\n'
        'FROM trips\n'
        'GROUP BY 1, 2',
        "SQL — fct_daily_trips.sql",
    )

    pdf.add_page()
    pdf.tool_heading("Schema Tests", "Automated data quality in dbt")
    pdf.code_block(
        '# models/staging/schema.yml\n'
        'version: 2\n'
        '\n'
        'sources:\n'
        '  - name: raw\n'
        '    schema: public\n'
        '    tables:\n'
        '      - name: taxi_trips\n'
        '        loaded_at_field: tpep_pickup_datetime\n'
        '        freshness:\n'
        '          warn_after: {count: 24, period: hour}\n'
        '          error_after: {count: 48, period: hour}\n'
        '\n'
        'models:\n'
        '  - name: stg_taxi_trips\n'
        '    columns:\n'
        '      - name: pickup_at\n'
        '        tests: [not_null]\n'
        '      - name: fare_amount\n'
        '        tests:\n'
        '          - not_null\n'
        '          - dbt_utils.accepted_range:\n'
        '              min_value: 0\n'
        '              max_value: 1000',
        "YAML — schema.yml",
    )

    pdf.tool_heading("Jinja Macro", "Reusable transformation patterns")
    pdf.code_block(
        '-- macros/cents_to_dollars.sql\n'
        '{% macro cents_to_dollars(column_name) %}\n'
        '    ROUND({{ column_name }} / 100.0, 2)\n'
        '{% endmacro %}\n'
        '\n'
        '-- Usage in a model:\n'
        '-- SELECT {{ cents_to_dollars("fare_cents") }} AS fare_dollars',
        "SQL + Jinja",
    )

    pdf.add_page()
    pdf.tool_heading("Incremental Model", "Process only new data (not full table)")
    pdf.code_block(
        '-- models/marts/fct_trips_incremental.sql\n'
        '{{ config(\n'
        '    materialized="incremental",\n'
        '    unique_key="trip_id",\n'
        '    incremental_strategy="merge"\n'
        ') }}\n'
        '\n'
        'WITH new_trips AS (\n'
        '    SELECT * FROM {{ ref("stg_taxi_trips") }}\n'
        '    {% if is_incremental() %}\n'
        '    WHERE pickup_at > (\n'
        '        SELECT MAX(pickup_at) FROM {{ this }}\n'
        '    )\n'
        '    {% endif %}\n'
        ')\n'
        'SELECT\n'
        '    {{ dbt_utils.generate_surrogate_key(\n'
        '        ["pickup_at", "pickup_zone_id", "fare_amount"]\n'
        '    ) }} AS trip_id,\n'
        '    *\n'
        'FROM new_trips',
        "SQL — incremental model",
    )

    pdf.tool_heading("Sources and Freshness", "Track raw data arrival")
    pdf.code_block(
        '# models/staging/sources.yml\n'
        'version: 2\n'
        '\n'
        'sources:\n'
        '  - name: raw\n'
        '    schema: public\n'
        '    tables:\n'
        '      - name: taxi_trips\n'
        '        loaded_at_field: tpep_pickup_datetime\n'
        '        freshness:\n'
        '          warn_after: {count: 24, period: hour}\n'
        '          error_after: {count: 48, period: hour}\n'
        '      - name: taxi_zones\n'
        '        freshness: null  # static reference data\n'
        '\n'
        '# Check freshness: dbt source freshness',
        "YAML — sources.yml",
    )

    pdf.tool_heading("dbt Snapshot", "SCD Type 2 built into dbt")
    pdf.code_block(
        '-- snapshots/taxi_zones_snapshot.sql\n'
        '{% snapshot taxi_zones_snapshot %}\n'
        '{{ config(\n'
        '    target_schema="snapshots",\n'
        '    unique_key="zone_id",\n'
        '    strategy="check",\n'
        '    check_cols=["zone_name", "borough"],\n'
        ') }}\n'
        '\n'
        'SELECT * FROM {{ source("raw", "taxi_zones") }}\n'
        '\n'
        '{% endsnapshot %}',
        "SQL — dbt snapshot",
    )

    pdf.sub_heading("Architecture Diagram: dbt Model Layers")
    pdf.diagram_block(
        '  RAW (PostgreSQL)        STAGING (views)         MARTS (tables)\n'
        '\n'
        '  +---------------+     +------------------+     +------------------+\n'
        '  | raw.taxi_trips| --> | stg_taxi_trips   | --> | fct_daily_trips  |\n'
        '  | (as loaded by |     | (renamed cols,   |     | (aggregated by   |\n'
        '  |  Airflow)     |     |  filtered nulls, |     |  date + zone,    |\n'
        '  +---------------+     |  typed)          |     |  avg fare, etc.) |\n'
        '                        +------------------+     +------------------+\n'
        '  +---------------+     +------------------+     +------------------+\n'
        '  | raw.taxi_zones| --> | stg_taxi_zones   | --> | dim_zone         |\n'
        '  | (lookup table)|     | (renamed, clean) |     | (enriched)       |\n'
        '  +---------------+     +------------------+     +------------------+\n'
        '\n'
        '  Materialization:       view (cheap, always    table (fast queries,\n'
        '                          fresh)                 refreshed on dbt run)\n'
        '\n'
        '  Tests at every layer: not_null, unique, accepted_values, freshness'
    )


def _week6_deep_dive(pdf):
    """Week 6 deep-dive: Spark + Lakehouse."""
    pdf.add_page()
    pdf.section_title("Week 6 Deep-Dive: Spark + Delta Lake")

    pdf.tool_heading("PySpark: Read + Transform", "DataFrame API basics")
    pdf.code_block(
        'from pyspark.sql import SparkSession\n'
        'from pyspark.sql import functions as F\n'
        '\n'
        'spark = SparkSession.builder \\\n'
        '    .appName("taxi-lakehouse") \\\n'
        '    .config("spark.jars.packages",\n'
        '            "io.delta:delta-spark_2.12:3.1.0") \\\n'
        '    .config("spark.sql.extensions",\n'
        '            "io.delta.sql.DeltaSparkSessionExtension") \\\n'
        '    .getOrCreate()\n'
        '\n'
        '# Read raw CSV\n'
        'raw = spark.read.csv("data/raw/flights.csv",\n'
        '                     header=True, inferSchema=True)\n'
        '\n'
        '# Transform\n'
        'cleaned = (\n'
        '    raw\n'
        '    .filter(F.col("DEP_DELAY").isNotNull())\n'
        '    .withColumn("flight_date",\n'
        '        F.to_date(F.col("FL_DATE")))\n'
        '    .withColumn("is_delayed",\n'
        '        F.when(F.col("DEP_DELAY") > 15, True)\n'
        '         .otherwise(False))\n'
        '    .select("flight_date", "CARRIER", "ORIGIN",\n'
        '            "DEST", "DEP_DELAY", "ARR_DELAY",\n'
        '            "is_delayed")\n'
        ')\n'
        '\n'
        '# Write as Delta Lake table (partitioned)\n'
        'cleaned.write \\\n'
        '    .format("delta") \\\n'
        '    .partitionBy("flight_date") \\\n'
        '    .mode("overwrite") \\\n'
        '    .save("data/lakehouse/silver/flights")',
        "Python — spark_etl.py",
    )

    pdf.tool_heading("Delta Lake Time Travel", "Query historical versions")
    pdf.code_block(
        '# Read the latest version\n'
        'current = spark.read.format("delta") \\\n'
        '    .load("data/lakehouse/silver/flights")\n'
        '\n'
        '# Read version 0 (initial load)\n'
        'v0 = spark.read.format("delta") \\\n'
        '    .option("versionAsOf", 0) \\\n'
        '    .load("data/lakehouse/silver/flights")\n'
        '\n'
        '# Read as of a timestamp\n'
        'yesterday = spark.read.format("delta") \\\n'
        '    .option("timestampAsOf", "2024-06-14") \\\n'
        '    .load("data/lakehouse/silver/flights")\n'
        '\n'
        '# Show version history\n'
        'from delta.tables import DeltaTable\n'
        'dt = DeltaTable.forPath(spark, "data/lakehouse/silver/flights")\n'
        'dt.history().show(truncate=False)',
        "Python — time_travel.py",
    )

    pdf.add_page()
    pdf.tool_heading("Spark SQL Analytics", "SQL interface on Delta tables")
    pdf.code_block(
        'spark.sql("""\n'
        '    SELECT\n'
        '        CARRIER,\n'
        '        COUNT(*) AS flights,\n'
        '        ROUND(AVG(DEP_DELAY), 1) AS avg_delay,\n'
        '        ROUND(SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)\n'
        '              * 100.0 / COUNT(*), 1) AS delay_pct\n'
        '    FROM delta.`data/lakehouse/silver/flights`\n'
        '    GROUP BY CARRIER\n'
        '    ORDER BY delay_pct DESC\n'
        '    LIMIT 10\n'
        '""").show()',
        "Python — spark_analytics.py",
    )

    pdf.sub_heading("Architecture Diagram: Medallion Lakehouse")
    pdf.diagram_block(
        '  RAW FILES          BRONZE              SILVER              GOLD\n'
        '  (CSV/JSON)       (raw Delta)        (cleaned Delta)    (aggregated)\n'
        '\n'
        '  +----------+     +-----------+     +-------------+    +----------+\n'
        '  | flights  | --> | raw_      | --> | flights     | -> | daily_   |\n'
        '  | .csv     |     | flights   |     | (deduped,   |    | flight_  |\n'
        '  +----------+     | (as-is,   |     |  typed,     |    | stats    |\n'
        '                   |  append)  |     |  filtered)  |    +----------+\n'
        '  +----------+     +-----------+     +-------------+    +----------+\n'
        '  | weather  | --> | raw_      | --> | weather     | -> | carrier_ |\n'
        '  | .json    |     | weather   |     | (joined,    |    | perf     |\n'
        '  +----------+     +-----------+     |  enriched)  |    +----------+\n'
        '                                     +-------------+\n'
        '\n'
        '  Storage: local filesystem or S3\n'
        '  Format:  Delta Lake (Parquet + transaction log)\n'
        '  Engine:  Apache Spark 3.5 / Delta 3.1'
    )

    pdf.add_page()
    pdf.tool_heading("Performance Tuning", "Making Spark jobs faster")
    pdf.code_block(
        '# Tune shuffle partitions (default 200 is too many for small data)\n'
        'spark.conf.set("spark.sql.shuffle.partitions", "50")\n'
        '\n'
        '# Broadcast small dimension tables for efficient joins\n'
        'from pyspark.sql.functions import broadcast\n'
        'joined = flights.join(\n'
        '    broadcast(airports),  # small table < 10MB\n'
        '    flights.ORIGIN == airports.IATA,\n'
        '    "left"\n'
        ')\n'
        '\n'
        '# Cache intermediate results used multiple times\n'
        'silver = spark.read.format("delta") \\\n'
        '    .load("data/lakehouse/silver/flights")\n'
        'silver.cache()  # keep in memory\n'
        'silver.count()  # trigger caching\n'
        '\n'
        '# Partition pruning: filter on partition column\n'
        '# Spark reads only matching partitions from disk\n'
        'recent = silver.filter(\n'
        '    F.col("flight_date") >= "2024-01-01"\n'
        ')\n'
        '\n'
        '# Adaptive Query Execution (Spark 3.x default)\n'
        'spark.conf.set("spark.sql.adaptive.enabled", "true")\n'
        'spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")',
        "Python — Spark tuning",
    )

    pdf.tool_heading("Delta Lake MERGE", "Upsert pattern for lakehouse")
    pdf.code_block(
        'from delta.tables import DeltaTable\n'
        '\n'
        '# Target: existing silver table\n'
        'target = DeltaTable.forPath(spark, "data/lakehouse/silver/flights")\n'
        '\n'
        '# Source: new batch of incoming data\n'
        'new_data = spark.read.parquet("data/raw/flights_latest.parquet")\n'
        '\n'
        '# MERGE: update existing rows, insert new ones\n'
        'target.alias("t").merge(\n'
        '    new_data.alias("s"),\n'
        '    "t.flight_id = s.flight_id"\n'
        ').whenMatchedUpdateAll() \\\n'
        ' .whenNotMatchedInsertAll() \\\n'
        ' .execute()\n'
        '\n'
        '# Compact small files (maintenance)\n'
        'target.optimize().executeCompaction()\n'
        '\n'
        '# Remove old versions (save storage)\n'
        'target.vacuum(168)  # keep 7 days of history',
        "Python — Delta MERGE + maintenance",
    )


def _week7_deep_dive(pdf):
    """Week 7 deep-dive: Kafka + Great Expectations."""
    pdf.add_page()
    pdf.section_title("Week 7 Deep-Dive: Kafka + Data Quality")

    pdf.tool_heading("Kafka Producer", "Publish events to a topic")
    pdf.code_block(
        'from confluent_kafka import Producer\n'
        'import json\n'
        'import time\n'
        'import random\n'
        '\n'
        'producer = Producer({\n'
        '    "bootstrap.servers": "localhost:9092",\n'
        '    "acks": "all",\n'
        '    "linger.ms": 10,\n'
        '    "batch.num.messages": 100,\n'
        '})\n'
        '\n'
        'SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]\n'
        '\n'
        'def generate_trade():\n'
        '    return {\n'
        '        "symbol": random.choice(SYMBOLS),\n'
        '        "price": round(random.uniform(100, 500), 2),\n'
        '        "quantity": random.randint(1, 1000),\n'
        '        "timestamp": int(time.time() * 1000),\n'
        '    }\n'
        '\n'
        'for _ in range(10_000):\n'
        '    trade = generate_trade()\n'
        '    producer.produce(\n'
        '        topic="trades",\n'
        '        key=trade["symbol"],\n'
        '        value=json.dumps(trade),\n'
        '    )\n'
        'producer.flush()',
        "Python — producer.py",
    )

    pdf.tool_heading("Kafka Consumer", "Read events and write to database")
    pdf.code_block(
        'from confluent_kafka import Consumer\n'
        'import json\n'
        '\n'
        'consumer = Consumer({\n'
        '    "bootstrap.servers": "localhost:9092",\n'
        '    "group.id": "trade-writer",\n'
        '    "auto.offset.reset": "earliest",\n'
        '    "enable.auto.commit": False,\n'
        '})\n'
        'consumer.subscribe(["trades"])\n'
        '\n'
        'batch = []\n'
        'try:\n'
        '    while True:\n'
        '        msg = consumer.poll(timeout=1.0)\n'
        '        if msg is None:\n'
        '            continue\n'
        '        if msg.error():\n'
        '            print(f"Error: {msg.error()}")\n'
        '            continue\n'
        '        trade = json.loads(msg.value())\n'
        '        batch.append(trade)\n'
        '        if len(batch) >= 500:\n'
        '            # insert_batch(batch)  # your DB insert\n'
        '            consumer.commit()\n'
        '            batch.clear()\n'
        'finally:\n'
        '    consumer.close()',
        "Python — consumer.py",
    )

    pdf.add_page()
    pdf.sub_heading("Architecture Diagram: Kafka Streaming Pipeline")
    pdf.diagram_block(
        '  +------------+     +-------------------+     +-------------+\n'
        '  |  Producer  | --> |   Kafka Broker    | --> |  Consumer   |\n'
        '  | (Python)   |     |                   |     |  Group A    |\n'
        '  | generate   |     |  topic: trades    |     |  (write to  |\n'
        '  | trade      |     |  partitions: 6    |     |   Postgres) |\n'
        '  | events     |     |  replication: 3   |     +-------------+\n'
        '  +------------+     |                   |     +-------------+\n'
        '                     |  topic: orders    | --> |  Consumer   |\n'
        '                     |  partitions: 3    |     |  Group B    |\n'
        '                     +-------------------+     |  (analytics)|\n'
        '                            |                  +-------------+\n'
        '                     +------v---------+\n'
        '                     | Schema Registry|\n'
        '                     | (Avro/JSON)    |\n'
        '                     +----------------+'
    )

    pdf.tool_heading("Kafka with Avro Schema", "Type-safe serialization")
    pdf.code_block(
        'from confluent_kafka.schema_registry import SchemaRegistryClient\n'
        'from confluent_kafka.schema_registry.avro import AvroSerializer\n'
        'from confluent_kafka import SerializingProducer\n'
        '\n'
        'schema_str = """\n'
        '{\n'
        '  "type": "record",\n'
        '  "name": "Trade",\n'
        '  "fields": [\n'
        '    {"name": "symbol",    "type": "string"},\n'
        '    {"name": "price",     "type": "double"},\n'
        '    {"name": "quantity",  "type": "int"},\n'
        '    {"name": "timestamp", "type": "long"}\n'
        '  ]\n'
        '}\n'
        '"""\n'
        '\n'
        'sr = SchemaRegistryClient({"url": "http://localhost:8081"})\n'
        'avro_ser = AvroSerializer(sr, schema_str)\n'
        '\n'
        'producer = SerializingProducer({\n'
        '    "bootstrap.servers": "localhost:9092",\n'
        '    "value.serializer": avro_ser,\n'
        '})\n'
        '\n'
        'producer.produce("trades",\n'
        '    value={"symbol": "AAPL", "price": 175.50,\n'
        '           "quantity": 100, "timestamp": 1718000000})',
        "Python — Avro producer",
    )

    pdf.add_page()
    pdf.tool_heading("Great Expectations Suite", "Automated data validation")
    pdf.code_block(
        'import great_expectations as gx\n'
        '\n'
        'context = gx.get_context()\n'
        '\n'
        '# Add a Pandas datasource\n'
        'ds = context.sources.add_pandas("trades_ds")\n'
        'asset = ds.add_dataframe_asset("trades_df")\n'
        '\n'
        '# Build expectation suite\n'
        'suite = context.add_expectation_suite("trades_quality")\n'
        '\n'
        'suite.add_expectation(\n'
        '    gx.expectations.ExpectColumnValuesToNotBeNull(\n'
        '        column="symbol"))\n'
        'suite.add_expectation(\n'
        '    gx.expectations.ExpectColumnValuesToBeBetween(\n'
        '        column="price", min_value=0.01, max_value=10000))\n'
        'suite.add_expectation(\n'
        '    gx.expectations.ExpectColumnValuesToBeBetween(\n'
        '        column="quantity", min_value=1, max_value=100000))\n'
        'suite.add_expectation(\n'
        '    gx.expectations.ExpectColumnValuesToBeInSet(\n'
        '        column="symbol",\n'
        '        value_set=["AAPL","GOOG","MSFT","AMZN","TSLA"]))\n'
        '\n'
        '# Run checkpoint\n'
        'checkpoint = context.add_or_update_checkpoint(\n'
        '    name="trades_checkpoint",\n'
        '    validations=[{\n'
        '        "batch_request": asset.build_batch_request(),\n'
        '        "expectation_suite_name": "trades_quality",\n'
        '    }],\n'
        ')\n'
        'result = checkpoint.run()\n'
        'assert result.success, "Data quality check failed!"',
        "Python — validate_trades.py",
    )

    pdf.tool_heading("Custom Expectation", "Domain-specific validation rules")
    pdf.code_block(
        'from great_expectations.expectations.expectation import (\n'
        '    ColumnMapExpectation,\n'
        ')\n'
        '\n'
        'class ExpectColumnValuesToBeValidTicker(ColumnMapExpectation):\n'
        '    """Expect stock ticker symbols to be 1-5 uppercase letters."""\n'
        '\n'
        '    map_metric = "column_values.match_regex"\n'
        '    success_keys = ("regex",)\n'
        '\n'
        '    default_kwarg_values = {\n'
        '        "regex": r"^[A-Z]{1,5}$",\n'
        '        "mostly": 1.0,\n'
        '    }\n'
        '\n'
        '# Usage:\n'
        '# suite.add_expectation(\n'
        '#     ExpectColumnValuesToBeValidTicker(column="symbol")\n'
        '# )',
        "Python — custom expectation",
    )

    pdf.tool_heading("GX in Airflow", "Integrate data quality into your DAG")
    pdf.code_block(
        '# Inside an Airflow PythonOperator\n'
        'def validate_loaded_data(**ctx):\n'
        '    import great_expectations as gx\n'
        '    import pandas as pd\n'
        '\n'
        '    df = pd.read_sql("SELECT * FROM trades", engine)\n'
        '    context = gx.get_context()\n'
        '    result = context.run_checkpoint(\n'
        '        checkpoint_name="trades_checkpoint",\n'
        '        batch_request={\n'
        '            "runtime_parameters": {"batch_data": df},\n'
        '            "batch_identifiers": {"run_id": ctx["run_id"]},\n'
        '        },\n'
        '    )\n'
        '    if not result.success:\n'
        '        raise ValueError("Data quality check failed!")\n'
        '\n'
        '# In DAG: download >> load >> validate >> notify\n'
        'validate = PythonOperator(\n'
        '    task_id="validate",\n'
        '    python_callable=validate_loaded_data,\n'
        ')',
        "Python — GX in Airflow DAG",
    )


def _week8_deep_dive(pdf):
    """Week 8 deep-dive: Cloud + CI/CD."""
    pdf.add_page()
    pdf.section_title("Week 8 Deep-Dive: Cloud + CI/CD")

    pdf.tool_heading("Terraform: S3 + Redshift", "Infrastructure as code")
    pdf.code_block(
        'terraform {\n'
        '  required_providers {\n'
        '    aws = { source = "hashicorp/aws", version = "~> 5.0" }\n'
        '  }\n'
        '  backend "s3" {\n'
        '    bucket = "my-tf-state"\n'
        '    key    = "data-platform/terraform.tfstate"\n'
        '    region = "us-east-1"\n'
        '  }\n'
        '}\n'
        '\n'
        'provider "aws" { region = var.region }\n'
        '\n'
        'resource "aws_s3_bucket" "lakehouse" {\n'
        '  bucket = "${var.project}-lakehouse"\n'
        '  tags   = { Environment = var.env }\n'
        '}\n'
        '\n'
        'resource "aws_s3_bucket_versioning" "lakehouse" {\n'
        '  bucket = aws_s3_bucket.lakehouse.id\n'
        '  versioning_configuration { status = "Enabled" }\n'
        '}\n'
        '\n'
        'resource "aws_redshift_cluster" "warehouse" {\n'
        '  cluster_identifier  = "${var.project}-warehouse"\n'
        '  database_name       = "analytics"\n'
        '  master_username     = "admin"\n'
        '  master_password     = var.redshift_password\n'
        '  node_type           = "dc2.large"\n'
        '  number_of_nodes     = 2\n'
        '  skip_final_snapshot = true\n'
        '}',
        "HCL — main.tf",
    )

    pdf.tool_heading("GitHub Actions CI", "Automated testing on every PR")
    pdf.code_block(
        'name: Data Platform CI\n'
        'on:\n'
        '  pull_request:\n'
        '    branches: [main]\n'
        '\n'
        'jobs:\n'
        '  lint-and-test:\n'
        '    runs-on: ubuntu-latest\n'
        '    services:\n'
        '      postgres:\n'
        '        image: postgres:16\n'
        '        env:\n'
        '          POSTGRES_DB: test_warehouse\n'
        '          POSTGRES_PASSWORD: test\n'
        '        ports: ["5432:5432"]\n'
        '        options: >-\n'
        '          --health-cmd pg_isready\n'
        '          --health-interval 10s\n'
        '          --health-retries 5\n'
        '    steps:\n'
        '      - uses: actions/checkout@v4\n'
        '      - uses: actions/setup-python@v5\n'
        '        with: { python-version: "3.11" }\n'
        '      - run: pip install -e ".[dev]"\n'
        '      - run: ruff check .\n'
        '      - run: pytest tests/ -v --tb=short\n'
        '\n'
        '  dbt-test:\n'
        '    runs-on: ubuntu-latest\n'
        '    needs: lint-and-test\n'
        '    steps:\n'
        '      - uses: actions/checkout@v4\n'
        '      - run: pip install dbt-postgres\n'
        '      - run: dbt deps\n'
        '      - run: dbt build --select state:modified+',
        "YAML — .github/workflows/ci.yml",
    )

    pdf.add_page()
    pdf.sub_heading("Architecture Diagram: Cloud Data Platform")
    pdf.diagram_block(
        '  +--------+     +----------+     +----------+     +-----------+\n'
        '  | GitHub | --> | Actions  | --> | Terraform| --> |   AWS     |\n'
        '  | (code) |     | (CI/CD)  |     | (IaC)   |     |           |\n'
        '  +--------+     +----------+     +----------+     |  +-----+ |\n'
        '                                                   |  | S3  | |\n'
        '  +---------+     +---------+     +----------+     |  | raw | |\n'
        '  | Kafka   | --> | Airflow | --> | Spark    | --> |  +-----+ |\n'
        '  | (stream)|     | (orch)  |     | (proc)   |     |    |     |\n'
        '  +---------+     +---------+     +----------+     |    v     |\n'
        '                                                   |  +-----+ |\n'
        '  +---------+     +---------+                      |  |Redsh| |\n'
        '  | GX      | --> | dbt     | -------------------> |  |ift  | |\n'
        '  | (qual)  |     | (ELT)   |                      |  +-----+ |\n'
        '  +---------+     +---------+                      |    |     |\n'
        '                                                   |    v     |\n'
        '                                                   | +------+ |\n'
        '                                                   | |Super-| |\n'
        '                                                   | |set   | |\n'
        '                                                   | +------+ |\n'
        '                                                   +-----------+'
    )

    pdf.tool_heading("AWS CLI: S3 Operations", "Common data engineering S3 commands")
    pdf.code_block(
        '# Sync local data to S3\n'
        'aws s3 sync data/lakehouse/ s3://my-lakehouse/ \\\n'
        '    --exclude "*.tmp"\n'
        '\n'
        '# List objects with size\n'
        'aws s3 ls s3://my-lakehouse/silver/flights/ \\\n'
        '    --recursive --human-readable\n'
        '\n'
        '# Copy from Redshift via UNLOAD\n'
        'UNLOAD (\'SELECT * FROM analytics.fct_daily_trips\')\n'
        'TO \'s3://my-lakehouse/exports/daily_trips_\'\n'
        'IAM_ROLE \'arn:aws:iam::123456:role/RedshiftS3\'\n'
        'FORMAT AS PARQUET;',
        "Bash / SQL",
    )

    pdf.tool_heading("GitHub Actions CD", "Deploy on merge to main")
    pdf.code_block(
        'name: Deploy Data Platform\n'
        'on:\n'
        '  push:\n'
        '    branches: [main]\n'
        '\n'
        'jobs:\n'
        '  deploy-infra:\n'
        '    runs-on: ubuntu-latest\n'
        '    environment: production\n'
        '    steps:\n'
        '      - uses: actions/checkout@v4\n'
        '      - uses: hashicorp/setup-terraform@v3\n'
        '      - run: terraform init\n'
        '        working-directory: terraform/\n'
        '      - run: terraform apply -auto-approve\n'
        '        working-directory: terraform/\n'
        '        env:\n'
        '          TF_VAR_redshift_password: ${{ secrets.REDSHIFT_PW }}\n'
        '\n'
        '  deploy-dbt:\n'
        '    needs: deploy-infra\n'
        '    runs-on: ubuntu-latest\n'
        '    steps:\n'
        '      - uses: actions/checkout@v4\n'
        '      - run: pip install dbt-redshift\n'
        '      - run: dbt build --target prod\n'
        '        env:\n'
        '          DBT_PROFILES_DIR: ./dbt\n'
        '\n'
        '  deploy-dags:\n'
        '    needs: deploy-infra\n'
        '    runs-on: ubuntu-latest\n'
        '    steps:\n'
        '      - uses: actions/checkout@v4\n'
        '      - run: |\n'
        '          aws s3 sync dags/ s3://my-airflow-bucket/dags/ \\\n'
        '              --delete',
        "YAML — .github/workflows/cd.yml",
    )

    pdf.add_page()
    pdf.tool_heading("Terraform Variables + Outputs", "Parameterize your infrastructure")
    pdf.code_block(
        '# variables.tf\n'
        'variable "project" {\n'
        '  type        = string\n'
        '  description = "Project name prefix"\n'
        '  default     = "de-platform"\n'
        '}\n'
        '\n'
        'variable "env" {\n'
        '  type    = string\n'
        '  default = "dev"\n'
        '}\n'
        '\n'
        'variable "region" {\n'
        '  type    = string\n'
        '  default = "us-east-1"\n'
        '}\n'
        '\n'
        'variable "redshift_password" {\n'
        '  type      = string\n'
        '  sensitive = true\n'
        '}\n'
        '\n'
        '# outputs.tf\n'
        'output "s3_bucket_name" {\n'
        '  value = aws_s3_bucket.lakehouse.bucket\n'
        '}\n'
        '\n'
        'output "redshift_endpoint" {\n'
        '  value = aws_redshift_cluster.warehouse.endpoint\n'
        '}\n'
        '\n'
        'output "redshift_iam_role_arn" {\n'
        '  value = aws_iam_role.redshift_s3.arn\n'
        '}',
        "HCL — variables.tf + outputs.tf",
    )

    pdf.tool_heading("Redshift COPY + Spectrum", "Load and query S3 data")
    pdf.code_block(
        '-- COPY: bulk load from S3 into Redshift\n'
        'COPY analytics.fact_trips\n'
        'FROM \'s3://my-lakehouse/gold/daily_trips/\'\n'
        'IAM_ROLE \'arn:aws:iam::123456:role/RedshiftS3\'\n'
        'FORMAT AS PARQUET;\n'
        '\n'
        '-- Redshift Spectrum: query S3 directly (no COPY needed)\n'
        'CREATE EXTERNAL SCHEMA spectrum_lake\n'
        'FROM DATA CATALOG\n'
        'DATABASE \'lakehouse_db\'\n'
        'IAM_ROLE \'arn:aws:iam::123456:role/RedshiftSpectrum\';\n'
        '\n'
        '-- Query S3 data as if it were a local table\n'
        'SELECT\n'
        '    flight_date,\n'
        '    COUNT(*) AS flights,\n'
        '    AVG(dep_delay) AS avg_delay\n'
        'FROM spectrum_lake.silver_flights\n'
        'WHERE flight_date >= \'2024-01-01\'\n'
        'GROUP BY 1;',
        "SQL — Redshift COPY + Spectrum",
    )

    pdf.tool_heading("IAM Least-Privilege Policy", "Secure access for Redshift to S3")
    pdf.code_block(
        'resource "aws_iam_role_policy" "redshift_s3_access" {\n'
        '  name = "${local.prefix}-redshift-s3-policy"\n'
        '  role = aws_iam_role.redshift_s3.id\n'
        '\n'
        '  policy = jsonencode({\n'
        '    Version = "2012-10-17"\n'
        '    Statement = [\n'
        '      {\n'
        '        Effect = "Allow"\n'
        '        Action = [\n'
        '          "s3:GetObject",\n'
        '          "s3:ListBucket",\n'
        '          "s3:GetBucketLocation",\n'
        '        ]\n'
        '        Resource = [\n'
        '          aws_s3_bucket.lakehouse.arn,\n'
        '          "${aws_s3_bucket.lakehouse.arn}/*",\n'
        '        ]\n'
        '      }\n'
        '    ]\n'
        '  })\n'
        '}',
        "HCL — IAM policy",
    )


# ═══════════════════════════════════════════════════════════════
# COOKBOOK APPENDIX: cheat sheets for each tool
# ═══════════════════════════════════════════════════════════════

def _cookbook_python(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Python", 60, 130, 60)

    pdf.cheat_entry("python -m venv .venv", "Create a virtual environment", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("source .venv/bin/activate", "Activate venv (macOS/Linux)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("pip install -e '.[dev]'", "Install package in editable mode with dev extras", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("pip freeze > requirements.txt", "Pin all installed package versions", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("pip install pip-tools\npip-compile pyproject.toml", "Generate locked requirements from pyproject.toml", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("python -m pytest tests/ -v", "Run tests with verbose output", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("python -m pytest --cov=src", "Run tests with coverage report", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("ruff check . --fix", "Lint and auto-fix Python files", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("ruff format .", "Format all Python files", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("python -c 'import sys; print(sys.path)'", "Debug module resolution issues", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("Common Patterns")
    pdf.code_block(
        '# Context manager for file handling\n'
        'from pathlib import Path\n'
        'data = Path("data.json").read_text()\n'
        '\n'
        '# Dataclass for typed records\n'
        'from dataclasses import dataclass\n'
        '@dataclass\n'
        'class Trade:\n'
        '    symbol: str\n'
        '    price: float\n'
        '    quantity: int\n'
        '\n'
        '# Generator for memory-efficient iteration\n'
        'def read_chunks(path, size=10_000):\n'
        '    import pandas as pd\n'
        '    for chunk in pd.read_csv(path, chunksize=size):\n'
        '        yield chunk\n'
        '\n'
        '# Retry decorator\n'
        'from tenacity import retry, stop_after_attempt\n'
        '@retry(stop=stop_after_attempt(3))\n'
        'def fetch_data(url):\n'
        '    return httpx.get(url).raise_for_status()',
        "Python patterns",
    )

    pdf.sub_heading("Data Engineering Patterns")
    pdf.code_block(
        '# Atomic file writes (no partial files on crash)\n'
        'import tempfile, shutil\n'
        'with tempfile.NamedTemporaryFile(\n'
        '    dir="data/", delete=False, suffix=".parquet"\n'
        ') as tmp:\n'
        '    df.to_parquet(tmp.name)\n'
        '    shutil.move(tmp.name, "data/final.parquet")\n'
        '\n'
        '# Connection pooling with context manager\n'
        'from contextlib import contextmanager\n'
        'from sqlalchemy import create_engine\n'
        '\n'
        '@contextmanager\n'
        'def get_engine():\n'
        '    engine = create_engine(DATABASE_URL,\n'
        '        pool_size=5, pool_recycle=3600)\n'
        '    try:\n'
        '        yield engine\n'
        '    finally:\n'
        '        engine.dispose()\n'
        '\n'
        '# Parallel downloads with asyncio\n'
        'import asyncio\n'
        'async def download_all(urls):\n'
        '    async with httpx.AsyncClient() as c:\n'
        '        tasks = [c.get(url) for url in urls]\n'
        '        return await asyncio.gather(*tasks)',
        "Python — DE patterns",
    )

    pdf.sub_heading("Pandas for Data Engineering")
    pdf.code_block(
        '# Read large files efficiently\n'
        'df = pd.read_parquet("data.parquet",\n'
        '    columns=["id", "amount", "ts"])  # read only needed cols\n'
        '\n'
        '# Chunked CSV reading\n'
        'for chunk in pd.read_csv("big.csv", chunksize=100_000):\n'
        '    process(chunk)\n'
        '\n'
        '# Deduplication\n'
        'df = df.drop_duplicates(subset=["id"], keep="last")\n'
        '\n'
        '# Type-safe loading\n'
        'dtypes = {"id": "int64", "amount": "float64",\n'
        '          "status": "category"}\n'
        'df = pd.read_csv("data.csv", dtype=dtypes,\n'
        '    parse_dates=["created_at"])\n'
        '\n'
        '# Efficient groupby + agg\n'
        'result = df.groupby("category").agg(\n'
        '    total=("amount", "sum"),\n'
        '    count=("id", "count"),\n'
        '    avg=("amount", "mean"),\n'
        ').reset_index()\n'
        '\n'
        '# Write to Parquet (always prefer over CSV)\n'
        'df.to_parquet("output.parquet", index=False,\n'
        '    engine="pyarrow", compression="snappy")',
        "Python — pandas for DE",
    )


def _cookbook_sql(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: SQL", 60, 130, 60)

    pdf.cheat_entry("SELECT *, ROW_NUMBER()\n  OVER (PARTITION BY col\n        ORDER BY ts DESC) rn\nFROM t", "Row number per group (dedup with WHERE rn = 1)", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("SELECT *, LAG(val) OVER\n  (ORDER BY ts) AS prev_val\nFROM t", "Previous row value (for change detection)", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("SELECT *, SUM(amount) OVER\n  (ORDER BY ts ROWS BETWEEN\n   UNBOUNDED PRECEDING\n   AND CURRENT ROW) AS running\nFROM t", "Running total / cumulative sum", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("WITH cte AS (\n  SELECT ... FROM t\n)\nSELECT * FROM cte", "Common Table Expression (named subquery)", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("CREATE INDEX CONCURRENTLY\n  idx_name ON t(col);", "Non-blocking index creation (Postgres)", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("EXPLAIN (ANALYZE, BUFFERS)\n  SELECT ... ;", "Query execution plan with I/O stats", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("COPY table FROM '/path/data.csv'\n  WITH (FORMAT csv, HEADER true);", "Bulk load CSV into Postgres", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("\\dt          -- list tables\n\\d+ tablename -- describe table\n\\timing      -- toggle timing", "psql meta-commands", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("COALESCE(col, 0)", "Replace NULL with a default value", "sql")
    pdf.cheat_separator()
    pdf.cheat_entry("PERCENTILE_CONT(0.5) WITHIN\n  GROUP (ORDER BY val) AS median", "Median (50th percentile)", "sql")

    pdf.add_page()
    pdf.sub_heading("Advanced SQL Patterns")
    pdf.code_block(
        '-- Deduplicate rows (keep latest per key)\n'
        'DELETE FROM events a USING events b\n'
        'WHERE a.id < b.id\n'
        '  AND a.event_key = b.event_key;\n'
        '\n'
        '-- Or with ROW_NUMBER:\n'
        'WITH ranked AS (\n'
        '    SELECT *, ROW_NUMBER() OVER (\n'
        '        PARTITION BY event_key\n'
        '        ORDER BY created_at DESC\n'
        '    ) AS rn\n'
        '    FROM events\n'
        ')\n'
        'SELECT * FROM ranked WHERE rn = 1;\n'
        '\n'
        '-- Gap detection (find missing dates)\n'
        'WITH date_series AS (\n'
        '    SELECT generate_series(\n'
        '        \'2024-01-01\'::date,\n'
        '        \'2024-12-31\'::date,\n'
        '        \'1 day\'::interval\n'
        '    )::date AS dt\n'
        ')\n'
        'SELECT dt AS missing_date\n'
        'FROM date_series\n'
        'LEFT JOIN daily_data d ON d.report_date = dt\n'
        'WHERE d.report_date IS NULL;\n'
        '\n'
        '-- Pivot with FILTER (PostgreSQL)\n'
        'SELECT\n'
        '    pickup_date,\n'
        '    COUNT(*) FILTER (WHERE borough = \'Manhattan\') AS manhattan,\n'
        '    COUNT(*) FILTER (WHERE borough = \'Brooklyn\') AS brooklyn,\n'
        '    COUNT(*) FILTER (WHERE borough = \'Queens\') AS queens\n'
        'FROM trips JOIN zones USING (zone_id)\n'
        'GROUP BY pickup_date;',
        "SQL — advanced patterns",
    )


def _cookbook_docker(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Docker", 60, 130, 60)

    pdf.cheat_entry("docker build -t myapp:v1 .", "Build image from Dockerfile in current dir", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker build --target builder .", "Build only up to a specific stage", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker run -d -p 8000:8000\n  --name api myapp:v1", "Run container in background with port mapping", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker run --rm -it myapp sh", "Run interactive shell in a disposable container", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker compose up -d", "Start all services in detached mode", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker compose down -v", "Stop services and remove volumes", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker compose logs -f service", "Follow logs for a specific service", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker exec -it container bash", "Open a shell in a running container", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker system prune -af", "Remove all unused images, containers, volumes", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker volume ls\ndocker volume inspect vol", "List and inspect volumes", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker stats", "Live CPU/memory usage for all containers", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("docker inspect container\n  | jq '.[0].NetworkSettings'", "Inspect container network config", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("Dockerfile Best Practices")
    pdf.code_block(
        '# 1. Use specific base tags (not :latest)\n'
        'FROM python:3.11-slim\n'
        '\n'
        '# 2. Copy dependency files first (layer caching)\n'
        'COPY pyproject.toml .\n'
        'RUN pip install --no-cache-dir .\n'
        '\n'
        '# 3. Copy source code last (changes most often)\n'
        'COPY src/ src/\n'
        '\n'
        '# 4. Run as non-root user\n'
        'RUN useradd -m app\n'
        'USER app',
        "Dockerfile patterns",
    )

    pdf.sub_heading("Docker Troubleshooting")
    pdf.code_block(
        '# Debug a failing container\n'
        'docker logs container_name --tail 50\n'
        'docker inspect container_name | jq \'.[0].State\'\n'
        'docker exec -it container_name /bin/sh\n'
        '\n'
        '# Check resource usage\n'
        'docker stats --no-stream\n'
        '\n'
        '# View layer sizes in an image\n'
        'docker history myimage:v1 --human\n'
        '\n'
        '# Network debugging\n'
        'docker network inspect bridge\n'
        'docker exec container_name ping other_container\n'
        '\n'
        '# Clean everything (nuclear option)\n'
        'docker system prune -af --volumes\n'
        '\n'
        '# Check what is using disk space\n'
        'docker system df -v',
        "Docker troubleshooting",
    )


def _cookbook_airflow(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Airflow", 60, 130, 60)

    pdf.cheat_entry("airflow db init", "Initialize Airflow metadata database", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow db migrate", "Apply pending database migrations", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow dags list", "List all discovered DAGs", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow dags test dag_id\n  2024-01-01", "Test a DAG run for a specific date (no DB writes)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow tasks test dag_id\n  task_id 2024-01-01", "Test a single task (no DB writes)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow dags trigger dag_id", "Manually trigger a DAG run", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow dags backfill dag_id\n  -s 2024-01-01 -e 2024-06-30", "Backfill DAG runs for a date range", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow connections add conn_id\n  --conn-type postgres\n  --conn-host localhost ...", "Create a connection via CLI", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow variables set KEY VALUE", "Set an Airflow variable", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("airflow users create --role Admin\n  --username admin\n  --email a@b.com\n  --firstname A --lastname B\n  --password pass", "Create an admin user", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("DAG Patterns")
    pdf.code_block(
        '# Idempotent task: use UPSERT / MERGE\n'
        '# instead of INSERT to handle re-runs\n'
        '\n'
        '# Task branching\n'
        'from airflow.operators.python import BranchPythonOperator\n'
        '\n'
        'def choose_branch(**ctx):\n'
        '    if ctx["ds"] > "2024-06-01":\n'
        '        return "new_pipeline"\n'
        '    return "legacy_pipeline"\n'
        '\n'
        '# Task groups (sub-DAGs replacement)\n'
        'from airflow.utils.task_group import TaskGroup\n'
        '\n'
        'with TaskGroup("extract") as extract:\n'
        '    task_a = PythonOperator(...)\n'
        '    task_b = PythonOperator(...)\n'
        '\n'
        'extract >> transform >> load',
        "Airflow patterns",
    )

    pdf.sub_heading("Common Pitfalls")
    pdf.code_block(
        '# BAD: doing heavy work inside the DAG file\n'
        'import pandas as pd\n'
        'df = pd.read_csv("huge.csv")  # runs at DAG parse time!\n'
        '\n'
        '# GOOD: defer work to task execution\n'
        'def process(**ctx):\n'
        '    import pandas as pd  # import inside function\n'
        '    df = pd.read_csv("huge.csv")\n'
        '\n'
        '# BAD: passing large data via XCom\n'
        '# XCom stores in metadata DB (Postgres)\n'
        '# return big_dataframe  # will serialize entire DF!\n'
        '\n'
        '# GOOD: pass file paths, not data\n'
        'def extract(**ctx):\n'
        '    path = "/tmp/data.parquet"\n'
        '    download_to(path)\n'
        '    return path  # small string, not data',
        "Airflow anti-patterns",
    )

    pdf.sub_heading("Airflow Docker Compose")
    pdf.code_block(
        '# Minimal Airflow for local development\n'
        'services:\n'
        '  postgres:\n'
        '    image: postgres:16-alpine\n'
        '    environment:\n'
        '      POSTGRES_DB: airflow\n'
        '      POSTGRES_USER: airflow\n'
        '      POSTGRES_PASSWORD: airflow\n'
        '    volumes: [pg_data:/var/lib/postgresql/data]\n'
        '\n'
        '  airflow-init:\n'
        '    image: apache/airflow:2.9.0\n'
        '    entrypoint: /bin/bash -c\n'
        '    command: airflow db migrate && airflow users create\n'
        '      --role Admin --username admin --password admin\n'
        '      --email admin@local.dev --firstname Admin\n'
        '      --lastname User\n'
        '    depends_on: [postgres]\n'
        '\n'
        '  airflow-webserver:\n'
        '    image: apache/airflow:2.9.0\n'
        '    command: airflow webserver\n'
        '    ports: ["8080:8080"]\n'
        '    volumes: [./dags:/opt/airflow/dags]\n'
        '    depends_on: [airflow-init]\n'
        '\n'
        '  airflow-scheduler:\n'
        '    image: apache/airflow:2.9.0\n'
        '    command: airflow scheduler\n'
        '    volumes: [./dags:/opt/airflow/dags]\n'
        '    depends_on: [airflow-init]',
        "YAML — Airflow docker-compose",
    )


def _cookbook_dbt(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: dbt", 60, 130, 60)

    pdf.cheat_entry("dbt init my_project", "Scaffold a new dbt project", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt deps", "Install dbt packages from packages.yml", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt run", "Run all models", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt run --select model_name", "Run a single model", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt run --select model_name+", "Run a model and all downstream", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt run --select +model_name", "Run a model and all upstream", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt test", "Run all schema and data tests", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt build", "Run + test in dependency order", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt build --select state:modified+", "Build only changed models and downstream", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt docs generate\ndbt docs serve", "Generate and serve documentation site", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt snapshot", "Capture SCD Type-2 changes", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt seed", "Load CSV seed files into warehouse", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("Jinja Snippets")
    pdf.code_block(
        '-- Conditional logic\n'
        "{% if target.name == 'prod' %}\n"
        '    {{ config(materialized="table") }}\n'
        '{% else %}\n'
        '    {{ config(materialized="view") }}\n'
        '{% endif %}\n'
        '\n'
        '-- Loop over columns\n'
        '{% set metrics = ["revenue", "cost", "profit"] %}\n'
        '{% for m in metrics %}\n'
        '    SUM({{ m }}) AS total_{{ m }}{% if not loop.last %},{% endif %}\n'
        '{% endfor %}\n'
        '\n'
        '-- Source reference\n'
        'SELECT * FROM {{ source("raw", "orders") }}\n'
        '\n'
        '-- Ref another model\n'
        'SELECT * FROM {{ ref("stg_orders") }}',
        "Jinja patterns",
    )

    pdf.sub_heading("dbt Test Patterns")
    pdf.code_block(
        '# Custom data test (tests/assert_positive_revenue.sql)\n'
        '-- This query should return 0 rows to pass\n'
        'SELECT order_id, revenue\n'
        'FROM {{ ref("fct_orders") }}\n'
        'WHERE revenue < 0\n'
        '\n'
        '# Custom generic test (tests/generic/test_is_even.sql)\n'
        '{% test is_even(model, column_name) %}\n'
        'SELECT {{ column_name }}\n'
        'FROM {{ model }}\n'
        'WHERE {{ column_name }} % 2 != 0\n'
        '{% endtest %}\n'
        '\n'
        '# Usage in schema.yml:\n'
        '# columns:\n'
        '#   - name: quantity\n'
        '#     tests:\n'
        '#       - is_even',
        "SQL — custom dbt tests",
    )

    pdf.sub_heading("dbt packages.yml")
    pdf.code_block(
        '# packages.yml — install community packages\n'
        'packages:\n'
        '  - package: dbt-labs/dbt_utils\n'
        '    version: ">=1.0.0"\n'
        '  - package: calogica/dbt_expectations\n'
        '    version: ">=0.10.0"\n'
        '  - package: dbt-labs/codegen\n'
        '    version: ">=0.12.0"\n'
        '\n'
        '# Install with: dbt deps\n'
        '# Use dbt_utils macros:\n'
        '# {{ dbt_utils.generate_surrogate_key(["col1", "col2"]) }}\n'
        '# {{ dbt_utils.star(from=ref("stg_orders")) }}',
        "YAML — packages.yml",
    )


def _cookbook_spark(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Spark", 60, 130, 60)

    pdf.cheat_entry("spark-submit --master local[*]\n  --packages io.delta:delta-spark\n  etl.py", "Submit a Spark job locally", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("spark-submit --master yarn\n  --deploy-mode cluster\n  --num-executors 10 etl.py", "Submit to YARN cluster", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("df = spark.read.parquet(\"s3://..\")", "Read Parquet from S3", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("df.write.format(\"delta\")\n  .partitionBy(\"date\")\n  .save(\"path\")", "Write partitioned Delta table", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("df.repartition(10)", "Control output file count", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("df.cache()", "Cache DataFrame in memory", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("spark.conf.set(\n  \"spark.sql.shuffle.partitions\",\n  \"200\")", "Tune shuffle partitions", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("F.broadcast(small_df)", "Broadcast join hint for small tables", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("df.explain(\"formatted\")", "Show physical execution plan", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("Common Transformations")
    pdf.code_block(
        'from pyspark.sql import functions as F\n'
        '\n'
        '# Filter + add columns\n'
        'df = df.filter(F.col("amount") > 0) \\\n'
        '       .withColumn("year", F.year("ts")) \\\n'
        '       .withColumn("amount_usd",\n'
        '           F.col("amount") * F.col("rate"))\n'
        '\n'
        '# Window function\n'
        'from pyspark.sql.window import Window\n'
        'w = Window.partitionBy("user_id").orderBy("ts")\n'
        'df = df.withColumn("prev_amount", F.lag("amount").over(w))\n'
        '\n'
        '# Dedup by key (keep latest)\n'
        'df = df.dropDuplicates(["id"])\n'
        '\n'
        '# Pivot\n'
        'pivoted = df.groupBy("date").pivot("category") \\\n'
        '            .agg(F.sum("amount"))',
        "PySpark patterns",
    )

    pdf.sub_heading("Spark SQL Functions Reference")
    pdf.code_block(
        '# Date/time functions\n'
        'F.year("ts"), F.month("ts"), F.dayofweek("ts")\n'
        'F.date_trunc("month", "ts")   # truncate to month\n'
        'F.datediff("end_ts", "start_ts")  # days between\n'
        '\n'
        '# String functions\n'
        'F.lower("col"), F.upper("col"), F.trim("col")\n'
        'F.regexp_extract("col", r"(\\d+)", 1)\n'
        'F.split("col", ",")  # returns array\n'
        '\n'
        '# Null handling\n'
        'F.coalesce("col1", "col2", F.lit(0))\n'
        'F.when(F.col("x").isNull(), "missing")\n'
        ' .otherwise(F.col("x"))\n'
        '\n'
        '# Aggregation\n'
        'F.count_distinct("user_id")\n'
        'F.approx_count_distinct("user_id")  # faster\n'
        'F.collect_list("tag")   # array of values\n'
        'F.collect_set("tag")    # unique values',
        "PySpark — function reference",
    )


def _cookbook_kafka(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Kafka", 60, 130, 60)

    pdf.cheat_entry("kafka-topics.sh --create\n  --topic trades\n  --partitions 6\n  --replication-factor 3\n  --bootstrap-server :9092", "Create a topic", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("kafka-topics.sh --list\n  --bootstrap-server :9092", "List all topics", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("kafka-topics.sh --describe\n  --topic trades\n  --bootstrap-server :9092", "Show topic details (partitions, ISR)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("kafka-console-producer.sh\n  --topic trades\n  --bootstrap-server :9092", "Produce messages from stdin", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("kafka-console-consumer.sh\n  --topic trades\n  --from-beginning\n  --bootstrap-server :9092", "Consume all messages from beginning", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("kafka-consumer-groups.sh\n  --describe --group mygroup\n  --bootstrap-server :9092", "Show consumer group lag", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("kafka-consumer-groups.sh\n  --reset-offsets --to-earliest\n  --group mygroup --topic trades\n  --execute\n  --bootstrap-server :9092", "Reset consumer group offsets", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("Python Client Patterns")
    pdf.code_block(
        '# confluent-kafka producer with delivery report\n'
        'from confluent_kafka import Producer\n'
        '\n'
        'def delivery_report(err, msg):\n'
        '    if err:\n'
        '        print(f"FAILED: {err}")\n'
        '    else:\n'
        '        print(f"OK: {msg.topic()}[{msg.partition()}]")\n'
        '\n'
        'p = Producer({"bootstrap.servers": "localhost:9092"})\n'
        'p.produce("topic", value=b"hello",\n'
        '          callback=delivery_report)\n'
        'p.flush()\n'
        '\n'
        '# JSON serialization pattern\n'
        'import json\n'
        'p.produce("topic",\n'
        '          key=record["id"].encode(),\n'
        '          value=json.dumps(record).encode())',
        "Python — Kafka patterns",
    )

    pdf.sub_heading("Consumer Patterns")
    pdf.code_block(
        '# Batch consumer with manual commit\n'
        'from confluent_kafka import Consumer\n'
        'import json\n'
        '\n'
        'c = Consumer({\n'
        '    "bootstrap.servers": "localhost:9092",\n'
        '    "group.id": "etl-pipeline",\n'
        '    "auto.offset.reset": "earliest",\n'
        '    "enable.auto.commit": False,\n'
        '    "max.poll.interval.ms": 300000,\n'
        '})\n'
        'c.subscribe(["trades"])\n'
        '\n'
        'BATCH_SIZE = 500\n'
        'batch = []\n'
        '\n'
        'while True:\n'
        '    msg = c.poll(1.0)\n'
        '    if msg is None:\n'
        '        if batch:  # flush remaining\n'
        '            write_to_db(batch)\n'
        '            c.commit()\n'
        '            batch.clear()\n'
        '        continue\n'
        '    if msg.error():\n'
        '        handle_error(msg.error())\n'
        '        continue\n'
        '\n'
        '    batch.append(json.loads(msg.value()))\n'
        '    if len(batch) >= BATCH_SIZE:\n'
        '        write_to_db(batch)\n'
        '        c.commit()  # commit AFTER successful write\n'
        '        batch.clear()',
        "Python — batch consumer",
    )

    pdf.sub_heading("Kafka Configuration Reference")
    pdf.cheat_entry("acks=all", "Wait for all replicas to acknowledge (strongest durability)", "toml")
    pdf.cheat_separator()
    pdf.cheat_entry("linger.ms=10", "Wait 10ms to batch messages (throughput vs latency)", "toml")
    pdf.cheat_separator()
    pdf.cheat_entry("compression.type=snappy", "Compress messages (30-50% size reduction)", "toml")
    pdf.cheat_separator()
    pdf.cheat_entry("enable.idempotence=true", "Exactly-once producer semantics", "toml")
    pdf.cheat_separator()
    pdf.cheat_entry("max.poll.records=500", "Max messages per consumer poll()", "toml")
    pdf.cheat_separator()
    pdf.cheat_entry("session.timeout.ms=45000", "Consumer heartbeat timeout", "toml")
    pdf.cheat_separator()

    pdf.sub_heading("Kafka Docker Compose")
    pdf.code_block(
        '# Minimal Kafka + Schema Registry for local dev\n'
        'services:\n'
        '  kafka:\n'
        '    image: confluentinc/cp-kafka:7.6.0\n'
        '    environment:\n'
        '      KAFKA_NODE_ID: 1\n'
        '      KAFKA_PROCESS_ROLES: broker,controller\n'
        '      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093\n'
        '      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093\n'
        '      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092\n'
        '      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER\n'
        '      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT\n'
        '      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk\n'
        '    ports: ["9092:9092"]\n'
        '\n'
        '  schema-registry:\n'
        '    image: confluentinc/cp-schema-registry:7.6.0\n'
        '    depends_on: [kafka]\n'
        '    environment:\n'
        '      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092\n'
        '      SCHEMA_REGISTRY_HOST_NAME: schema-registry\n'
        '    ports: ["8081:8081"]',
        "YAML — Kafka docker-compose",
    )


def _cookbook_terraform(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Terraform", 60, 130, 60)

    pdf.cheat_entry("terraform init", "Initialize providers and backend", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform plan -out=tfplan", "Preview changes (save plan file)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform apply tfplan", "Apply saved plan", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform apply -auto-approve", "Apply without interactive prompt", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform destroy", "Destroy all managed resources", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform state list", "List all resources in state", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform state show\n  aws_s3_bucket.lakehouse", "Show details of a specific resource", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform import\n  aws_s3_bucket.lakehouse\n  my-bucket-name", "Import existing resource into state", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform fmt -recursive", "Format all .tf files", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("terraform validate", "Check config syntax and consistency", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("Common Resource Patterns")
    pdf.code_block(
        '# Variables with defaults\n'
        'variable "env" {\n'
        '  type    = string\n'
        '  default = "dev"\n'
        '}\n'
        '\n'
        '# Locals for computed values\n'
        'locals {\n'
        '  prefix = "${var.project}-${var.env}"\n'
        '}\n'
        '\n'
        '# IAM role for Redshift to access S3\n'
        'resource "aws_iam_role" "redshift_s3" {\n'
        '  name = "${local.prefix}-redshift-s3"\n'
        '  assume_role_policy = jsonencode({\n'
        '    Version = "2012-10-17"\n'
        '    Statement = [{\n'
        '      Action = "sts:AssumeRole"\n'
        '      Effect = "Allow"\n'
        '      Principal = {\n'
        '        Service = "redshift.amazonaws.com"\n'
        '      }\n'
        '    }]\n'
        '  })\n'
        '}\n'
        '\n'
        '# Output values\n'
        'output "bucket_arn" {\n'
        '  value = aws_s3_bucket.lakehouse.arn\n'
        '}',
        "HCL patterns",
    )

    pdf.sub_heading("Terraform for Data Pipelines")
    pdf.code_block(
        '# S3 bucket with lifecycle (move old data to Glacier)\n'
        'resource "aws_s3_bucket_lifecycle_configuration" "lake" {\n'
        '  bucket = aws_s3_bucket.lakehouse.id\n'
        '\n'
        '  rule {\n'
        '    id     = "archive-old-bronze"\n'
        '    status = "Enabled"\n'
        '    filter { prefix = "bronze/" }\n'
        '    transition {\n'
        '      days          = 90\n'
        '      storage_class = "GLACIER"\n'
        '    }\n'
        '    expiration { days = 365 }\n'
        '  }\n'
        '}\n'
        '\n'
        '# Redshift with IAM role attachment\n'
        'resource "aws_redshift_cluster" "wh" {\n'
        '  cluster_identifier = "${local.prefix}-wh"\n'
        '  database_name      = "analytics"\n'
        '  node_type          = "dc2.large"\n'
        '  number_of_nodes    = 2\n'
        '  master_username    = "admin"\n'
        '  master_password    = var.redshift_password\n'
        '  iam_roles = [aws_iam_role.redshift_s3.arn]\n'
        '}',
        "HCL — S3 lifecycle + Redshift",
    )


def _cookbook_git(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Git + GitHub Actions", 60, 130, 60)

    pdf.cheat_entry("git log --oneline -20", "View last 20 commits (short format)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git log --graph --all --oneline", "Visual branch history", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git diff --staged", "Show staged changes (before commit)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git stash push -m 'wip'\ngit stash pop", "Stash and restore work-in-progress", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git rebase -i HEAD~3", "Interactively squash/edit last 3 commits", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git cherry-pick <sha>", "Apply a specific commit to current branch", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git bisect start\ngit bisect bad\ngit bisect good <sha>", "Binary search for a bug-introducing commit", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git reflog", "View all HEAD movements (recover lost commits)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("git worktree add ../fix branch", "Work on multiple branches simultaneously", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("GitHub Actions Workflow Patterns")
    pdf.code_block(
        '# Reusable workflow (called by other workflows)\n'
        'on:\n'
        '  workflow_call:\n'
        '    inputs:\n'
        '      environment:\n'
        '        type: string\n'
        '        required: true\n'
        '\n'
        '# Matrix builds\n'
        'strategy:\n'
        '  matrix:\n'
        '    python: ["3.11", "3.12"]\n'
        '    os: [ubuntu-latest, macos-latest]\n'
        '\n'
        '# Cache dependencies\n'
        '- uses: actions/cache@v4\n'
        '  with:\n'
        '    path: ~/.cache/pip\n'
        '    key: ${{ runner.os }}-pip-${{ hashFiles("**/requirements.txt") }}\n'
        '\n'
        '# Deploy on push to main\n'
        'on:\n'
        '  push:\n'
        '    branches: [main]\n'
        '  pull_request:\n'
        '    branches: [main]',
        "YAML — GitHub Actions patterns",
    )

    pdf.sub_heading("Git Branching Strategy")
    pdf.code_block(
        '# Feature branch workflow\n'
        'git checkout -b feature/add-kafka-consumer main\n'
        '# ... make changes, commit ...\n'
        'git push -u origin feature/add-kafka-consumer\n'
        '# Create PR, get review, merge\n'
        '\n'
        '# Release branch workflow\n'
        'git checkout -b release/v1.2.0 main\n'
        '# Cherry-pick hotfixes:\n'
        'git cherry-pick <hotfix-sha>\n'
        'git tag v1.2.0\n'
        'git push origin v1.2.0\n'
        '\n'
        '# Useful aliases for .gitconfig\n'
        '# [alias]\n'
        '#   lg = log --oneline --graph --all\n'
        '#   st = status -sb\n'
        '#   co = checkout\n'
        '#   unstage = reset HEAD --',
        "Git — branching patterns",
    )


def _cookbook_gx(pdf):
    pdf.add_page()
    pdf.section_title("Cookbook: Great Expectations", 60, 130, 60)

    pdf.cheat_entry("great_expectations init", "Initialize a GX project in current directory", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("great_expectations suite new", "Create a new expectation suite interactively", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("great_expectations suite edit\n  suite_name", "Edit an existing suite in a Jupyter notebook", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("great_expectations checkpoint run\n  my_checkpoint", "Run a checkpoint (validate data)", "bash")
    pdf.cheat_separator()
    pdf.cheat_entry("great_expectations docs build", "Build and open Data Docs site", "bash")
    pdf.cheat_separator()

    pdf.sub_heading("Common Expectations")
    pdf.code_block(
        '# Column-level expectations\n'
        'expect_column_values_to_not_be_null("id")\n'
        'expect_column_values_to_be_unique("id")\n'
        'expect_column_values_to_be_between(\n'
        '    "price", min_value=0, max_value=10000)\n'
        'expect_column_values_to_be_in_set(\n'
        '    "status", ["active", "closed", "pending"])\n'
        'expect_column_values_to_match_regex(\n'
        '    "email", r"^[\\w.]+@[\\w.]+\\.[a-z]{2,}$")\n'
        '\n'
        '# Table-level expectations\n'
        'expect_table_row_count_to_be_between(\n'
        '    min_value=1000, max_value=10_000_000)\n'
        'expect_table_columns_to_match_ordered_list(\n'
        '    ["id", "name", "price", "quantity", "ts"])\n'
        '\n'
        '# Multi-column expectations\n'
        'expect_compound_columns_to_be_unique(\n'
        '    ["date", "symbol"])\n'
        'expect_column_pair_values_a_to_be_greater_than_b(\n'
        '    "end_time", "start_time")',
        "Python — GX expectation patterns",
    )

    pdf.sub_heading("Checkpoint Configuration")
    pdf.code_block(
        '# great_expectations/checkpoints/trades.yml\n'
        'name: trades_checkpoint\n'
        'config_version: 1.0\n'
        'class_name: Checkpoint\n'
        'run_name_template: "trades_%Y%m%d"\n'
        'validations:\n'
        '  - batch_request:\n'
        '      datasource_name: warehouse\n'
        '      data_asset_name: trades\n'
        '    expectation_suite_name: trades_quality\n'
        'action_list:\n'
        '  - name: store_validation_result\n'
        '    action:\n'
        '      class_name: StoreValidationResultAction\n'
        '  - name: update_data_docs\n'
        '    action:\n'
        '      class_name: UpdateDataDocsAction',
        "YAML — checkpoint config",
    )

    pdf.sub_heading("GX + Pandas Quick Start")
    pdf.code_block(
        'import great_expectations as gx\n'
        'import pandas as pd\n'
        '\n'
        '# Quick validation without project setup\n'
        'df = pd.read_parquet("data/trades.parquet")\n'
        '\n'
        'context = gx.get_context()  # ephemeral context\n'
        'ds = context.sources.add_pandas("mem")\n'
        'asset = ds.add_dataframe_asset("trades")\n'
        '\n'
        'batch = asset.build_batch_request(\n'
        '    dataframe=df\n'
        ')\n'
        'validator = context.get_validator(\n'
        '    batch_request=batch,\n'
        '    expectation_suite_name="quick_check",\n'
        '    create_expectation_suite_with_name="quick_check",\n'
        ')\n'
        '\n'
        '# Run expectations interactively\n'
        'validator.expect_table_row_count_to_be_between(\n'
        '    min_value=100, max_value=1_000_000)\n'
        'validator.expect_column_values_to_not_be_null("symbol")\n'
        'validator.expect_column_mean_to_be_between(\n'
        '    "price", min_value=1, max_value=1000)\n'
        '\n'
        '# Get results\n'
        'results = validator.validate()\n'
        'print(f"Success: {results.success}")\n'
        'print(f"Failed: {results.statistics["unsuccessful_expectations"]}")',
        "Python — quick validation",
    )

    pdf.sub_heading("GX Project Structure")
    pdf.diagram_block(
        '  great_expectations/\n'
        '  +-- great_expectations.yml       # main config\n'
        '  +-- expectations/\n'
        '  |   +-- trades_quality.json      # expectation suite\n'
        '  |   +-- orders_quality.json\n'
        '  +-- checkpoints/\n'
        '  |   +-- trades_checkpoint.yml    # validation config\n'
        '  +-- plugins/\n'
        '  |   +-- custom_expectations/\n'
        '  |       +-- expect_valid_ticker.py\n'
        '  +-- uncommitted/\n'
        '      +-- data_docs/               # generated HTML reports\n'
        '          +-- local_site/\n'
        '              +-- index.html'
    )

    pdf.sub_heading("Data Quality Strategy by Layer")
    pdf.code_block(
        '# Bronze (raw): minimal checks\n'
        '# - Schema matches expected columns\n'
        '# - Row count > 0\n'
        '# - No completely empty columns\n'
        '\n'
        '# Silver (cleaned): data validity\n'
        '# - Primary keys are unique and not null\n'
        '# - Values in expected ranges\n'
        '# - Foreign keys exist in dimension tables\n'
        '# - Timestamps are in valid ranges\n'
        '\n'
        '# Gold (aggregated): business logic\n'
        '# - Totals match across tables\n'
        '# - No negative revenue/quantities\n'
        '# - Row counts within expected bounds\n'
        '# - Distributions haven\'t shifted dramatically',
        "Data quality strategy",
    )


# ═══════════════════════════════════════════════════════════════
# MAIN BUILD FUNCTION
# ═══════════════════════════════════════════════════════════════

def build_pdf():
    pdf = RoadmapPDF(orientation="P", unit="mm", format="A4")
    pdf._register_fonts()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # ── COVER PAGE ───────────────────────────────────────────
    pdf.add_page()
    pdf.ln(40)
    pdf.set_font(F, "B", 32)
    pdf.set_text_color(25, 100, 200)
    pdf.cell(0, 15, "Modern Data Engineering", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 15, "Roadmap", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)
    pdf.set_font(F, "", 14)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, "8-Week Engineering Sprint  |  12-15 hrs/week", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_draw_color(25, 100, 200)
    pdf.set_line_width(0.8)
    mid = pdf.w / 2
    pdf.line(mid - 40, pdf.get_y(), mid + 40, pdf.get_y())
    pdf.ln(10)

    pdf.set_font(F, "", 11)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(0, 7, "For engineers with SQL + BI experience", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "transitioning to modern data engineering.", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(12)

    pdf.set_font(F, "B", 12)
    pdf.set_text_color(25, 100, 200)
    pdf.cell(0, 8, "Target Stack", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    for line in [
        "Python  |  PostgreSQL  |  Docker  |  Airflow  |  dbt",
        "Spark  |  Kafka  |  AWS (S3 + Redshift)  |  Terraform",
        "GitHub Actions  |  Great Expectations  |  Delta Lake",
    ]:
        pdf.set_font(F, "", 10)
        pdf.set_text_color(70, 70, 70)
        pdf.cell(0, 6, line, align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)

    pdf.set_font(F, "B", 12)
    pdf.set_text_color(25, 100, 200)
    pdf.cell(0, 8, "Weekly Time Split (15 hrs)", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font(F, "", 10)
    pdf.set_text_color(70, 70, 70)
    pdf.cell(0, 6, "Learning: 5 hrs  |  Hands-on: 7 hrs  |  Reading: 3 hrs", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # What's inside callout
    pdf.set_font(F, "B", 11)
    pdf.set_text_color(25, 100, 200)
    pdf.cell(0, 7, "What's Inside This Guide", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font(F, "", 10)
    pdf.set_text_color(70, 70, 70)
    for item in [
        "8 week overviews with topics, resources, goals & deliverables",
        "40+ working code examples (Python, SQL, YAML, HCL, Bash)",
        "Architecture diagrams for every major component",
        "10 tool cheat sheets in the Cookbook appendix",
    ]:
        pdf.cell(0, 6, f"  \u2022  {item}", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── WEEK PAGES (overview + deep-dive) ────────────────────

    # Week 1
    pdf.add_week_page(
        week_num=1,
        title="Python for Data Engineering (Production Style)",
        summary=(
            "This week focuses on leveling up your Python beyond scripting into production-grade "
            "engineering. You already know Python basics\u2014 now you'll learn how professional data "
            "engineers structure projects: proper packaging with pyproject.toml, isolated virtual "
            "environments, structured logging (not print statements), and building APIs that ingest "
            "data from external sources. The emphasis is on writing code that is testable, "
            "maintainable, and deployable\u2014 not just functional."
        ),
        topics=[
            "Project packaging: pyproject.toml, src layout, entry points",
            "Virtual environments: venv, pip-tools, dependency pinning",
            "Structured logging: Python logging module, log levels, JSON formatters",
            "HTTP clients: requests/httpx for API ingestion with retry logic",
            "Error handling: custom exceptions, graceful degradation",
            "Type hints and dataclasses for data contracts",
            "FastAPI basics: building lightweight ingestion endpoints",
            "Config management: environment variables, .env files, pydantic-settings",
        ],
        resources=[
            ("Python Official Tutorial", "https://docs.python.org/3/tutorial/"),
            ("Real Python (production tutorials)", "https://realpython.com/"),
            ("FastAPI Documentation", "https://fastapi.tiangolo.com/"),
            ("Kaggle Datasets", "https://www.kaggle.com/datasets"),
            ("NYC Open Data", "https://opendata.cityofnewyork.us/"),
        ],
        goal="Build production-quality Python skills\u2014 learn to write code that other engineers can read, test, and deploy with confidence.",
        deliverables=[
            "Python project with proper packaging (pyproject.toml, src layout)",
            "FastAPI ingestion service that pulls from a public API (e.g., NYC Open Data)",
            "Raw JSON saved to local storage with timestamped filenames",
            "Structured logging throughout (no print statements)",
            "Unit tests with pytest covering the ingestion logic",
        ],
    )
    _week1_deep_dive(pdf)

    # Week 2
    pdf.add_week_page(
        week_num=2,
        title="Modern SQL + Data Modeling",
        summary=(
            "SQL is the lingua franca of data engineering, and this week you'll move well beyond "
            "SELECT statements. You'll master window functions (ROW_NUMBER, LAG, LEAD, NTILE), "
            "CTEs for readable query composition, and query optimization using EXPLAIN ANALYZE. "
            "The second half focuses on dimensional modeling\u2014 the Kimball methodology that "
            "underpins every modern data warehouse. You'll learn to identify facts (measurable "
            "events) vs. dimensions (descriptive context), design star schemas, and handle slowly "
            "changing dimensions (SCDs). This is the foundation that makes dbt, Redshift, and "
            "Snowflake work effectively."
        ),
        topics=[
            "Window functions: ROW_NUMBER, RANK, LAG, LEAD, NTILE, running totals",
            "Common Table Expressions (CTEs) and recursive queries",
            "Query optimization: EXPLAIN ANALYZE, index strategies, partitioning",
            "Dimensional modeling: facts vs. dimensions, grain definition",
            "Star schema design: fact tables, dimension tables, surrogate keys",
            "Slowly Changing Dimensions (SCD Type 1, 2, 3)",
            "Data normalization vs. denormalization trade-offs",
            "PostgreSQL-specific features: JSONB, array types, materialized views",
        ],
        resources=[
            ("Mode SQL Tutorial", "https://mode.com/sql-tutorial/"),
            ("PostgreSQL Documentation", "https://www.postgresql.org/docs/"),
            ("Kimball Group Resources", "https://www.kimballgroup.com/"),
            ("Chinook Sample Database", "https://github.com/lerocha/chinook-database"),
        ],
        goal="Master analytical SQL patterns and design a dimensional model from scratch\u2014 the skill that separates data engineers from Python scripters.",
        deliverables=[
            "Chinook DB loaded into PostgreSQL with analytical query workbook",
            "10+ analytical queries using window functions, CTEs, and aggregations",
            "Star schema design document (ERD) for a business domain of your choice",
            "Fact + Dimension DDL scripts with surrogate keys and indexes",
            "Query performance comparison: before/after indexing with EXPLAIN ANALYZE",
        ],
    )
    _week2_deep_dive(pdf)

    # Week 3
    pdf.add_week_page(
        week_num=3,
        title="Docker + Local Data Stack",
        summary=(
            "Containerization is non-negotiable in modern data engineering. This week you'll learn "
            "Docker from first principles: images, containers, volumes, networks, and multi-stage "
            "builds. Then you'll use Docker Compose to orchestrate a multi-container local data "
            "stack\u2014 the same pattern used in production. This is where most learners fall behind "
            "because they skip straight to cloud services. Resist that urge. Understanding "
            "containers deeply means you can debug deployment issues, optimize images, and work "
            "with any orchestration platform (Kubernetes, ECS, etc.) later."
        ),
        topics=[
            "Docker fundamentals: images, containers, layers, caching",
            "Dockerfile best practices: multi-stage builds, minimal base images",
            "Docker volumes: named volumes, bind mounts, data persistence",
            "Docker networking: bridge networks, service discovery, port mapping",
            "Docker Compose: service definitions, dependencies, health checks",
            "Environment management: .env files, secrets, config injection",
            "Image optimization: layer caching, .dockerignore, slim images",
            "Debugging containers: logs, exec, inspect, resource limits",
        ],
        resources=[
            ("Docker Getting Started", "https://docs.docker.com/get-started/"),
            ("Docker Compose Docs", "https://docs.docker.com/compose/"),
        ],
        goal="Build a complete local data infrastructure using containers\u2014 understand that every production data platform runs on this foundation.",
        deliverables=[
            "Custom Dockerfiles for Python services (multi-stage, non-root user)",
            "docker-compose.yml with PostgreSQL, Airflow, and pgAdmin",
            "Health checks and dependency ordering between services",
            "Persistent volumes for database data across restarts",
            "README documenting how to bring the stack up and down",
        ],
    )
    _week3_deep_dive(pdf)

    # Week 4
    pdf.add_week_page(
        week_num=4,
        title="Orchestration + ETL with Apache Airflow",
        summary=(
            "Orchestration is what turns ad-hoc scripts into reliable data pipelines. Apache "
            "Airflow is the industry standard\u2014 used at Airbnb, Uber, Spotify, and thousands of "
            "companies. This week you'll learn to think in DAGs (Directed Acyclic Graphs): "
            "decomposing pipelines into tasks with clear dependencies, retries, and scheduling. "
            "You'll understand operators (BashOperator, PythonOperator, PostgresOperator), sensors "
            "for event-driven triggers, XCom for inter-task communication, and the critical "
            "distinction between orchestrating work vs. doing work inside Airflow. The NYC Taxi "
            "dataset is large enough to simulate real-world pipeline challenges."
        ),
        topics=[
            "Airflow architecture: scheduler, webserver, executor, metadata DB",
            "DAG design: task dependencies, trigger rules, branching",
            "Operators: BashOperator, PythonOperator, PostgresOperator, custom operators",
            "Sensors: file sensors, external task sensors, event-driven triggers",
            "XCom: inter-task data passing (and when NOT to use it)",
            "Scheduling: cron expressions, catchup, backfill strategies",
            "Connections and Hooks: managing external system credentials",
            "Best practices: idempotency, atomicity, incremental loads",
        ],
        resources=[
            ("Apache Airflow Docs", "https://airflow.apache.org/docs/"),
            ("Astronomer Learn Airflow", "https://www.astronomer.io/guides/"),
            ("NYC Taxi Trip Data", "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"),
        ],
        goal="Build a production-style ETL pipeline that downloads, transforms, and loads data on a schedule\u2014 with retries, logging, and monitoring.",
        deliverables=[
            "Airflow DAG that downloads NYC Taxi data (monthly partitions)",
            "Transformation tasks: data cleaning, type casting, deduplication with Pandas",
            "Load task: insert into PostgreSQL with upsert logic",
            "Retry and alerting configuration for failure scenarios",
            "Backfill demonstration: re-run historical months on demand",
        ],
    )
    _week4_deep_dive(pdf)

    # Week 5
    pdf.add_week_page(
        week_num=5,
        title="Modern ELT with dbt (Data Build Tool)",
        summary=(
            "dbt has revolutionized analytics engineering by bringing software engineering "
            "practices to SQL transformations. Instead of writing monolithic stored procedures, "
            "you write modular SQL models with version control, testing, and documentation built "
            "in. This week you'll learn the ELT paradigm (Extract-Load first, then Transform in "
            "the warehouse) and build a full dbt project: staging models that clean raw data, "
            "intermediate models for business logic, and mart models optimized for analysts. "
            "You'll add schema tests (not_null, unique, relationships), write custom macros with "
            "Jinja, and generate a documentation site that makes your data warehouse self-describing."
        ),
        topics=[
            "ELT vs ETL: why modern stacks transform in the warehouse",
            "dbt project structure: models, tests, macros, seeds, snapshots",
            "Model materialization: view, table, incremental, ephemeral",
            "Staging -> Intermediate -> Mart layering pattern",
            "Testing: schema tests, data tests, custom test macros",
            "Jinja templating: macros, control structures, dynamic SQL",
            "Documentation: model descriptions, column docs, auto-generated site",
            "Snapshots: tracking slowly changing dimensions with dbt",
        ],
        resources=[
            ("dbt Documentation", "https://docs.getdbt.com/docs/introduction"),
            ("dbt Fundamentals (Free Course)", "https://courses.getdbt.com/"),
        ],
        goal="Build a complete dbt project that transforms raw data into clean, tested, documented analytics models\u2014 the core of modern analytics engineering.",
        deliverables=[
            "dbt project: raw \u2192 staging \u2192 intermediate \u2192 marts layer structure",
            "5+ models with proper materializations (view for staging, table for marts)",
            "Schema tests: not_null, unique, accepted_values, relationships",
            "Custom Jinja macro for a reusable transformation pattern",
            "Generated dbt docs site with model lineage graph",
        ],
    )
    _week5_deep_dive(pdf)

    # Week 6
    pdf.add_week_page(
        week_num=6,
        title="Lakehouse Architecture + Apache Spark",
        summary=(
            "The lakehouse paradigm merges the flexibility of data lakes (cheap storage, any "
            "format) with the reliability of data warehouses (ACID transactions, schema "
            "enforcement). This week you'll learn Apache Spark for distributed data processing "
            "and Delta Lake for building reliable lakehouse tables. You'll understand partitioning "
            "strategies, the difference between Parquet and Delta, time travel for data recovery, "
            "and how to run Spark SQL analytics on large datasets. The US Flights dataset (millions "
            "of rows) is ideal for experiencing Spark's distributed processing power vs. Pandas."
        ),
        topics=[
            "Spark architecture: driver, executors, DAG scheduler, lazy evaluation",
            "DataFrames vs RDDs: when to use each (hint: almost always DataFrames)",
            "Spark SQL: window functions, aggregations, joins at scale",
            "File formats: CSV \u2192 Parquet \u2192 Delta (evolution of storage)",
            "Delta Lake: ACID transactions, schema enforcement/evolution, time travel",
            "Partitioning strategies: date-based, hash, bucketing trade-offs",
            "Performance tuning: broadcast joins, caching, partition pruning",
            "Lakehouse medallion architecture: bronze \u2192 silver \u2192 gold",
        ],
        resources=[
            ("Apache Spark Docs", "https://spark.apache.org/docs/latest/"),
            ("Delta Lake Documentation", "https://docs.delta.io/"),
            ("US Flights Dataset", "https://www.kaggle.com/datasets/usdot/flight-delays"),
        ],
        goal="Build a lakehouse pipeline with Spark and Delta Lake\u2014 understand distributed computing and why it matters when data exceeds single-machine capacity.",
        deliverables=[
            "Spark job: ingest raw CSV \u2192 Parquet conversion with proper schema",
            "Delta Lake tables with partitioning (e.g., by year/month)",
            "Time travel demo: query historical versions of Delta tables",
            "Spark SQL analytics: top routes, delay patterns, carrier performance",
            "Bronze \u2192 Silver \u2192 Gold medallion pipeline implementation",
        ],
    )
    _week6_deep_dive(pdf)

    # Week 7
    pdf.add_week_page(
        week_num=7,
        title="Streaming + Kafka + Data Quality",
        summary=(
            "Real-time data is everywhere: user clickstreams, IoT sensors, financial transactions, "
            "application logs. Apache Kafka is the backbone of streaming infrastructure at "
            "LinkedIn, Netflix, and most modern tech companies. This week you'll learn Kafka's "
            "architecture (brokers, topics, partitions, consumer groups) and build a producer/"
            "consumer pipeline. Then you'll add Great Expectations for data quality\u2014 automated "
            "validation that catches schema drift, null spikes, and distribution anomalies before "
            "bad data reaches your warehouse. Data quality is what separates hobby projects from "
            "production systems."
        ),
        topics=[
            "Kafka architecture: brokers, topics, partitions, replication",
            "Producers: serialization, partitioning strategies, acks",
            "Consumers: consumer groups, offsets, exactly-once semantics",
            "Schema management: Avro/JSON Schema with Schema Registry",
            "Great Expectations: expectations, suites, checkpoints, data docs",
            "Custom expectations: domain-specific validation rules",
            "Data quality in pipelines: inline validation vs. post-load checks",
            "Alerting on data quality failures: integration with Slack/email",
        ],
        resources=[
            ("Apache Kafka Docs", "https://kafka.apache.org/documentation/"),
            ("Great Expectations Docs", "https://docs.greatexpectations.io/"),
        ],
        goal="Build a streaming pipeline with data quality gates\u2014 understand that reliable data is more valuable than fast data.",
        deliverables=[
            "Kafka producer simulating real-time events (e.g., stock trades, web clicks)",
            "Kafka consumer writing events to PostgreSQL with error handling",
            "Great Expectations suite: 10+ expectations on your dataset",
            "Checkpoint integrated into your Airflow DAG (fail pipeline on bad data)",
            "Data Docs site: auto-generated validation reports",
        ],
    )
    _week7_deep_dive(pdf)

    # Week 8
    pdf.add_week_page(
        week_num=8,
        title="Cloud + CI/CD + Infrastructure as Code",
        summary=(
            "Everything you've built locally now goes to the cloud. AWS dominates the job market "
            "for data engineering, so you'll learn S3 (object storage for your lakehouse), "
            "Redshift (cloud data warehouse), and IAM (security). Terraform lets you define all "
            "this infrastructure as code\u2014 version-controlled, reviewable, and reproducible. "
            "Finally, GitHub Actions automates your pipeline: run dbt tests on every PR, lint "
            "SQL, validate schemas, and deploy on merge. This week ties everything together into "
            "a production-grade, cloud-native data platform."
        ),
        topics=[
            "AWS S3: buckets, lifecycle policies, storage classes, IAM policies",
            "AWS Redshift: clusters, COPY command, distribution/sort keys, Spectrum",
            "IAM: least-privilege policies, roles, service accounts",
            "Terraform: providers, resources, variables, state management, modules",
            "Terraform for data infra: S3 buckets, Redshift clusters, IAM roles",
            "GitHub Actions: workflows, jobs, steps, secrets, matrix builds",
            "CI for data: dbt test on PR, SQL linting, schema validation",
            "CD for data: automated deployment of Airflow DAGs and dbt models",
        ],
        resources=[
            ("AWS Documentation", "https://docs.aws.amazon.com/"),
            ("Terraform Documentation", "https://developer.hashicorp.com/terraform/docs"),
            ("GitHub Actions Docs", "https://docs.github.com/en/actions"),
        ],
        goal="Deploy your entire data platform to the cloud with infrastructure as code and automated CI/CD\u2014 the final step to production readiness.",
        deliverables=[
            "Terraform config: S3 bucket + Redshift cluster + IAM roles",
            "Airflow DAG modified to upload data to S3 and COPY into Redshift",
            "GitHub Actions CI: run dbt test and Great Expectations on every PR",
            "GitHub Actions CD: deploy DAGs and dbt models on merge to main",
            "Full architecture diagram documenting the end-to-end platform",
        ],
    )
    _week8_deep_dive(pdf)

    # ── DATASETS PAGE ────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("Public Datasets for Practice", 34, 139, 34)
    pdf.ln(2)
    pdf.body_text(
        "These datasets are freely available and large enough to simulate real-world "
        "data engineering challenges. Use them throughout the 8 weeks."
    )
    for name, url, desc in [
        ("NYC Taxi Trip Data", "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
         "Millions of taxi/rideshare records per month. Ideal for ETL, partitioning, and analytics."),
        ("Kaggle Datasets", "https://www.kaggle.com/datasets",
         "Thousands of curated datasets across domains. Great for exploration and prototyping."),
        ("UCI Machine Learning Repository", "https://archive.ics.uci.edu/",
         "Classic datasets for statistical analysis and data quality exercises."),
        ("Google BigQuery Public Datasets", "https://cloud.google.com/bigquery/public-data",
         "Massive-scale datasets (GitHub, Stack Overflow, Wikipedia) queryable for free."),
        ("GitHub Archive", "https://www.gharchive.org/",
         "Every public GitHub event since 2011. Perfect for streaming and time-series practice."),
        ("World Bank Open Data", "https://data.worldbank.org/",
         "Global economic indicators. Great for dimensional modeling and reporting."),
    ]:
        pdf.set_font(F, "B", 11)
        pdf.set_text_color(40, 40, 40)
        pdf.cell(0, 7, name, new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(F, "", 9)
        pdf.set_text_color(30, 100, 200)
        pdf.cell(0, 5, url, link=url, new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(F, "", 10)
        pdf.set_text_color(80, 80, 80)
        pdf.multi_cell(0, 5, desc)
        pdf.ln(3)

    # ── BOOKS PAGE ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("Recommended Reading", 160, 80, 40)
    pdf.ln(2)
    pdf.body_text(
        "These four books form the intellectual foundation for modern data engineering. "
        "Read them in parallel with the hands-on work\u2014 theory reinforces practice."
    )
    for title, author, desc in [
        ("Designing Data-Intensive Applications",
         "Martin Kleppmann",
         "The \"bible\" of distributed systems for data engineers. Covers replication, "
         "partitioning, consistency, batch/stream processing, and the fundamental trade-offs "
         "behind every technology in your stack. Read chapters alongside the corresponding "
         "week (e.g., Kafka chapters during Week 7)."),
        ("Fundamentals of Data Engineering",
         "Joe Reis & Matt Housley",
         "A modern overview of the entire data engineering landscape: ingestion, storage, "
         "transformation, serving, and orchestration. Excellent for building mental models "
         "of how all the tools fit together."),
        ("The Data Warehouse Toolkit",
         "Ralph Kimball & Margy Ross",
         "The definitive guide to dimensional modeling. Essential for Week 2's data modeling "
         "work and for understanding why star schemas power every BI platform. Focus on the "
         "first 8 chapters."),
        ("Streaming Systems",
         "Tyler Akidau, Slava Chernyak & Reuven Lax",
         "Deep dive into stream processing theory: watermarks, windows, triggers, and "
         "exactly-once semantics. Pairs perfectly with Week 7's Kafka work. Written by "
         "the creators of Google Dataflow."),
    ]:
        pdf.set_font(F, "B", 12)
        pdf.set_text_color(160, 80, 40)
        pdf.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(F, "I", 10)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 5, f"by {author}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(F, "", 10)
        pdf.set_text_color(50, 50, 50)
        pdf.multi_cell(0, 5.5, desc)
        pdf.ln(5)

    # ── STUDY PLAN TEMPLATE ────────────────────────────────
    pdf.add_page()
    pdf.section_title("Weekly Study Plan Template", 100, 80, 160)
    pdf.ln(2)
    pdf.body_text(
        "Use this template to structure your 15 hours each week. Adapt it to your "
        "schedule, but stick to the ratio: 30% learning, 50% building, 20% reading."
    )

    pdf.sub_heading("Monday-Tuesday: Learn (5 hrs)")
    pdf.bullet("Watch tutorials / read documentation for the week's core tool", indent=6)
    pdf.bullet("Follow along with official quickstart guides", indent=6)
    pdf.bullet("Take notes on key concepts and architecture decisions", indent=6)
    pdf.ln(2)

    pdf.sub_heading("Wednesday-Friday: Build (7 hrs)")
    pdf.bullet("Set up the tool locally (Docker, pip install, etc.)", indent=6)
    pdf.bullet("Implement the week's deliverables step by step", indent=6)
    pdf.bullet("Write tests as you build (not after)", indent=6)
    pdf.bullet("Commit working code to Git daily", indent=6)
    pdf.ln(2)

    pdf.sub_heading("Weekend: Read + Reflect (3 hrs)")
    pdf.bullet("Read the corresponding book chapters (see Recommended Reading)", indent=6)
    pdf.bullet("Review and refactor your code from the week", indent=6)
    pdf.bullet("Write a brief README for what you built", indent=6)
    pdf.bullet("Plan next week's focus areas", indent=6)
    pdf.ln(4)

    pdf.sub_heading("Progress Tracking")
    pdf.diagram_block(
        '  Week  Mon   Tue   Wed   Thu   Fri   Sat   Sun   Done?\n'
        '  ----  ----  ----  ----  ----  ----  ----  ----  -----\n'
        '    1   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]\n'
        '    2   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]\n'
        '    3   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]\n'
        '    4   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]\n'
        '    5   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]\n'
        '    6   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]\n'
        '    7   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]\n'
        '    8   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]   [ ]'
    )

    pdf.ln(4)
    pdf.sub_heading("Tips for Success")
    pdf.bullet("Don't skip Docker week. Every production issue traces back to containers.", indent=6)
    pdf.bullet("Write tests from day one. Testing isn't optional in data engineering.", indent=6)
    pdf.bullet("Use Git branches for each week's project. Practice the workflow.", indent=6)
    pdf.bullet("If stuck for > 30 minutes, check Stack Overflow or official Discord.", indent=6)
    pdf.bullet("Focus on understanding WHY, not just HOW. Interview questions test depth.", indent=6)

    # ── CAPSTONE PROJECT PAGE ────────────────────────────────
    pdf.add_page()
    pdf.section_title("Final Portfolio Project", 180, 50, 50)
    pdf.ln(2)
    pdf.body_text(
        "After 8 weeks, you'll combine everything into a single end-to-end project that "
        "demonstrates your capabilities to hiring managers. This is your portfolio centerpiece."
    )

    pdf.sub_heading("Architecture")
    pdf.body_text(
        "Kafka (streaming ingestion) \u2192 Airflow (orchestration) \u2192 S3 (raw storage) \u2192 "
        "Spark/Delta Lake (lakehouse processing) \u2192 dbt (transformation) \u2192 "
        "Redshift (serving layer) \u2192 Tableau/Superset (visualization)"
    )
    pdf.body_text(
        "Cross-cutting concerns: Great Expectations (data quality at every stage), "
        "Terraform (all infrastructure as code), GitHub Actions (CI/CD automation)."
    )

    pdf.sub_heading("Capstone Architecture Diagram")
    pdf.diagram_block(
        '                        +-------------------+\n'
        '                        |   Data Sources    |\n'
        '                        | (APIs, files, DB) |\n'
        '                        +--------+----------+\n'
        '                                 |\n'
        '                    +------------v-----------+\n'
        '                    |     Apache Kafka       |\n'
        '                    |  (streaming ingestion) |\n'
        '                    +------------+-----------+\n'
        '                                 |\n'
        '          +----------+-----------+-----------+----------+\n'
        '          |          |                       |          |\n'
        '  +-------v---+  +--v--------+      +-------v---+      |\n'
        '  |  Airflow  |  |    GX     |      |   S3      |      |\n'
        '  |  (orch)   |  | (quality) |      | (storage) |      |\n'
        '  +-------+---+  +----------+       +-----+-----+      |\n'
        '          |                               |             |\n'
        '          +-------->  Spark / Delta  <----+             |\n'
        '                    (bronze>silver>gold)                |\n'
        '                           |                           |\n'
        '                    +------v------+             +------v------+\n'
        '                    |     dbt     |             |  Terraform  |\n'
        '                    | (transform) |             |   (IaC)     |\n'
        '                    +------+------+             +-------------+\n'
        '                           |\n'
        '                    +------v------+\n'
        '                    |  Redshift   |\n'
        '                    |  (serving)  |\n'
        '                    +------+------+\n'
        '                           |\n'
        '                    +------v------+\n'
        '                    |  Superset   |\n'
        '                    | (dashboards)|\n'
        '                    +-------------+'
    )

    pdf.add_page()
    pdf.sub_heading("Example: End-to-End Pipeline Commands")
    pdf.code_block(
        '# 1. Start infrastructure\n'
        'docker compose up -d\n'
        'terraform init && terraform apply\n'
        '\n'
        '# 2. Start Kafka producer\n'
        'python producer.py --topic trades --rate 1000/sec\n'
        '\n'
        '# 3. Start Kafka consumer (writes to S3)\n'
        'python consumer.py --topic trades --sink s3://lakehouse/bronze/\n'
        '\n'
        '# 4. Run Spark ETL (bronze -> silver -> gold)\n'
        'spark-submit --master local[4] etl/medallion.py\n'
        '\n'
        '# 5. Run dbt transforms\n'
        'cd dbt/ && dbt build --target prod\n'
        '\n'
        '# 6. Run data quality checks\n'
        'great_expectations checkpoint run trades_checkpoint\n'
        '\n'
        '# 7. Load into Redshift\n'
        'psql $REDSHIFT_URL -f sql/copy_gold_to_redshift.sql\n'
        '\n'
        '# 8. All of this is scheduled via Airflow DAG\n'
        '# and tested via GitHub Actions CI/CD',
        "Bash — end-to-end pipeline",
    )

    pdf.sub_heading("Portfolio README Template")
    pdf.code_block(
        '# Project Name: Real-Time Trading Analytics Platform\n'
        '\n'
        '## Architecture\n'
        '[Include the ASCII diagram from this guide]\n'
        '\n'
        '## Tech Stack\n'
        'Python | Kafka | Spark | Delta Lake | dbt | Airflow\n'
        'PostgreSQL | Redshift | Terraform | GitHub Actions | GX\n'
        '\n'
        '## Quick Start\n'
        '```bash\n'
        'git clone https://github.com/you/project.git\n'
        'cp .env.example .env\n'
        'docker compose up -d\n'
        'make setup  # install deps, init DBs, seed data\n'
        'make run    # start all pipeline components\n'
        '```\n'
        '\n'
        '## Data Flow\n'
        '1. Kafka producer generates simulated trade events\n'
        '2. Consumer writes raw events to S3 (bronze)\n'
        '3. Spark cleans, deduplicates, enriches (silver)\n'
        '4. dbt aggregates into analytics tables (gold)\n'
        '5. GX validates data quality at each stage\n'
        '6. Airflow orchestrates the entire pipeline\n'
        '7. Superset dashboards visualize key metrics',
        "Markdown — README template",
    )

    pdf.sub_heading("What to Include in Your GitHub Repo")
    for item in [
        "Professional README with architecture diagram",
        "Docker Compose for local development",
        "Terraform configs for cloud deployment",
        "Airflow DAGs with clear documentation",
        "dbt project with tests and generated docs",
        "Great Expectations suites and data docs",
        "GitHub Actions workflows (CI + CD)",
        "Screenshots of dashboards and monitoring",
        "Setup instructions that actually work (test them!)",
    ]:
        pdf.bullet(item)

    pdf.ln(4)
    pdf.sub_heading("Realistic Outcomes After 8 Weeks")
    pdf.body_text(
        "You won't be a \"master\"\u2014 that takes years of production experience. But you will:"
    )
    for outcome in [
        "Understand and work with the modern data stack end-to-end",
        "Deploy reliable batch and streaming data pipelines",
        "Use infrastructure as code for reproducible environments",
        "Apply data quality practices that prevent production incidents",
        "Be interview-ready for mid-level data engineering roles",
        "Have a portfolio project that demonstrates real engineering skills",
    ]:
        pdf.bullet(outcome)

    # ═══════════════════════════════════════════════════════════
    # INTERVIEW PREP
    # ═══════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("Interview Prep: Common Questions", 150, 60, 60)
    pdf.ln(2)
    pdf.body_text(
        "These questions come up repeatedly in data engineering interviews. "
        "Use your project experience to answer with concrete examples."
    )

    pdf.sub_heading("System Design")
    pdf.bullet("Design a real-time analytics pipeline for e-commerce clickstream data.", indent=6)
    pdf.bullet("How would you build a data lake that supports both batch and streaming?", indent=6)
    pdf.bullet("Design a pipeline that processes 1TB of data daily with SLA guarantees.", indent=6)
    pdf.bullet("How do you handle schema evolution in a production data pipeline?", indent=6)
    pdf.ln(2)

    pdf.sub_heading("SQL & Modeling")
    pdf.bullet("Explain the difference between star schema and snowflake schema.", indent=6)
    pdf.bullet("Write a query to find the top 3 products per category by revenue.", indent=6)
    pdf.bullet("What are SCD Type 1 vs Type 2? When would you use each?", indent=6)
    pdf.bullet("How do you optimize a slow query? Walk through your approach.", indent=6)
    pdf.ln(2)

    pdf.sub_heading("Data Quality & Operations")
    pdf.bullet("How do you ensure data quality in a production pipeline?", indent=6)
    pdf.bullet("A pipeline failed at 3 AM. Walk through your incident response.", indent=6)
    pdf.bullet("How do you handle late-arriving data in a streaming pipeline?", indent=6)
    pdf.bullet("Explain exactly-once semantics. Is it really possible?", indent=6)
    pdf.ln(2)

    pdf.sub_heading("Architecture & Trade-offs")
    pdf.bullet("ETL vs ELT: when would you choose each approach?", indent=6)
    pdf.bullet("Batch vs streaming: what factors drive the decision?", indent=6)
    pdf.bullet("When would you use Spark vs a simpler tool like Pandas?", indent=6)
    pdf.bullet("How do you decide between managed cloud services and self-hosted?", indent=6)
    pdf.ln(2)

    pdf.sub_heading("Sample Answer Framework (STAR)")
    pdf.code_block(
        'Situation: "In my portfolio project, I built a real-time\n'
        '           stock trading pipeline using Kafka and Spark."\n'
        '\n'
        'Task:      "I needed to handle 10K events/sec with < 5 min\n'
        '           end-to-end latency and zero data loss."\n'
        '\n'
        'Action:    "I configured Kafka with acks=all and\n'
        '           enable.idempotence=true for exactly-once\n'
        '           delivery. I used Delta Lake MERGE for upserts\n'
        '           and Great Expectations checkpoints to catch\n'
        '           schema drift before data reached the warehouse."\n'
        '\n'
        'Result:    "The pipeline processed 500K events/day with\n'
        '           99.9% uptime. Data quality checks caught 3\n'
        '           schema changes during development that would\n'
        '           have caused silent data corruption."',
        "Interview answer framework",
    )

    # ═══════════════════════════════════════════════════════════
    # QUICK REFERENCE: When to Use What
    # ═══════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("Quick Reference: When to Use What", 100, 50, 150)
    pdf.ln(2)
    pdf.body_text(
        "A decision guide for choosing the right tool for common data engineering tasks."
    )

    pdf.sub_heading("Data Storage")
    pdf.cheat_entry("PostgreSQL", "Structured data < 1TB, strong consistency, ACID, complex queries")
    pdf.cheat_separator()
    pdf.cheat_entry("S3 / Object Storage", "Any size, any format, cheap, immutable files (Parquet, JSON, CSV)")
    pdf.cheat_separator()
    pdf.cheat_entry("Delta Lake / Iceberg", "ACID on object storage, time travel, schema evolution, lakehouse")
    pdf.cheat_separator()
    pdf.cheat_entry("Redshift / BigQuery", "Analytical queries on TB-PB scale, columnar, managed")
    pdf.cheat_separator()

    pdf.sub_heading("Data Processing")
    pdf.cheat_entry("Pandas", "Single machine, < 10GB, prototyping, quick analysis")
    pdf.cheat_separator()
    pdf.cheat_entry("Apache Spark", "Distributed, > 10GB, production ETL, cluster processing")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt", "SQL transforms in warehouse, testing, documentation, lineage")
    pdf.cheat_separator()
    pdf.cheat_entry("Apache Flink", "True streaming (event-at-a-time), low latency, stateful processing")
    pdf.cheat_separator()

    pdf.sub_heading("Orchestration")
    pdf.cheat_entry("Apache Airflow", "Batch scheduling, DAG-based, mature ecosystem, Python-native")
    pdf.cheat_separator()
    pdf.cheat_entry("Dagster", "Asset-oriented, type-safe, great testing, modern alternative")
    pdf.cheat_separator()
    pdf.cheat_entry("GitHub Actions", "CI/CD, lightweight scheduling, code-triggered workflows")
    pdf.cheat_separator()

    pdf.sub_heading("Streaming")
    pdf.cheat_entry("Apache Kafka", "High-throughput message bus, event sourcing, pub/sub at scale")
    pdf.cheat_separator()
    pdf.cheat_entry("AWS SQS / SNS", "Simple queuing, managed, no cluster ops, lower throughput")
    pdf.cheat_separator()

    pdf.sub_heading("Data Quality")
    pdf.cheat_entry("Great Expectations", "Batch validation, expectation suites, data docs, rich ecosystem")
    pdf.cheat_separator()
    pdf.cheat_entry("dbt tests", "SQL-based tests integrated with transform layer")
    pdf.cheat_separator()

    pdf.sub_heading("Infrastructure")
    pdf.cheat_entry("Docker", "Local dev, packaging, reproducible environments")
    pdf.cheat_separator()
    pdf.cheat_entry("Docker Compose", "Multi-container local stacks, dev/test environments")
    pdf.cheat_separator()
    pdf.cheat_entry("Terraform", "Cloud infrastructure as code, reproducible, version-controlled")
    pdf.cheat_separator()
    pdf.cheat_entry("Kubernetes", "Production container orchestration (after mastering Docker)")
    pdf.cheat_separator()

    # ═══════════════════════════════════════════════════════════
    # COOKBOOK APPENDIX
    # ═══════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.ln(30)
    pdf.set_font(F, "B", 28)
    pdf.set_text_color(60, 130, 60)
    pdf.cell(0, 15, "COOKBOOK", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font(F, "", 14)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, "Quick-Reference Cheat Sheets", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_font(F, "", 11)
    pdf.set_text_color(70, 70, 70)
    tools = [
        "Python", "SQL", "Docker", "Airflow", "dbt",
        "Spark", "Kafka", "Terraform", "Git + GitHub Actions",
        "Great Expectations",
    ]
    for tool in tools:
        pdf.cell(0, 7, f"  \u2022  {tool}", align="C", new_x="LMARGIN", new_y="NEXT")

    _cookbook_python(pdf)
    _cookbook_sql(pdf)
    _cookbook_docker(pdf)
    _cookbook_airflow(pdf)
    _cookbook_dbt(pdf)
    _cookbook_spark(pdf)
    _cookbook_kafka(pdf)
    _cookbook_terraform(pdf)
    _cookbook_git(pdf)
    _cookbook_gx(pdf)

    # ═══════════════════════════════════════════════════════════
    # GLOSSARY
    # ═══════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("Glossary of Key Terms", 80, 80, 80)
    pdf.ln(2)

    glossary = [
        ("ACID", "Atomicity, Consistency, Isolation, Durability \u2014 transaction guarantees for databases"),
        ("Backfill", "Re-running a pipeline for historical date ranges to fill in missing data"),
        ("CDC", "Change Data Capture \u2014 tracking row-level changes in a database"),
        ("CTE", "Common Table Expression \u2014 named subquery using WITH clause"),
        ("DAG", "Directed Acyclic Graph \u2014 workflow of tasks with dependencies (no cycles)"),
        ("Data Lake", "Storage layer that holds raw data in any format (typically object storage)"),
        ("Data Lakehouse", "Architecture combining data lake flexibility with warehouse reliability"),
        ("Delta Lake", "Open-source storage layer adding ACID transactions to data lakes"),
        ("ELT", "Extract, Load, Transform \u2014 load raw data first, transform in the warehouse"),
        ("ETL", "Extract, Transform, Load \u2014 transform data before loading into the warehouse"),
        ("Grain", "The level of detail in a fact table (e.g., one row per transaction)"),
        ("IaC", "Infrastructure as Code \u2014 managing infra through version-controlled config files"),
        ("Idempotent", "Operation that produces the same result regardless of how many times it runs"),
        ("Medallion", "Architecture pattern with bronze (raw), silver (cleaned), gold (aggregated) layers"),
        ("Partition", "Physical division of data by a key (e.g., date) for faster queries"),
        ("SCD", "Slowly Changing Dimension \u2014 techniques for tracking dimension changes over time"),
        ("Schema Registry", "Service that stores and validates Avro/JSON schemas for Kafka messages"),
        ("Surrogate Key", "Artificial key (auto-increment or hash) used instead of natural business key"),
        ("XCom", "Airflow mechanism for passing small data between tasks (cross-communication)"),
        ("Consumer Group", "Set of Kafka consumers that share work reading from topic partitions"),
        ("Offset", "Kafka's pointer tracking which messages a consumer has processed"),
        ("Bloom Filter", "Probabilistic data structure for fast \"element not present\" checks"),
        ("Compaction", "Merging small files into larger ones for better read performance"),
        ("Materialized View", "Pre-computed query result stored as a table, refreshed on demand"),
        ("Window Function", "SQL function that operates on a set of rows related to the current row"),
        ("Star Schema", "Dimensional model with a central fact table joined to dimension tables"),
        ("Fact Table", "Table storing measurable events (transactions, clicks, trips) at defined grain"),
        ("Dimension Table", "Table storing descriptive context (dates, locations, customers)"),
    ]

    for term, definition in glossary:
        pdf.set_font(F, "B", 10)
        pdf.set_text_color(40, 40, 40)
        pdf.cell(28, 5.5, term)
        pdf.set_font(F, "", 9)
        pdf.set_text_color(70, 70, 70)
        pdf.multi_cell(0, 5.5, definition)
        pdf.ln(1)

    # ── Save ─────────────────────────────────────────────────
    output_path = "/Users/hpradip/workspace/DE-project/Modern_Data_Engineering_Roadmap.pdf"
    pdf.output(output_path)
    return output_path


if __name__ == "__main__":
    path = build_pdf()
    print(f"PDF generated: {path}")
