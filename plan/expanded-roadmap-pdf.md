# Plan: Expanded DE Roadmap PDF with Code Examples, Diagrams & Cookbook

## What Was Built
An 80-page PDF generated from a single Python script (`generate_roadmap_pdf.py`) using fpdf2. The PDF serves as a comprehensive 8-week Modern Data Engineering learning roadmap with working code examples, architecture diagrams, and tool cheat sheets.

## PDF Structure (80 pages)

```
Cover page + "What's Inside"                    (1 pg)
Week 1 overview + deep-dive (Python)             (1 + 4 pg)
Week 2 overview + deep-dive (SQL)                (1 + 4 pg)
Week 3 overview + deep-dive (Docker)             (1 + 4 pg)
Week 4 overview + deep-dive (Airflow)            (1 + 5 pg)
Week 5 overview + deep-dive (dbt)                (1 + 4 pg)
Week 6 overview + deep-dive (Spark/Delta)        (1 + 4 pg)
Week 7 overview + deep-dive (Kafka + GX)         (1 + 5 pg)
Week 8 overview + deep-dive (Cloud + CI/CD)      (1 + 5 pg)
Datasets page                                    (1 pg)
Books page                                       (1 pg)
Study Plan Template                              (1 pg)
Capstone project + diagrams + README template    (3 pg)
Interview Prep                                   (2 pg)
Quick Reference: When to Use What                (2 pg)
COOKBOOK cover page                               (1 pg)
  - Python cheat sheet                           (2 pg)
  - SQL cheat sheet                              (2 pg)
  - Docker cheat sheet                           (2 pg)
  - Airflow cheat sheet                          (2 pg)
  - dbt cheat sheet                              (2 pg)
  - Spark cheat sheet                            (2 pg)
  - Kafka cheat sheet                            (2 pg)
  - Terraform cheat sheet                        (2 pg)
  - Git + GitHub Actions cheat sheet             (2 pg)
  - Great Expectations cheat sheet               (2 pg)
Glossary                                         (1 pg)
```

## Technical Decisions

### Font Setup (CRITICAL)
- **Body text**: Arial TTF family (`/System/Library/Fonts/Supplemental/Arial.ttf` + Bold/Italic/BoldItalic)
- **Unicode symbols** (checkmarks): Arial Unicode (`/Library/Fonts/Arial Unicode.ttf`)
- **Monospace (code blocks)**: **Andale Mono** (`/System/Library/Fonts/Supplemental/Andale Mono.ttf`)

> **WARNING: Courier New TTF is BROKEN with fpdf2 on macOS.** Text renders as invisible/empty.
> The built-in `Courier` font works but doesn't support Unicode (em dashes, arrows, etc.).
> **Use Andale Mono** — it's a system TTF, supports Unicode, and renders correctly.

### PDF Helper Methods

1. **`code_block(code, language="")`**
   - Monospace text on gray (#F5F5F5) background with border
   - Uses `cell(w, line_h, text, fill=True)` per line (NOT rect + cell overlay — that causes invisible text)
   - Auto page-break check before rendering
   - Optional language label above the block (italic, gray)

2. **`diagram_block(text)`**
   - Monospace text on light blue (#E6F2FF) background with border
   - Same fill-per-line approach as code_block
   - Used for ASCII architecture diagrams

3. **`tool_heading(name, description)`**
   - Tool name (bold, blue) + em-dash + description (regular, gray) on one line

4. **`cheat_entry(command, description)`**
   - Two-column layout: command (monospace, 75mm) + description (regular, remaining width)
   - Auto page-break check per row

5. **`cheat_separator()`**
   - Thin gray line between cheat entries

### Key Rendering Pattern

```python
# CORRECT: fill per line, then draw border
y_start = self.get_y()
self.cell(w, 2, "", fill=True, new_x="LMARGIN", new_y="NEXT")  # top padding
for line in lines:
    self.cell(w, line_h, "  " + line, fill=True, new_x="LMARGIN", new_y="NEXT")
self.cell(w, 2, "", fill=True, new_x="LMARGIN", new_y="NEXT")  # bottom padding
self.rect(self.l_margin, y_start, w, self.get_y() - y_start, style="D")  # border only

# WRONG: draw filled rect then overlay text (text becomes invisible)
# self.rect(x, y, w, h, style="DF")  # <-- fills background
# self.cell(0, h, text)              # <-- text invisible on some fonts
```

### Existing Helper Methods (from original PDF)
- `section_title(title, r, g, b)` — colored heading with underline
- `sub_heading(text)` — bold 12pt subheading
- `body_text(text)` — regular 10pt paragraph with multi_cell
- `bullet(text, indent)` — bulleted list item
- `link_bullet(label, url)` — bullet with clickable link
- `goal_box(goal_text)` — blue background box with "GOAL:" prefix
- `deliverable_box(items)` — green background box with checkmark items
- `add_week_page(week_num, title, summary, topics, resources, goal, deliverables)` — full week overview page

## Content Per Week Deep-Dive

Each week gets 4-6 pages after its overview page:

| Week | Topic | Code Examples | Diagrams |
|------|-------|--------------|----------|
| 1 | Python | pyproject.toml, logging, FastAPI, dataclasses, httpx retry, pytest, pydantic-settings | Project structure tree |
| 2 | SQL | Window functions, CTEs, star schema DDL, EXPLAIN ANALYZE, SCD Type 2, materialized views, JSONB | — |
| 3 | Docker | Multi-stage Dockerfile, docker-compose.yml, .dockerignore, health checks, networking, volumes | Container stack |
| 4 | Airflow | Full ETL DAG, XCom patterns, connections, backfill, sensors, custom operator | DAG flow |
| 5 | dbt | dbt_project.yml, staging model, mart model, schema.yml, Jinja macro, incremental model, sources, snapshot | Model layers |
| 6 | Spark | PySpark read+transform, Delta time travel, Spark SQL, performance tuning, Delta MERGE | Medallion lakehouse |
| 7 | Kafka + GX | Producer, consumer, Avro schema, GX suite, custom expectation, GX in Airflow | Streaming pipeline |
| 8 | Cloud + CI/CD | Terraform main.tf, GitHub Actions CI + CD, Redshift COPY, IAM policy, variables/outputs | Cloud architecture |

## Cookbook Cheat Sheets (per tool)

Each cheat sheet has:
- 8-12 `cheat_entry()` command/description pairs
- 1-2 `code_block()` pattern examples
- Some include `sub_heading()` sections for patterns/tips

## Verification Steps

1. `python3 generate_roadmap_pdf.py` — no errors, no warnings (ignore fpdf ResourceWarning)
2. File size: ~250-300KB with embedded Andale Mono font
3. Page count: ~80 pages
4. Visual check: code blocks have visible monospace text on gray/blue backgrounds
5. Check links are clickable, page breaks are clean

## Dependencies
- `fpdf2` (pip install fpdf2)
- macOS system fonts: Arial, Arial Unicode, Andale Mono
- `poppler` (brew install poppler) — only for PDF-to-image verification

## How to Regenerate
```bash
python3 generate_roadmap_pdf.py
# Output: Modern_Data_Engineering_Roadmap.pdf
```

## Pitfalls Encountered

1. **Courier New TTF renders invisible text** with fpdf2 on macOS — switched to Andale Mono
2. **Built-in Courier can't handle Unicode** — em dashes (`—`) and arrows (`→`) in code comments cause `FPDFUnicodeEncodingException`
3. **rect(style="DF") + cell() overlay** — text rendered on top of a filled rect can be invisible depending on font/driver; use `cell(fill=True)` per line instead
4. **Auto page break inside code blocks** — must check if block fits before rendering, otherwise rect stays on old page while text flows to new page
