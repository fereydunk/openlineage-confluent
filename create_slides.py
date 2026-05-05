"""Create the OpenLineage Confluent slide deck in Google Slides."""

from __future__ import annotations

from google.auth import default
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive",
]

# ── Colors ──────────────────────────────────────────────────────────────────
NAVY   = {"red": 0.039, "green": 0.145, "blue": 0.251}   # #0A2540
BLUE   = {"red": 0.0,   "green": 0.451, "blue": 0.902}   # #0073E6
TEAL   = {"red": 0.0,   "green": 0.722, "blue": 0.851}   # #00B8D9
WHITE  = {"red": 1.0,   "green": 1.0,   "blue": 1.0}
SLATE  = {"red": 0.957, "green": 0.969, "blue": 0.984}   # #F4F7FB
MUTED  = {"red": 0.42,  "green": 0.478, "blue": 0.553}   # #6B7A8D
TEXT   = {"red": 0.102, "green": 0.169, "blue": 0.235}   # #1A2B3C
GREEN  = {"red": 0.0,   "green": 0.784, "blue": 0.325}   # #00C853
ORANGE = {"red": 1.0,   "green": 0.427, "blue": 0.0}     # #FF6D00
LGRAY  = {"red": 0.882, "green": 0.910, "blue": 0.941}   # card border

# ── Layout helpers ───────────────────────────────────────────────────────────
W, H = 9144000, 5143500   # EMU for 25.4 cm × 14.29 cm (16:9)


def emu(cm: float) -> int:
    return int(cm * 360000)


def rgb(r: int, g: int, b: int) -> dict:
    return {"red": r / 255, "green": g / 255, "blue": b / 255}


def solid(color: dict) -> dict:
    return {"solidFill": {"color": {"rgbColor": color}}}


def pt(n: float) -> dict:
    return {"magnitude": n, "unit": "PT"}


def textbox(obj_id, text, x, y, w, h,
            font_size=18, bold=False, color=None,
            bg=None, align="LEFT", v_align="TOP",
            font="Google Sans"):
    color = color or TEXT
    reqs = [
        {
            "createShape": {
                "objectId": obj_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": _cur_slide,
                    "size": {"width": {"magnitude": w, "unit": "EMU"},
                             "height": {"magnitude": h, "unit": "EMU"}},
                    "transform": {"scaleX": 1, "scaleY": 1,
                                  "translateX": x, "translateY": y,
                                  "unit": "EMU"},
                },
            }
        },
        {
            "insertText": {
                "objectId": obj_id,
                "insertionIndex": 0,
                "text": text,
            }
        },
        {
            "updateTextStyle": {
                "objectId": obj_id,
                "style": {
                    "fontFamily": font,
                    "fontSize": pt(font_size),
                    "bold": bold,
                    "foregroundColor": {"opaqueColor": {"rgbColor": color}},
                },
                "textRange": {"type": "ALL"},
                "fields": "fontFamily,fontSize,bold,foregroundColor",
            }
        },
        {
            "updateParagraphStyle": {
                "objectId": obj_id,
                "style": {
                    "alignment": align,
                    "spaceAbove": pt(0),
                    "spaceBelow": pt(0),
                },
                "textRange": {"type": "ALL"},
                "fields": "alignment,spaceAbove,spaceBelow",
            }
        },
        {
            "updateShapeProperties": {
                "objectId": obj_id,
                "shapeProperties": {
                    "contentAlignment": v_align,
                    "shapeBackgroundFill": solid(bg) if bg else {"propertyState": "NOT_RENDERED"},
                },
                "fields": "contentAlignment,shapeBackgroundFill",
            }
        },
    ]
    return reqs


def rect(obj_id, x, y, w, h, color):
    return [
        {
            "createShape": {
                "objectId": obj_id,
                "shapeType": "RECTANGLE",
                "elementProperties": {
                    "pageObjectId": _cur_slide,
                    "size": {"width": {"magnitude": w, "unit": "EMU"},
                             "height": {"magnitude": h, "unit": "EMU"}},
                    "transform": {"scaleX": 1, "scaleY": 1,
                                  "translateX": x, "translateY": y,
                                  "unit": "EMU"},
                },
            }
        },
        {
            "updateShapeProperties": {
                "objectId": obj_id,
                "shapeProperties": {
                    "shapeBackgroundFill": solid(color),
                    "outline": {"propertyState": "NOT_RENDERED"},
                },
                "fields": "shapeBackgroundFill,outline",
            }
        },
    ]


# ── Slide factory ────────────────────────────────────────────────────────────
_cur_slide = ""
_slide_idx = 0
_all_requests: list[dict] = []
_slide_ids: list[str] = []


def new_slide(layout="BLANK", bg=None):
    global _cur_slide, _slide_idx
    _slide_idx += 1
    sid = f"slide_{_slide_idx:02d}"
    _cur_slide = sid
    _slide_ids.append(sid)
    reqs: list[dict] = [{"createSlide": {"objectId": sid,
                                          "slideLayoutReference": {"predefinedLayout": layout}}}]
    if bg:
        reqs.append({
            "updatePageProperties": {
                "objectId": sid,
                "pageProperties": {"pageBackgroundFill": solid(bg)},
                "fields": "pageBackgroundFill",
            }
        })
    _all_requests.extend(reqs)


def add(*reqs_lists):
    for lst in reqs_lists:
        _all_requests.extend(lst)


def oid(suffix):
    return f"{_cur_slide}_{suffix}"


def header_bar(title: str, subtitle: str = ""):
    add(rect(oid("hbg"), 0, 0, W, emu(3.2), NAVY))
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    add(textbox(oid("htitle"), title,
                emu(1.2), emu(0.4), emu(21), emu(1.5),
                font_size=26, bold=True, color=WHITE))
    if subtitle:
        add(textbox(oid("hsub"), subtitle,
                    emu(1.2), emu(1.9), emu(21), emu(1.0),
                    font_size=13, color=MUTED))


def card(suffix, title, body, x, y, w, h, accent=BLUE):
    card_id = oid(suffix)
    txt_id  = oid(suffix + "_t")
    add(rect(card_id, x, y, w, h, WHITE))
    add(rect(oid(suffix + "_acc"), x, y, emu(0.12), h, accent))
    add(textbox(txt_id, f"{title}\n{body}",
                x + emu(0.22), y + emu(0.15), w - emu(0.4), h - emu(0.3),
                font_size=11, color=TEXT))
    # bold the title line via separate style request
    _all_requests.append({
        "updateTextStyle": {
            "objectId": txt_id,
            "style": {"bold": True, "fontSize": pt(12)},
            "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": len(title)},
            "fields": "bold,fontSize",
        }
    })


def stat_box(suffix, num, lbl, x, y, w):
    h = emu(1.8)
    add(rect(oid(suffix + "_bg"), x, y, w, h, WHITE))
    add(textbox(oid(suffix + "_num"), num,
                x, y + emu(0.1), w, emu(1.1),
                font_size=36, bold=True, color=BLUE, align="CENTER"))
    add(textbox(oid(suffix + "_lbl"), lbl,
                x, y + emu(1.1), w, emu(0.6),
                font_size=9, bold=True, color=MUTED, align="CENTER"))


def table_slide(headers: list[str], rows: list[list[str]],
                col_widths: list[float], top: float = 3.5):
    """Render a simple table using individual text boxes."""
    x0 = emu(0.7)
    y0 = emu(top)
    row_h = emu(0.75)
    header_h = emu(0.65)

    # header background
    add(rect(oid("th_bg"), x0, y0, W - emu(1.4), header_h, NAVY))

    cx = x0
    for i, (hdr, cw) in enumerate(zip(headers, col_widths)):
        add(textbox(oid(f"th_{i}"), hdr, cx + emu(0.1), y0 + emu(0.05),
                    emu(cw) - emu(0.1), header_h,
                    font_size=9, bold=True, color=WHITE, v_align="MIDDLE"))
        cx += emu(cw)

    for ri, row in enumerate(rows):
        ry = y0 + header_h + ri * row_h
        bg = SLATE if ri % 2 == 0 else WHITE
        add(rect(oid(f"tr_{ri}_bg"), x0, ry, W - emu(1.4), row_h, bg))
        cx = x0
        for ci, (cell, cw) in enumerate(zip(row, col_widths)):
            add(textbox(oid(f"tr_{ri}_{ci}"), cell,
                        cx + emu(0.1), ry + emu(0.07),
                        emu(cw) - emu(0.15), row_h - emu(0.1),
                        font_size=10,
                        bold=(ci == 0),
                        color=NAVY if ci == 0 else TEXT,
                        v_align="MIDDLE"))
            cx += emu(cw)


# ── Build every slide ────────────────────────────────────────────────────────

def build_slides():

    # ── Slide 1: Title ──────────────────────────────────────────────────────
    new_slide(bg=NAVY)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    add(textbox(oid("eyebrow"), "CONFLUENT  ·  OPENLINEAGE",
                emu(1.2), emu(1.0), emu(22), emu(0.8),
                font_size=11, bold=True, color=TEAL))
    add(textbox(oid("title"), "End-to-End Data Lineage\nfor Confluent Cloud",
                emu(1.2), emu(1.8), emu(22), emu(3.0),
                font_size=40, bold=True, color=WHITE))
    add(textbox(oid("sub"),
                "A bridge that translates every Confluent component — managed connectors, "
                "Flink statements, ksqlDB queries, consumer groups, and self-managed Connect "
                "— into the open OpenLineage standard.",
                emu(1.2), emu(4.8), emu(17), emu(1.8),
                font_size=14, color=MUTED))
    pills = "Managed Connectors  ·  Apache Flink  ·  ksqlDB  ·  Consumer Groups  ·  Self-Managed Connect  ·  Schema Registry"
    add(textbox(oid("pills"), pills,
                emu(1.2), emu(6.6), emu(22), emu(0.8),
                font_size=11, color=TEAL))

    # ── Slide 2: The Challenge ───────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("The Data Lineage Challenge",
               "Modern data platforms are built on Kafka — but lineage visibility stops at the topic boundary")
    CW = (W - emu(1.4)) / 2
    CH = emu(2.8)
    TOP = emu(3.4)
    GAP = emu(0.3)
    card("c1", "Lineage Blind Spots",
         "Managed connectors, Flink jobs, and ksqlDB queries each live in separate control planes with no shared lineage model.",
         emu(0.7), TOP, CW - GAP/2, CH, ORANGE)
    card("c2", "Disconnected Islands",
         "A message flowing from a DatagenSource → Flink → HTTP Sink spans three systems with zero automatic end-to-end tracing.",
         emu(0.7) + CW + GAP/2, TOP, CW - GAP/2, CH, ORANGE)
    card("c3", "No Standard Format",
         "Each Confluent API returns a different shape. There is no single output format consumable by downstream governance tools.",
         emu(0.7), TOP + CH + GAP, CW - GAP/2, CH, BLUE)
    card("c4", "Hidden Producer Identity",
         "Kafka producers (Java clients, microservices) leave no fingerprint at the broker level — making self-service lineage impossible.",
         emu(0.7) + CW + GAP/2, TOP + CH + GAP, CW - GAP/2, CH, BLUE)

    # ── Slide 3: OpenLineage ─────────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("OpenLineage: The Universal Standard",
               "A vendor-neutral, open specification for capturing data lineage events at runtime")
    CW3 = (W - emu(1.4)) / 3
    TOP3 = emu(3.4)
    CH3 = emu(2.2)
    card("c1", "Event-Driven Model",
         "Every job run emits START / COMPLETE / FAIL events. Lineage is captured in real time, not reconstructed from logs.",
         emu(0.7), TOP3, CW3 - emu(0.15), CH3, TEAL)
    card("c2", "Jobs + Datasets",
         "Jobs (connectors, Flink statements) consume InputDatasets and produce OutputDatasets. Edges are implicit from events.",
         emu(0.7) + CW3, TOP3, CW3 - emu(0.15), CH3, TEAL)
    card("c3", "Extensible via Facets",
         "Core spec is lean. Any system can add custom facets (schema, throughput, SLA) without breaking consumers.",
         emu(0.7) + CW3 * 2, TOP3, CW3 - emu(0.15), CH3, GREEN)

    # Flow diagram row
    nodes = ["Job Run\nFlink / Connect / ksqlDB",
             "RunEvent\nSTART / COMPLETE / FAIL",
             "Lineage Backend\nMarquez / DataHub / Atlas",
             "Governance UI\nSearch, Impact, Audit"]
    colors = [BLUE, {"red": 0, "green": 0.584, "blue": 0.702},
              {"red": 0, "green": 0.529, "blue": 0.353},
              {"red": 0.114, "green": 0.196, "blue": 0.38}]
    NW = emu(4.5)
    NX = emu(0.7)
    NY = emu(6.1)
    NH = emu(1.5)
    AW = emu(0.5)
    total = NW * 4 + AW * 3
    NW2 = int((W - emu(1.4)) / 4) - emu(0.2)
    for i, (label, col) in enumerate(zip(nodes, colors)):
        nx = emu(0.7) + i * (NW2 + emu(0.6))
        add(rect(oid(f"fn_{i}"), nx, NY, NW2, NH, col))
        add(textbox(oid(f"fn_t_{i}"), label,
                    nx + emu(0.1), NY + emu(0.15), NW2 - emu(0.2), NH - emu(0.3),
                    font_size=11, bold=False, color=WHITE, align="CENTER", v_align="MIDDLE"))
        if i < 3:
            add(textbox(oid(f"fn_arr_{i}"), "→",
                        nx + NW2 + emu(0.1), NY + emu(0.4), emu(0.4), emu(0.7),
                        font_size=20, color=MUTED, align="CENTER"))

    # ── Slide 4: Architecture ────────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Bridge Architecture",
               "A lightweight Python service that polls Confluent APIs and translates to OpenLineage events")

    arch_nodes = [
        ("Confluent APIs\nConnect · Flink · Metrics\nksqlDB · Kafka REST · SR",
         {"red": 0.114, "green": 0.196, "blue": 0.38}),
        ("LineageGraph\nUnified edge model", BLUE),
        ("OL Mapper\nNamespace · Facets · RunIds",
         {"red": 0, "green": 0.584, "blue": 0.702}),
        ("Emitter\nDiff-tracking · Parallel · ABORT",
         {"red": 0, "green": 0.529, "blue": 0.353}),
        ("Marquez\nor any OL backend",
         {"red": 0.18, "green": 0.25, "blue": 0.34}),
    ]
    ANW = emu(3.8)
    ANY_ = emu(3.5)
    ANH = emu(1.6)
    for i, (lbl, col) in enumerate(arch_nodes):
        ax = emu(0.5) + i * (ANW + emu(0.35))
        add(rect(oid(f"an_{i}"), ax, ANY_, ANW, ANH, col))
        add(textbox(oid(f"an_t_{i}"), lbl,
                    ax + emu(0.1), ANY_ + emu(0.1), ANW - emu(0.2), ANH - emu(0.2),
                    font_size=10, color=WHITE, align="CENTER", v_align="MIDDLE"))
        if i < 4:
            add(textbox(oid(f"an_a_{i}"), "→",
                        ax + ANW + emu(0.05), ANY_ + emu(0.55), emu(0.25), emu(0.5),
                        font_size=16, color=MUTED, align="CENTER"))

    CW4 = (W - emu(1.4)) / 3
    TOP4 = emu(5.4)
    CH4 = emu(2.0)
    card("ca", "Diff-Tracking State",
         "SQLite state store detects added and removed jobs. Removed jobs emit ABORT events — no stale lineage.",
         emu(0.7), TOP4, CW4 - emu(0.15), CH4, BLUE)
    card("cb", "Parallel Emission",
         "All RunEvents are emitted concurrently via a thread pool — scales to 10 000+ topics without blocking.",
         emu(0.7) + CW4, TOP4, CW4 - emu(0.15), CH4, TEAL)
    card("cc", "Scheduled Pipeline",
         "Sleep-loop pacing: waits poll_interval_seconds AFTER each cycle finishes — no overlap. Run ad-hoc with run-once or continuously with run.",
         emu(0.7) + CW4 * 2, TOP4, CW4 - emu(0.15), CH4, GREEN)

    # ── Slide 5: Seven Sources ────────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Seven Lineage Sources",
               "Complete coverage of every Confluent component that moves data")

    table_slide(
        ["Source", "API Used", "What It Captures", "Job Type"],
        [
            ["Managed Connect", "api.confluent.cloud/connect/v1", "All managed source & sink connectors in Confluent Cloud", "SOURCE_CONNECTOR / SINK_CONNECTOR"],
            ["Flink SQL", "confluent flink statement list -o json", "Persistent Flink statements; input/output topics parsed from SQL", "QUERY"],
            ["Consumer Groups", "Metrics API consumer_lag_offsets", "Any client committing offsets — the only cloud-plane consumer signal", "CONSUMER_GROUP"],
            ["Kafka Producers", "Metrics API received_bytes by client_id", "Per-producer client identity + topics produced", "PRODUCER"],
            ["ksqlDB", "ksqlDB REST POST /ksql", "Persistent CREATE STREAM / TABLE queries and their topic wiring", "QUERY"],
            ["Self-Managed Connect", "GET {endpoint}/connectors?expand=info,status", "On-prem / self-hosted Connect clusters alongside cloud topology", "SOURCE_CONNECTOR / SINK_CONNECTOR"],
            ["Tableflow", "confluent tableflow topic list -o json", "Active Tableflow syncs → Iceberg/Glue (closes the lake-house loop)", "TABLE_SYNC"],
        ],
        [4.5, 5.5, 8.5, 6.0],
        top=3.3,
    )

    SW = (W - emu(1.4)) / 4
    SY = emu(12.2)
    stat_box("s1", "7",    "Lineage Sources",           emu(0.7),               SY, SW - emu(0.2))
    stat_box("s2", "237",  "Tests Passing",             emu(0.7) + SW,          SY, SW - emu(0.2))
    stat_box("s3", "0",    "Credentials for Tests",     emu(0.7) + SW * 2,      SY, SW - emu(0.2))
    stat_box("s4", "10k+", "Topics Supported",          emu(0.7) + SW * 3,      SY, SW - emu(0.2))

    # ── Slide 6: Live Topology ───────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Live Demo Topology",
               "End-to-end lineage stitching four job types into a single directed graph")

    topo_nodes = [
        # (label, color, x, y, w, h) — live env-m2qxq topology, fully bridge-detected
        ("orders-source",            rgb(255, 243, 224), emu(3.5),  emu(3.5), emu(5.5), emu(0.9)),
        ("orders-raw",               rgb(232, 244, 253), emu(11.0), emu(3.5), emu(4.0), emu(0.9)),
        ("Flink: orders-enrich",     rgb(232, 245, 233), emu(6.0),  emu(5.5), emu(5.5), emu(0.9)),
        ("orders-enriched",          rgb(232, 244, 253), emu(13.5), emu(5.5), emu(4.0), emu(0.9)),
        ("orders-http-sink",         rgb(255, 243, 224), emu(8.5),  emu(7.5), emu(5.0), emu(0.9)),
    ]

    for (lbl, bg_col, x, y, w, h) in topo_nodes:
        add(rect(oid(f"tn_{lbl[:8]}"), x, y, w, h, bg_col))
        add(textbox(oid(f"tn_t_{lbl[:8]}"), lbl,
                    x + emu(0.1), y + emu(0.1), w - emu(0.2), h - emu(0.2),
                    font_size=10, bold=True, color=TEXT, v_align="MIDDLE"))

    arrows = [
        ("→", emu(10.25), emu(3.85)),
        ("↓", emu(12.75), emu(4.5)),
        ("→", emu(12.75), emu(5.85)),
        ("↓", emu(15.25), emu(6.5)),
        ("←", emu(13.25), emu(7.85)),
    ]
    for i, (sym, ax, ay) in enumerate(arrows):
        add(textbox(oid(f"arr_{i}"), sym, ax, ay, emu(0.7), emu(0.7),
                    font_size=14, color=MUTED, align="CENTER"))

    legend = [
        ("Orange = Managed / Self-Managed Connect", rgb(255,243,224)),
        ("Green  = Flink persistent SQL statements", rgb(232,245,233)),
        ("Blue   = Kafka topics (OL datasets)",      rgb(232,244,253)),
    ]
    for i, (lbl, col) in enumerate(legend):
        lx = emu(0.7) + i * emu(6.1)
        add(rect(oid(f"leg_{i}"), lx, emu(9.6), emu(5.8), emu(0.6), col))
        add(textbox(oid(f"leg_t_{i}"), lbl, lx + emu(0.1), emu(9.65), emu(5.6), emu(0.5),
                    font_size=9, color=TEXT))

    # ── Slide 7: Rich Metadata ───────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Rich Topic Metadata via Custom Facets",
               "Topics carry schema, physical config, and live throughput — all queryable in the lineage backend")

    LW = emu(9.5)
    RW = W - LW - emu(1.05)
    TOP7 = emu(3.4)
    CH7 = emu(2.0)
    card("ca", "Schema Facet (SchemaDatasetFacet)",
         "Key and value subjects from Schema Registry are merged into a single field list with key.* / value.* prefixes. Supports AVRO, JSON Schema, and Protobuf.",
         emu(0.7), TOP7, LW, CH7, BLUE)
    card("cb", "KafkaTopicDatasetFacet",
         "Partition count, replication factor, and internal-topic flag fetched from Kafka REST. Mirrors what Confluent Stream Lineage shows on a topic node.",
         emu(0.7), TOP7 + CH7 + emu(0.2), LW, CH7, TEAL)
    card("cc", "KafkaTopicThroughputDatasetFacet",
         "Bytes in/out and records in/out over a configurable lookback window from the Metrics API. Enables data-volume impact analysis.",
         emu(0.7), TOP7 + (CH7 + emu(0.2)) * 2, LW, CH7, GREEN)

    code = (
        "# Dataset facet on ol-orders-enriched\n\n"
        "schema:\n"
        "  fields:\n"
        "    - name: value.order_id   type: string\n"
        "    - name: value.amount     type: double\n"
        "    - name: value.customer   type: string\n"
        "    - name: key.order_id     type: string\n\n"
        "kafkaTopic:\n"
        "  partitions:        6\n"
        "  replicationFactor: 3\n"
        "  isInternal:        false\n\n"
        "kafkaThroughput:\n"
        "  bytesIn:       1_482_304\n"
        "  bytesOut:      3_145_728\n"
        "  recordsIn:     12_400\n"
        "  windowMinutes: 10"
    )
    add(rect(oid("code_bg"), LW + emu(1.05), TOP7, RW, emu(6.2) + emu(0.2)*2, NAVY))
    add(textbox(oid("code"), code,
                LW + emu(1.2), TOP7 + emu(0.2), RW - emu(0.3), emu(6.2) + emu(0.2)*2 - emu(0.4),
                font_size=10, color=WHITE, font="Roboto Mono"))

    # ── Slide 8: Namespace Conventions ───────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Namespace Conventions",
               "Every job and dataset receives a stable, globally unique identifier in the OpenLineage graph")

    table_slide(
        ["Component", "OL Job Namespace", "OL Job Name", "processingType"],
        [
            ["Managed Connector",      "kafka-connect://<env_id>",         "<connector_name>",  "STREAMING"],
            ["Self-Managed Connector", "kafka-connect://<cluster_label>",  "<connector_name>",  "STREAMING"],
            ["Flink Statement",        "flink://<env_id>",                 "<statement_name>",  "STREAMING"],
            ["Consumer Group",         "kafka-consumer-group://<cluster>", "<group_id>",        "STREAMING"],
            ["ksqlDB Query",           "ksqldb://<ksql_cluster_id>",       "<query_id>",        "STREAMING"],
            ["Java SDK Producer",      "kafka-producer://<service_name>",  "<job_name>",        "STREAMING"],
            ["Kafka Topic (Dataset)",  "kafka://<bootstrap_server>",       "<topic_name>",      "—"],
        ],
        [5.0, 6.5, 5.5, 3.5],
        top=3.3,
    )

    add(rect(oid("note_bg"), emu(0.7), emu(12.0), W - emu(1.4), emu(1.5),
             {"red": 0.878, "green": 0.949, "blue": 0.961}))
    add(rect(oid("note_acc"), emu(0.7), emu(12.0), emu(0.12), emu(1.5), TEAL))
    add(textbox(oid("note"),
                "Stable RunIDs are derived deterministically from namespace + name + cycle_key "
                "via SHA-256 — ensuring the same job always maps to the same run across polling cycles, "
                "preventing duplicate lineage nodes.",
                emu(1.0), emu(12.1), W - emu(1.7), emu(1.3),
                font_size=10, color=TEXT))

    # ── Slide 9: Deployment ──────────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Simple Deployment",
               "One config file, three CLI commands — no code changes required")

    config_yaml = (
        "# config.yaml\n\n"
        "confluent:\n"
        "  api_key:    MRYYEBG4MF6FM2BP\n"
        "  api_secret: ***\n"
        "  env_id:     env-m2qxq\n"
        "  cluster_id: lkc-1j6rd3\n\n"
        "  schema_registry:\n"
        "    endpoint:   https://psrc-….confluent.cloud\n"
        "    api_key:    BPYPBFCQOV6A63XQ\n\n"
        "  kafka_rest:\n"
        "    endpoint:   https://pkc-….confluent.cloud\n"
        "    api_key:    JBFEOVMJA543R4MK\n\n"
        "openlineage:\n"
        "  backend_url:     http://localhost:5000\n"
        "  kafka_bootstrap: pkc-pgq85…:9092\n"
        "  interval_sec:    60"
    )
    add(rect(oid("code_bg"), emu(0.7), emu(3.4), emu(10.5), emu(9.8), NAVY))
    add(textbox(oid("code"), config_yaml,
                emu(0.9), emu(3.6), emu(10.1), emu(9.4),
                font_size=10, color=WHITE, font="Roboto Mono"))

    CRPX = emu(11.9)
    CH9  = emu(2.0)
    CW9  = W - CRPX - emu(0.7)
    TOP9 = emu(3.4)
    card("c1", "ol-confluent validate",
         "Checks all credentials and API connectivity before any data is emitted. Fast pre-flight for CI/CD.",
         CRPX, TOP9, CW9, CH9, BLUE)
    card("c2", "ol-confluent run-once",
         "Fetches the full topology snapshot and emits all RunEvents once. Ideal for scheduled jobs or demos.",
         CRPX, TOP9 + CH9 + emu(0.25), CW9, CH9, TEAL)
    card("c3", "ol-confluent run",
         "Continuous polling on the configured interval. Detects added and removed jobs automatically.",
         CRPX, TOP9 + (CH9 + emu(0.25)) * 2, CW9, CH9, GREEN)
    card("c4", "Docker Compose Ready",
         "Marquez backend + Web UI included. One docker compose up -d to start the full stack.",
         CRPX, TOP9 + (CH9 + emu(0.25)) * 3, CW9, CH9, MUTED)

    # ── Slide 10: Results ─────────────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Results at a Glance",
               "What the bridge delivers out of the box today")

    SW10 = (W - emu(1.4)) / 4
    stat_box("s1", "5",    "Lineage Sources",         emu(0.7),            emu(3.4), SW10 - emu(0.2))
    stat_box("s2", "4",    "Job Types Identified",    emu(0.7) + SW10,     emu(3.4), SW10 - emu(0.2))
    stat_box("s3", "3",    "Custom Facets",           emu(0.7) + SW10 * 2, emu(3.4), SW10 - emu(0.2))
    stat_box("s4", "146",  "Tests (all offline)",     emu(0.7) + SW10 * 3, emu(3.4), SW10 - emu(0.2))

    CW10 = (W - emu(1.4)) / 2
    TOP10 = emu(5.5)
    CH10  = emu(2.5)
    card("ca", "Full End-to-End Graph",
         "Connector → Topic → Flink → Topic → Sink rendered as a single navigable lineage graph in Marquez. Depth-4 view shows the complete data flow in one screen.",
         emu(0.7), TOP10, CW10 - emu(0.15), CH10, GREEN)
    card("cb", "Schema Propagation",
         "AVRO / JSON Schema / Protobuf field lists fetched from Schema Registry and attached to every topic dataset — enabling column-level impact analysis.",
         emu(0.7) + CW10, TOP10, CW10 - emu(0.15), CH10, GREEN)
    card("cc", "Stale Lineage Detection",
         "Deleted or stopped jobs emit ABORT events automatically. The lineage graph stays current without manual intervention.",
         emu(0.7), TOP10 + CH10 + emu(0.2), CW10 - emu(0.15), CH10, BLUE)
    card("cd", "Java SDK Integration",
         "The included OrderProducer demo shows how any Java Kafka client can self-instrument START / COMPLETE events using the OpenLineage Java SDK.",
         emu(0.7) + CW10, TOP10 + CH10 + emu(0.2), CW10 - emu(0.15), CH10, BLUE)

    # ── Slide 11: Roadmap ─────────────────────────────────────────────────────
    new_slide(bg=SLATE)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    header_bar("Roadmap & Next Steps",
               "Prioritized extensions to expand coverage and ecosystem integration")

    items = [
        ("Flink SQL UDTF Scanner",
         "Extend the SQL parser to extract field-level lineage from Flink UDTFs and lateral table functions — enabling column-level impact graphs.",
         BLUE),
        ("Producer Auto-Discovery",
         "Correlate Kafka client metrics with application metadata to automatically identify individual producers without SDK instrumentation.",
         BLUE),
        ("DataHub & Apache Atlas",
         "The bridge emits standard OpenLineage — swap Marquez for DataHub or Apache Atlas by changing a single backend URL in config.",
         TEAL),
        ("Helm Chart / Operator",
         "Package the bridge as a Kubernetes Helm chart for one-command deployment alongside existing Confluent Platform or CFK installs.",
         TEAL),
        ("Change Notifications",
         "Emit Slack / PagerDuty alerts when a high-value topic loses its producer or when consumer lag exceeds a threshold.",
         GREEN),
        ("Multi-Environment Support",
         "Single bridge instance polling multiple Confluent environments and clusters — unified lineage view across dev, staging, and production.",
         GREEN),
    ]
    CW11 = (W - emu(1.4)) / 3
    CH11 = emu(2.8)
    TOP11 = emu(3.4)
    for i, (title, body, acc) in enumerate(items):
        col = i % 3
        row = i // 3
        cx = emu(0.7) + col * (CW11)
        cy = TOP11 + row * (CH11 + emu(0.2))
        card(f"r_{i}", title, body, cx, cy, CW11 - emu(0.15), CH11, acc)

    # ── Slide 12: Thank You ───────────────────────────────────────────────────
    new_slide(bg=NAVY)
    add(rect(oid("accent"), 0, 0, W, emu(0.18), TEAL))
    add(textbox(oid("icon"), "🔗",
                W // 2 - emu(1), emu(1.5), emu(2), emu(1.5),
                font_size=48, align="CENTER"))
    add(textbox(oid("title"), "Questions?",
                emu(2), emu(3.0), W - emu(4), emu(2.0),
                font_size=48, bold=True, color=WHITE, align="CENTER"))
    add(textbox(oid("sub"),
                "OpenLineage Bridge for Confluent Cloud — complete end-to-end data lineage\n"
                "across every component of your streaming data platform.",
                emu(2), emu(5.0), W - emu(4), emu(1.5),
                font_size=16, color=MUTED, align="CENTER"))
    add(textbox(oid("links"),
                "openlineage.io   ·   confluent.io/product/flink   ·   marquezproject.ai",
                emu(2), emu(6.7), W - emu(4), emu(0.8),
                font_size=13, color=TEAL, align="CENTER"))


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    creds, _ = default(scopes=SCOPES)
    creds.refresh(Request())

    slides_svc = build("slides", "v1", credentials=creds)

    print("Creating presentation…")
    pres = slides_svc.presentations().create(
        body={"title": "OpenLineage Bridge for Confluent Cloud"}
    ).execute()
    pres_id = pres["presentationId"]
    default_slide_id = pres["slides"][0]["objectId"]
    print(f"  Presentation ID: {pres_id}")

    build_slides()

    # Delete the default blank slide after our slides are created
    _all_requests.append({"deleteObject": {"objectId": default_slide_id}})

    print(f"Sending {len(_all_requests)} API requests across {len(_slide_ids)} slides…")
    slides_svc.presentations().batchUpdate(
        presentationId=pres_id,
        body={"requests": _all_requests},
    ).execute()

    url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
    print(f"\nDone! Open your deck:\n  {url}")
    return url


if __name__ == "__main__":
    main()
