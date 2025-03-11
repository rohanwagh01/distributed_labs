#!/usr/bin/env python
import sys
from typing import Optional

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console

TOPICS = {
    "TIMR": "#9a9a99",
    "VOTE": "#67a0b2",
    "LEAD": "#d0b343",
    "TERM": "#70c43f",
    "LOG1": "#4878bc",
    "LOG2": "#398280",
    "CMIT": "#98719f",
    "PERS": "#d08341",
    "SNAP": "#FD971F",
    "DROP": "#ff615c",
    "CLNT": "#00813c",
    "TEST": "#fe2c79",
    "INFO": "#ffffff",
    "WARN": "#d08341",
    "ERRO": "#fe2626",
    "TRCE": "#fe2626",
}
# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics


def generate_html_style():
    styles = """
    <style>
        body { 
            background-color: #1e1e1e; 
            color: #ffffff; 
            font-family: monospace;
            padding: 20px;
        }
        .log-container {
            display: grid;
            grid-template-columns: repeat(var(--n-columns), 1fr);
            gap: 0;
            border: 1px dotted #444;
        }
        .log-entry {
            padding: 8px;
            margin: 0;
            white-space: pre-wrap;
            border-bottom: 1px dotted #444;
            border-right: 1px dotted #444;
            min-height: 1.2em;
        }
        .log-entry:last-child {
            border-right: none;
        }
        .log-container:last-child .log-entry {
            border-bottom: none;
        }
        .timestamp {
            color: #888888;
        }
        /* Single column log entries */
        .log-entry:only-child {
            border: none;
            border-bottom: 1px dotted #444;
        }
        .log-entry:only-child:last-child {
            border-bottom: none;
        }
    </style>
    """
    return styles


def main(
    file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    colorize: bool = typer.Option(True, "--no-color"),
    n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
    ignore: Optional[str] = typer.Option(None, "--ignore", "-i", callback=list_topics),
    just: Optional[str] = typer.Option(None, "--just", "-j", callback=list_topics),
    html_output: Optional[str] = typer.Option(None, "--html", "-h", help="Output to HTML file"),
):
    topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        try:
            time, topic, *msg = line.strip().split(" ")
            # To ignore some topics
            if topic not in topics:
                continue

            msg = " ".join(msg)

            # Debug calls from the test suite aren't associated with
            # any particular peer. Otherwise we can treat second column
            # as peer id
            if topic != "TEST":
                i = int(msg[1])

            # Colorize output by using rich syntax when needed
            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg_colored = f"[{color}]{msg}[/{color}]"
            else:
                msg_colored = msg

            # Only print to terminal if not outputting to HTML
            if html_output is None:
                # Single column printing. Always the case for debug stmts in tests
                if n_columns is None or topic == "TEST":
                    print(time, msg_colored)
                # Multi column printing, timing is dropped to maximize horizontal
                # space. Heavylifting is done through rich.column.Columns object
                else:
                    cols = ["" for _ in range(n_columns)]
                    msg_colored = "" + msg_colored
                    cols[i] = msg_colored
                    col_width = int(width / n_columns)
                    cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                    print(cols)

            if html_output is not None:
                # Collect all entries for HTML generation
                if not hasattr(main, 'html_entries'):
                    main.html_entries = []
                
                if n_columns is None or topic == "TEST":
                    entry = f'<div class="log-entry"><span class="timestamp">{time}</span> <span style="color: {TOPICS.get(topic, "#ffffff")}">{msg}</span></div>'
                    main.html_entries.append(entry)
                else:
                    # Create a new row for each log entry
                    cols = [""] * n_columns
                    cols[i] = f'<span style="color: {TOPICS.get(topic, "#ffffff")}">{msg}</span>'
                    entry = '<div class="log-container">' + ''.join(f'<div class="log-entry">{cell}</div>' for cell in cols) + '</div>'
                    main.html_entries.append(entry)
        except:
            # Code from tests or panics does not follow format
            # Print test results regardless of output mode
            if line.strip().startswith("Test:") or "PASS" in line or "FAIL" in line or line.startswith("panic"):
                if not panic:
                    print("#" * console.width)
                print(line, end="")

            elif html_output is None:
                print(line, end="")

            if html_output is not None and hasattr(main, 'html_entries'):
                main.html_entries.append(f'<div class="log-entry">{line}</div>')
            panic = True

    # Generate HTML file if requested
    if html_output is not None and hasattr(main, 'html_entries'):
        with open(html_output, 'w') as f:
            f.write('<!DOCTYPE html>\n<html>\n<head>\n')
            f.write(generate_html_style())
            if n_columns is not None:
                f.write(f'<style>:root {{ --n-columns: {n_columns}; }}</style>\n')
            f.write('</head>\n<body>\n')
            f.write('\n'.join(main.html_entries))
            f.write('\n</body>\n</html>')


if __name__ == "__main__":
    typer.run(main)
