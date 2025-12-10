from pathlib import Path

p = Path("vasco/cli_pipeline.py")
text = p.read_text(encoding="utf-8")
lines = text.splitlines()


def indent(s: str) -> int:
    return len(s) - len(s.lstrip(" "))


def is_try_line(s: str) -> bool:
    return s.lstrip().startswith("try:")


changed = False
i = 0
while i < len(lines):
    line = lines[i]
    if is_try_line(line):
        base = indent(line)
        # find first non-empty line after try
        j = i + 1
        while j < len(lines) and lines[j].strip() == "":
            j += 1
        # determine if there is a body (indent > base)
        has_body = j < len(lines) and indent(lines[j]) > base
        if not has_body:
            # insert a pass line as body under try:
            lines.insert(j, " " * (base + 4) + "pass")
            changed = True
            j += 1
        # now scan forward to see if there is except/finally at same base indent
        k = j
        handler_found = False
        while k < len(lines):
            cur = lines[k]
            cur_strip = cur.strip()
            cur_ind = indent(cur)
            if (
                cur_strip.startswith(("except ", "except:", "finally:"))
                and cur_ind == base
            ):
                handler_found = True
                break
            # block ends when dedent to <= base and line is not blank
            if cur_strip and cur_ind <= base and not cur_strip.startswith("#"):
                break
            k += 1
        if not handler_found:
            # insert generic handler at position k
            lines.insert(k, " " * base + "except Exception:")
            lines.insert(k + 1, " " * (base + 4) + "pass")
            changed = True
            i = k + 2
        else:
            i = k + 1
    else:
        i += 1

if changed:
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("[OK] Repaired dangling try blocks in vasco/cli_pipeline.py")
else:
    print("[INFO] No repairs needed (no dangling try detected)")

# Syntax check
import py_compile

py_compile.compile("vasco/cli_pipeline.py", doraise=True)
print("[OK] Syntax validated.")
