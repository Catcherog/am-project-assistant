#!/usr/bin/env python3
"""Generate a REVIEWABLE local bridge candidate and unified diff.

Never edits the input, service, launch configuration or a running process.
Supports the exact observed reference bridge anchors; changed anchors need local merge.
"""
import argparse
import difflib
import hashlib
import json
from pathlib import Path


def patched(source):
    if "from integration.r2_runtime import install" in source:
        raise ValueError("R2 hook already present; review the existing binding")
    if "from integration.am_grok_host import install" in source:
        raise ValueError("R1 hook present; disable/drain it and merge the R2 replacement locally")
    def replace_once(text,old,new):
        if text.count(old)!=1:
            raise ValueError("bridge anchor changed; merge this hook into the actual source")
        return text.replace(old,new,1)
    source=replace_once(source,"    def on_message(event):",
        "    from integration.r2_runtime import install\n"
        "    bridge.multi_ai, bridge.multi_ai_scheduler = install(bridge, config, client, root)\n\n"
        "    def on_message(event):")
    source=replace_once(source,"            diagnostic['accepted'] = bridge.ingest(payload)",
        "            multi = bridge.multi_ai.ingest(payload) if bridge.multi_ai is not None else None\n"
        "            diagnostic['accepted'] = multi.captured if multi is not None and multi.claimed else bridge.ingest(payload)")
    source=replace_once(source,
        "        accepted = False\n        if bridge.am_self is not None:",
        "        accepted = False\n"
        "        if bridge.multi_ai is not None:\n"
        "            r2_response = bridge.multi_ai.card_callback(json.loads(lark.JSON.marshal(event)))\n"
        "            if r2_response is not None:\n"
        "                return P2CardActionTriggerResponse(r2_response)\n"
        "        if bridge.am_self is not None:")
    source=replace_once(source,"    def on_recalled(event):\n",
        "    def on_recalled(event):\n"
        "        if bridge.multi_ai is not None:\n"
        "            bridge.multi_ai.recall(json.loads(lark.JSON.marshal(event)))\n")
    source=replace_once(source,"        bridge.stop.set()\n",
        "        bridge.stop.set()\n"
        "        r2_stopped = True\n"
        "        if bridge.multi_ai_scheduler is not None:\n"
        "            r2_stop = bridge.multi_ai_scheduler.stop()\n"
        "            r2_stopped = r2_stop['stopped']\n")
    source=replace_once(source,
        "        information_stopped = bridge.information is None or bridge.information.close()",
        "        information_stopped = bridge.information is None or bridge.information.close()\n"
        "        information_stopped = r2_stopped and information_stopped")
    compile(source,"bridge.r2.candidate.py","exec")
    return source


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--input",type=Path,required=True)
    p.add_argument("--output-dir",type=Path,required=True)
    args=p.parse_args()
    original=args.input.read_text()
    candidate=patched(original)
    args.output_dir.mkdir(parents=True,exist_ok=True)
    candidate_path=args.output_dir/"bridge.r2.candidate.py"
    if candidate_path.resolve()==args.input.resolve():
        raise ValueError("input and output must differ")
    candidate_path.write_text(candidate)
    delta="".join(difflib.unified_diff(original.splitlines(True),candidate.splitlines(True),
                                     fromfile="a/bridge.py",tofile="b/bridge.py"))
    (args.output_dir/"r2_bridge.patch").write_text(delta)
    result={"source_sha256":hashlib.sha256(original.encode()).hexdigest(),
            "candidate_sha256":hashlib.sha256(candidate.encode()).hexdigest(),
            "patch_sha256":hashlib.sha256(delta.encode()).hexdigest(),
            "compiled":True,"input_modified":False,"installed":False}
    (args.output_dir/"patch_evidence.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result))


if __name__=="__main__": main()
