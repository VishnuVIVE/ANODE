#!/usr/bin/env python3
"""
Parse Hadoop JobHistory files (XML/JSON) under HDFS path /user/hadoop/jobhistory,
compute NDT and Ci for each datanode, and write an XML weight file per workload.

Usage:
  python parse_jobhistory_and_compute_weights.py --hdfs-path /user/hadoop/jobhistory --workload WordCount --out /tmp/WordCount.xml
"""

import argparse
import xml.etree.ElementTree as ET
import subprocess
import json
import tempfile
import os
from collections import defaultdict

def hdfs_ls(path):
    cmd = ["hdfs", "dfs", "-ls", path]
    out = subprocess.check_output(cmd, universal_newlines=True)
    files = []
    for line in out.splitlines():
        parts = line.strip().split()
        if len(parts) >= 8:
            files.append(parts[-1])
    return files

def hdfs_cat(path):
    cmd = ["hdfs", "dfs", "-cat", path]
    return subprocess.check_output(cmd, universal_newlines=True)

def parse_history_xml(content):
    # This parser assumes the jobhistory xml format with <taskAttempt> or <map> elements including
    # startTime, finishTime, hdfsBytesRead and trackerName (node).
    # Real JobHistory formats differ by Hadoop version — adapt fields accordingly.
    root = ET.fromstring(content)
    results = []
    # Try to find all taskAttempt nodes
    for t in root.findall(".//taskAttempt"):
        try:
            start = int(t.findtext('startTime') or t.findtext('start'))
            finish = int(t.findtext('finishTime') or t.findtext('finish'))
            hdfs_bytes = int(t.findtext('hdfsBytesRead') or t.findtext('hdfs_bytes_read') or 0)
            tracker = t.findtext('trackerName') or t.findtext('node') or 'unknown'
            results.append((tracker, start, finish, hdfs_bytes))
        except Exception:
            continue
    # fallback: search for map/reduce elements
    if not results:
        for m in root.findall(".//map"):
            try:
                start = int(m.findtext('startTime') or 0)
                finish = int(m.findtext('finishTime') or 0)
                hdfs_bytes = int(m.findtext('hdfsBytesRead') or 0)
                tracker = m.findtext('trackerName') or 'unknown'
                results.append((tracker, start, finish, hdfs_bytes))
            except Exception:
                continue
    return results

def compute_ndt_and_Ci(entries):
    # entries: list of tuples (node, start, finish, hdfs_bytes)
    ndt_sum = defaultdict(float)
    ndt_count = defaultdict(int)
    for node, start, finish, hdfs_bytes in entries:
        if hdfs_bytes <= 0 or finish <= start:
            continue
        texec = (finish - start) / 1000.0  # convert ms -> seconds if logs are in ms
        ndt = texec / float(hdfs_bytes)
        ndt_sum[node] += ndt
        ndt_count[node] += 1
    node_avg_ndt = {}
    for node in ndt_sum:
        node_avg_ndt[node] = ndt_sum[node] / ndt_count[node]
    # Ci = 1 / avg(NDT)
    node_C = {}
    for node, avg_ndt in node_avg_ndt.items():
        if avg_ndt > 0:
            node_C[node] = 1.0 / avg_ndt
        else:
            node_C[node] = 0.0
    # Normalize Pi = Ci / sum(C)
    totalC = sum(node_C.values())
    node_P = {}
    if totalC > 0:
        for node, c in node_C.items():
            node_P[node] = c / totalC
    else:
        # fallback equal weights
        for node in node_C:
            node_P[node] = 1.0 / len(node_C) if len(node_C) else 0.0
    return node_C, node_P

def write_weights_xml(outpath, workload, node_C, node_P):
    root = ET.Element('anodeWeights', attrib={'workload': workload})
    for node, weight in node_P.items():
        e = ET.SubElement(root, 'node', attrib={'name': node, 'weight': str(round(weight,6)), 'capacity': str(node_C.get(node,0.0))})
    tree = ET.ElementTree(root)
    tree.write(outpath, encoding='utf-8', xml_declaration=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hdfs-path', default='/user/hadoop/jobhistory')
    parser.add_argument('--workload', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()

    # list jobhistory files
    files = hdfs_ls(args.hdfs_path)
    # naive: pick latest files that contain workload name
    matched = [f for f in files if args.workload.lower() in f.lower()]
    if not matched:
        print("No matching jobhistory files for workload:", args.workload)
        return

    entries = []
    for f in matched:
        try:
            content = hdfs_cat(f)
            # try parse xml/ json
            if content.strip().startswith('<'):
                parsed = parse_history_xml(content)
                entries.extend(parsed)
            else:
                # Try JSON list of tasks
                try:
                    data = json.loads(content)
                    # implement JSON extraction as needed
                except Exception:
                    pass
        except Exception as e:
            print("Failed to read", f, e)

    node_C, node_P = compute_ndt_and_Ci(entries)
    write_weights_xml(args.out, args.workload, node_C, node_P)
    print("Wrote weights to", args.out)

if __name__ == "__main__":
    main()
