#!/usr/bin/env python3
"""
Apply ANODE weights: upload weight XML to HDFS, update hdfs-site.xml inline property,
and trigger NameNode reconfig.

Usage:
  python anode_agent_apply.py --weights /tmp/WordCount.xml --hdfs-dest /user/hadoop/anode/weights/WordCount.xml --hdfs-site /etc/hadoop/conf/hdfs-site.xml
"""

import argparse
import subprocess
import xml.etree.ElementTree as ET
import tempfile
import os

def hdfs_put(local, hdfs_path):
    cmd = ["hdfs", "dfs", "-put", "-f", local, hdfs_path]
    subprocess.check_call(cmd)

def update_hdfs_site_property(hdfs_site_path, prop_name, prop_value):
    tree = ET.parse(hdfs_site_path)
    root = tree.getroot()
    # find if property exists
    for prop in root.findall('property'):
        name = prop.find('name')
        if name is not None and name.text == prop_name:
            val = prop.find('value')
            if val is None:
                val = ET.SubElement(prop, 'value')
            val.text = prop_value
            tree.write(hdfs_site_path, xml_declaration=True, encoding='utf-8')
            return
    # else add
    p = ET.SubElement(root, 'property')
    n = ET.SubElement(p, 'name'); n.text = prop_name
    v = ET.SubElement(p, 'value'); v.text = prop_value
    tree.write(hdfs_site_path, xml_declaration=True, encoding='utf-8')

def run_reconfig():
    # Trigger dynamic reconfig on NameNode (requires proper Hadoop version support)
    cmd = ["hdfs", "dfsadmin", "-reconfig"]
    subprocess.check_call(cmd)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--weights', required=True, help='Local XML weight file produced by parser')
    parser.add_argument('--hdfs-dest', required=True, help='Destination HDFS path for weights e.g. /user/hadoop/anode/weights/Workload.xml')
    parser.add_argument('--hdfs-site', default='/etc/hadoop/conf/hdfs-site.xml')
    args = parser.parse_args()

    # 1) push weights to HDFS
    hdfs_put(args.weights, args.hdfs_dest)

    # 2) update hdfs-site.xml dfs.anode.weights.file property to point to new file
    update_hdfs_site_property(args.hdfs_site, 'dfs.anode.weights.file', args.hdfs_dest)

    # 3) Optionally update inline weights property by reading the weight xml and composing datanode:weight pairs
    # read the weight xml:
    tree = ET.parse(args.weights)
    root = tree.getroot()
    pairs = []
    for node in root.findall('node'):
        name = node.get('name')
        weight = node.get('weight')
        if name and weight:
            # sanitize hostname if needed
            pairs.append(f"{name}:{weight}")
    inline_val = ",".join(pairs)
    update_hdfs_site_property(args.hdfs_site, 'dfs.datanode.data.dir.weight', inline_val)

    # 4) run reconfig so NameNode loads new properties (if supported)
    try:
        run_reconfig()
        print("Reconfig triggered.")
    except subprocess.CalledProcessError as e:
        print("Reconfig failed; you may need to restart NameNode. Error:", e)

if __name__ == "__main__":
    main()
