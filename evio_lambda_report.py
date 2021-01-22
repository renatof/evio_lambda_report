import json
import fileinput

if __name__ == "__main__":

  overlay_number = 1
  node_number = 1
  overlay_id_to_number_dict = {}
  overlay_number_to_peer_list_dict = {}
  unique_nodeid_overlayid_to_number_dict = {}
  rawline_number = 0

  # loop to read each line from stdin
  for rawline in fileinput.input():

    # read a line
    rawline.rstrip()
    rawline_number = rawline_number+1
    # print ("rawline: " + rawline)

    # parse the json
    rawline_parsed = json.loads(rawline)
    # for key, value in rawline_parsed.items():
    #   print(key, ' : ', value)
    
    # extract timestamp from MyKey
    mykey_dict = rawline_parsed['MyKey']
    sample_timestamp = mykey_dict['s']
    # print ("sample_timestamp: " + sample_timestamp)

    # Extract rest that is keyed under Body
    body_dict = rawline_parsed['Body']
    sample_body = body_dict['s']
    # print ("sample_body: " + sample_body)

    # parse the body json
    sample_body_parsed = json.loads(sample_body)
    # for key, value in sample_body_parsed.items():
    #   print(key, ' : ', value)

    # extract evio version, reportid, and nodeid for this sample
    version = sample_body_parsed['Version']
    reportid = sample_body_parsed['ReportId']
    nodeid = sample_body_parsed['NodeId']
    # print ("Version: " + version)
    # print ("ReportId: " + str(reportid))
    # print ("NodeId: " + nodeid)

    # extract overlayid and peer list
    for key, value in sample_body_parsed.items():
      myset = {'Version', 'ReportId', 'NodeId'}
      if key not in myset:
        overlayid = key
        peer_list = value
        # print("OverlayID: " + overlayid)
        # print("Peer list: " + str(peer_list))

    # build set of known peers in this overlay as the union of nodeid and peer_list
    peer_list.append(nodeid)
    # print ("peer_list: " + str(peer_list))

    # for each node in peer set, assign a unique nodeid_overlayid: nodeid:overlayid
    # then, if not seen before, add to unique_nodeid_overlayid_to_number_dict
    sample_node_number_set = list({})
    for peer_entry in peer_list:
      # derive unique id
      unique_nodeid_overlayid = peer_entry + ':' + overlayid

      # check if this node has ever been seen before
      if unique_nodeid_overlayid not in unique_nodeid_overlayid_to_number_dict:
        unique_nodeid_overlayid_to_number_dict[unique_nodeid_overlayid] = node_number
        # print ("Assigning new node number for " + unique_nodeid_overlayid + " = " + str(node_number))
        node_number = node_number + 1

      # this builds the set of node numbers seen in this line
      sample_node_number_set.append(unique_nodeid_overlayid_to_number_dict[unique_nodeid_overlayid])

    # print ("sample_node_number_set: " + str(sample_node_number_set))

    # now each node has a unique node_number
    # scan all overlays to check if any node has been assigned to any overlay
    found_overlay_number = 0
    for scan_node_number in sample_node_number_set:
      for scan_overlay_number , scan_peer_list in overlay_number_to_peer_list_dict.items():
        # skip this if already found an overlay
        if found_overlay_number == 0:
          if scan_node_number in scan_peer_list:
            found_overlay_number = scan_overlay_number

    # if no nodes found in any overlays, create new overlay and add set of all nodes in this line
    if found_overlay_number == 0:
      # print ("No overlay found, creating overlay_number: " + str(overlay_number))
      overlay_number_to_peer_list_dict[overlay_number] = sample_node_number_set
      overlay_number = overlay_number + 1
    # else, union with existing set
    else:
      for i in sample_node_number_set:
        if i not in overlay_number_to_peer_list_dict[found_overlay_number]:
          overlay_number_to_peer_list_dict[found_overlay_number].append(i)
      # print ("found_overlay_number: " + str(found_overlay_number))
    #   print ("Before union: " + str(overlay_number_to_peer_list_dict[found_overlay_number]))
    #  overlay_number_to_peer_list_dict[found_overlay_number].update(sample_node_number_set)
    #   print ("Union result: " + str(overlay_number_to_peer_list_dict[found_overlay_number]))

    pass

  print ("Nodes:")
  for i in range(1,node_number):
    for key, value in unique_nodeid_overlayid_to_number_dict.items():
      if value == i:
        print(key, ' : ', value)

  print ("Overlays found:")
  for key, value in overlay_number_to_peer_list_dict.items():
    print(key, ' : ', value)

  print ("Sanity check:")
  nodes = 0
  matches = 0
  for nkey, nvalue in unique_nodeid_overlayid_to_number_dict.items():
    nodes = nodes + 1
    for okey, ovalue in overlay_number_to_peer_list_dict.items():
      if nvalue in ovalue:
        matches = matches+1
  print ("Nodes len: " + str(nodes) + " matches: " + str(matches))


  print ("Merging:")
  for okey, ovalue in overlay_number_to_peer_list_dict.items():
    for i in ovalue:
      for ookey, oovalue in overlay_number_to_peer_list_dict.items():
        if okey != ookey:
          if i in oovalue:
            print ("merge " + str(okey) + " and " + str(ookey) + " because of " + str(i))
            for j in oovalue:
              if j not in overlay_number_to_peer_list_dict[okey]:
                overlay_number_to_peer_list_dict[okey].append(j)
            overlay_number_to_peer_list_dict[ookey] = []

  print ("Overlays merged:")
  matches = 0
  for key, value in overlay_number_to_peer_list_dict.items():
    if value: 
      print(key, ' : ', value)
      matches = matches + len(value)
  print ("Nodes len: " + str(nodes) + " matches: " + str(matches))


      


