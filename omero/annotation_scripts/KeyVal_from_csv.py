# coding=utf-8
"""
 KeyVal_from_csv.py

 Adds key-value pairs to a target object on OMERO from a CSV file.

-----------------------------------------------------------------------------
  Copyright (C) 2018
  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
------------------------------------------------------------------------------
Created by Christian Evenhuis

"""

import omero
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong, robject
import omero.scripts as scripts
from omero.constants.metadata import NSCLIENTMAPANNOTATION
from omero.util.populate_roi import DownloadingOriginalFileProvider

import csv


CHILD_OBJECTS = {
    "Project": "Dataset",
    "Dataset": "Image",
    "Screen": "Plate",
    "Plate": "Well",
    "Well": "WellSample",
    "WellSample": "Image"
}

ALLOWED_PARAM = {
    "Project": ["Project", "Dataset", "Image"],
    "Dataset": ["Dataset", "Image"],
    "Image": ["Image"],
    "Screen": ["Screen", "Plate", "Well", "Run", "Image"],
    "Plate": ["Plate", "Well", "Run", "Image"],
    "Well": ["Well", "Image"],
    "Run": ["Run", "Image"],
    "Tag": ["Project", "Dataset", "Image",
            "Screen", "Plate", "Well", "Run"]
}


def get_obj_name(omero_obj):
    if omero_obj.OMERO_CLASS == "Well":
        return omero_obj.getWellPos()
    else:
        return omero_obj.getName()


def get_original_file(omero_obj):
    """Find last AnnotationFile linked to object"""
    file_ann = None
    for ann in omero_obj.listAnnotations():
        if ann.OMERO_TYPE == omero.model.FileAnnotationI:
            file_name = ann.getFile().getName()
            # Pick file by Ann ID (or name if ID is None)
            if file_name.endswith(".csv") or file_name.endswith(".tsv"):
                if (file_ann is None) or (ann.getDate() > file_ann.getDate()):
                    # Get the most recent file
                    file_ann = ann

    assert file_ann is not None, f"No .csv FileAnnotation was found on \
        {omero_obj.OMERO_CLASS}:{get_obj_name(omero_obj)}:{omero_obj.getId()}"

    return file_ann


def link_file_ann(conn, object_type, object_, file_ann):
    """Link File Annotation to the Object, if not already linked."""
    # Check for existing links
    if object_type == "TagAnnotation":
        print("CSV file cannot be attached to the parent tag")
        return
    links = list(conn.getAnnotationLinks(
        object_type, parent_ids=[object_.getId()],
        ann_ids=[file_ann.getId()]
        ))
    if len(links) == 0:
        object_.linkAnnotation(file_ann)


def read_csv(conn, original_file, delimiter):
    """ Dedicated function to read the CSV file """
    print("Using FileAnnotation",
          f"{original_file.id.val}:{original_file.name.val}")
    provider = DownloadingOriginalFileProvider(conn)
    # read the csv
    temp_file = provider.get_original_file_data(original_file)
    # Needs omero-py 5.9.1 or later
    with open(temp_file.name, 'rt', encoding='utf-8-sig') as file_handle:
        if delimiter is None:
            try:  # Detecting csv delimiter from the first line
                delimiter = csv.Sniffer().sniff(
                    file_handle.readline(), ",;\t").delimiter
                print(f"Using delimiter {delimiter}",
                      "after reading one line")
            except Exception:
                # Send the error back to the UI
                assert False, ("Failed to sniff CSV delimiter, " +
                               "please specify the separator")

        # reset to start and read whole file...
        file_handle.seek(0)
        rows = list(csv.reader(file_handle, delimiter=delimiter))

    rowlen = len(rows[0])
    error_msg = (
        "CSV rows lenght mismatch: Header has {} " +
        "items, while line {} has {}"
    )
    for i in range(1, len(rows)):
        assert len(rows[i]) == rowlen, error_msg.format(
            rowlen, i, len(rows[i])
        )

    # keys are in the header row (first row for no namespaces
    # second row with namespaces declared)
    namespaces = []
    if rows[0][0].lower() == "namespace":
        namespaces = [el.strip() for el in rows[0]]
        namespaces = [ns if ns else NSCLIENTMAPANNOTATION for ns in namespaces]
        rows = rows[1:]
    header = [el.strip() for el in rows[0]]
    rows = rows[1:]

    print(f"Header: {header}\n")
    return rows, header, namespaces


def get_children_recursive(source_object, target_type):
    if CHILD_OBJECTS[source_object.OMERO_CLASS] == target_type:
        # Stop condition, we return the source_obj children
        if source_object.OMERO_CLASS != "WellSample":
            return source_object.listChildren()
        else:
            return [source_object.getImage()]
    else:  # Not yet the target
        result = []
        for child_obj in source_object.listChildren():
            # Going down in the Hierarchy list
            result.extend(get_children_recursive(child_obj, target_type))
        return result


def target_iterator(conn, source_object, target_type, is_tag):
    if target_type == source_object.OMERO_CLASS:
        target_obj_l = [source_object]
    elif source_object.OMERO_CLASS == "PlateAcquisition":
        # Check if there is more than one Run, otherwise
        # it's equivalent to start from a plate (and faster this way)
        plate_o = source_object.getParent()
        wellsamp_l = get_children_recursive(plate_o, "WellSample")
        if len(list(plate_o.listPlateAcquisitions())) > 1:
            # Only case where we need to filter on PlateAcquisition
            run_id = source_object.getId()
            wellsamp_l = filter(lambda x: x._obj.plateAcquisition._id._val
                                == run_id, wellsamp_l)
        target_obj_l = [wellsamp.getImage() for wellsamp in wellsamp_l]
    elif target_type == "PlateAcquisition":
        # No direct children access from a plate
        if source_object.OMERO_CLASS == "Screen":
            plate_l = get_children_recursive(source_object, "Plate")
        elif source_object.OMERO_CLASS == "Plate":
            plate_l = [source_object]
        target_obj_l = [r for p in plate_l for r in p.listPlateAcquisitions()]
    elif is_tag:
        target_obj_l = conn.getObjectsByAnnotations(target_type,
                                                    [source_object.getId()])
        # Need that to load objects
        obj_ids = [o.getId() for o in target_obj_l]
        target_obj_l = list(conn.getObjects(target_type, obj_ids))
    else:
        target_obj_l = get_children_recursive(source_object,
                                              target_type)

    print(f"Iterating objects from {source_object}:")
    for target_obj in target_obj_l:
        print(f"\t- {target_obj}")
        yield target_obj
    print()


def keyval_from_csv(conn, script_params):
    source_type = script_params["Data_Type"]
    target_type = script_params["Target Data_Type"]
    source_ids = script_params["IDs"]
    file_ids = script_params["File_Annotation"]
    namespace = script_params["Namespace (leave blank for default)"]
    to_exclude = script_params["Columns to exclude"]
    target_id_colname = script_params["Target ID colname"]
    target_name_colname = script_params["Target name colname"]
    separator = script_params["Separator"]
    attach_file = script_params["Attach csv to parents"]
    exclude_empty_value = script_params["Exclude empty values"]
    split_on = script_params["Split value on"]

    ntarget_processed = 0
    ntarget_updated = 0
    missing_names = 0

    result_obj = None

    # One file output per given ID
    source_objects = conn.getObjects(source_type, source_ids)
    for source_object, file_ann_id in zip(source_objects, file_ids):
        if file_ann_id is not None:
            file_ann = conn.getObject("Annotation", oid=file_ann_id)
            assert file_ann is not None, f"Annotation {file_ann_id} not found"
            assert file_ann.OMERO_TYPE == omero.model.FileAnnotationI, "The \
                    provided annotation ID must reference a FileAnnotation, \
                    not a {file_ann.OMERO_TYPE}"
        else:
            file_ann = get_original_file(source_object)
        original_file = file_ann.getFile()._obj

        rows, header, namespaces = read_csv(conn, original_file, separator)
        if len(namespaces) == 0:
            namespaces = [namespace] * len(header)

        is_tag = source_type == "TagAnnotation"
        target_obj_l = target_iterator(conn, source_object,
                                       target_type, is_tag)

        # Index of the column used to identify the targets. Try for IDs first
        idx_id, idx_name = -1, -1
        if target_id_colname in header:
            idx_id = header.index(target_id_colname)
        if target_name_colname in header:
            idx_name = header.index(target_name_colname)
        cols_to_ignore = [header.index(el) for el in to_exclude
                          if el in header]

        assert (idx_id != -1) or (idx_name != -1), "Neither \
            the column for the objects name or the objects index were found"

        use_id = idx_id != -1  # use the obj_idx column if exist
        if not use_id:
            # Identify images by name fail if two images have identical names
            idx_id = idx_name
            target_d = dict()
            for target_obj in target_obj_l:
                name = get_obj_name(target_obj)
                if target_type == "Well":
                    name = name.upper()
                assert name not in target_d.keys(), f"Target objects \
                    identified by name have duplicate: {name}"
                target_d[name] = target_obj
        else:
            # Setting the dictionnary target_id:target_obj
            # keys as string to match CSV reader output
            target_d = {str(target_obj.getId()): target_obj
                        for target_obj in target_obj_l}
        ntarget_processed += len(target_d)

        ok_idxs = [i for i in range(len(header)) if i not in cols_to_ignore]
        for row in rows:
            # Iterate the CSV rows and search for the matching target
            target_id = row[idx_id]
            if target_id in target_d.keys():
                target_obj = target_d[target_id]
            else:
                missing_names += 1
                print(f"Not found: {target_id}")
                continue

            if split_on != "":
                parsed_row, parsed_ns, parsed_head = [], [], []
                for i in ok_idxs:
                    curr_vals = row[i].strip().split(split_on)
                    parsed_row.extend(curr_vals)
                    parsed_ns.extend([namespaces[i]] * len(curr_vals))
                    parsed_head.extend([header[i]] * len(curr_vals))
            else:
                parsed_row = [row[i] for i in ok_idxs]
                parsed_ns = [namespaces[i] for i in ok_idxs]
                parsed_head = [header[i] for i in ok_idxs]

            updated = annotate_object(
                conn, target_obj, parsed_row, parsed_head, parsed_ns,
                cols_to_ignore, exclude_empty_value
            )

            if updated:
                if result_obj is None:
                    result_obj = target_obj
                ntarget_updated += 1

        if ntarget_updated > 0 and attach_file:
            # Only attaching if this is successful
            link_file_ann(conn, source_type, source_object, file_ann)
        print("\n------------------------------------\n")

    message = f"Added KV-pairs to \
        {ntarget_updated}/{ntarget_processed} {target_type}"
    if missing_names > 0:
        message += f". {missing_names} {target_type} not found \
            (using {'ID' if use_id else 'name'} to identify them)."

    return message, result_obj


def annotate_object(conn, obj, row, header, namespaces, exclude_empty_value):
    updated = False
    for curr_ns in set(namespaces):
        kv_list = []
        for ns, h, r in zip(namespaces, header, row):
            if ns == curr_ns and (len(r) > 0 or not exclude_empty_value):
                kv_list.append([h, r])
        if len(kv_list) > 0:  # Always exclude empty KV pairs
            # creation and linking of a MapAnnotation
            map_ann = omero.gateway.MapAnnotationWrapper(conn)
            map_ann.setNs(curr_ns)
            map_ann.setValue(kv_list)
            map_ann.save()
            obj.linkAnnotation(map_ann)
            print(f"MapAnnotation:{map_ann.id} created on {obj}")
            updated = True
    return updated


def run_script():
    # Cannot add fancy layout if we want auto fill and selct of object ID
    source_types = [
                    rstring("Project"), rstring("Dataset"), rstring("Image"),
                    rstring("Screen"), rstring("Plate"), rstring("Well"),
                    rstring("Run"), rstring("Image"), rstring("Tag"),
    ]

    # Duplicate Image for UI, but not a problem for script
    target_types = [
                    rstring("<on current>"), rstring("Project"),
                    rstring("- Dataset"), rstring("-- Image"),
                    rstring("Screen"), rstring("- Plate"),
                    rstring("-- Well"), rstring("-- Run"),
                    rstring("--- Image")
    ]

    separators = ["guess", ";", ",", "TAB"]

    client = scripts.client(
        'Import_KV_from_csv',
        """
    Reads a .csv file to annotate target objects with key-value pairs.
    TODO: add hyperlink to readthedocs
    \t
    Parameters:
    \t
    - Data Type: parent-objects type in which target-objects are searched.
    - IDs: IDs of the parent-objects.
    - Target Data Type: Type of the target-objects that will be annotated.
    - File_Annotation: IDs of .csv FileAnnotation or input file.
    - Namespace: Namespace that will be given to the annotations.
    \t
    - Separator: Separator used in the .csv file.
    - Columns to exclude: Columns name of the .csv file to exclude.
    - Target ID colname: Column name in the .csv of the target IDs.
    - Target name colname: Column name in the .csv of the target names.
    \t
    Default namespace: openmicroscopy.org/omero/client/mapAnnotation
    \t
        """,  # Tabs are needed to add line breaks in the HTML

        scripts.String(
            "Data_Type", optional=False, grouping="1",
            description="Parent-data type of the objects to annotate.",
            values=source_types, default="Dataset"),

        scripts.List(
            "IDs", optional=False, grouping="1.1",
            description="List of parent-data IDs containing" +
                        " the objects to annotate.").ofType(rlong(0)),

        scripts.String(
            "Target Data_Type", optional=False, grouping="1.2",
            description="The data type which will be annotated. " +
                        "Entries in the .csv correspond to these objects.",
            values=target_types, default="<on current>"),

        scripts.String(
            "File_Annotation", optional=True, grouping="1.3",
            description="If no file is provided, list of file IDs " +
                        "containing metadata to populate (must match length" +
                        " of 'IDs'). If neither, searches the most recently " +
                        "attached CSV file on each parent object."),

        scripts.String(
            "Namespace (leave blank for default)",
            optional=True, grouping="1.4",
            description="Namespace given to the created key-value " +
                        "pairs annotations. Default is the client" +
                        "namespace, meaning editable in OMERO.web"),

        scripts.Bool(
            "Advanced parameters", optional=True, grouping="2", default=False,
            description="Ticking or unticking this has no effect"),

        scripts.String(
            "Separator", optional=False, grouping="2.1",
            description="The separator used in the .csv file. 'guess' will " +
                        "attempt to detetect automatically which of " +
                        ",;\\t is used.",
            values=separators, default="guess"),

        scripts.List(
            "Columns to exclude", optional=False, grouping="2.2",
            default="<ID>,<NAME>",
            description="List of columns in the .csv file to exclude " +
                        "from the key-value pair import. <ID>" +
                        " and <NAME> correspond to the two " +
                        "following parameters.").ofType(rstring("")),

        scripts.String(
            "Target ID colname", optional=False, grouping="2.3",
            default="OBJECT_ID",
            description="The column name in the .csv containing the id" +
                        " of the objects to annotate. " +
                        "Matches <ID> in exclude parameter."),

        scripts.String(
            "Target name colname", optional=False, grouping="2.4",
            default="OBJECT_NAME",
            description="The column name in the .csv containing the name of " +
                        "the objects to annotate (used if no column " +
                        "ID is provided or  found in the .csv). Matches " +
                        "<NAME> in exclude parameter."),

        scripts.Bool(
            "Exclude empty values", grouping="2.5", default=False,
            description="Exclude a key-value if the value is empty"),

        scripts.Bool(
            "Attach csv to parents", grouping="2.6", default=False,
            description="Attach the given CSV to the parent-data objects" +
            "when not already attached to it."),

        scripts.String(
            "Split value on", optional=True, grouping="2.7",
            default="",
            description="Split values according to that input to " +
            "create key duplicates."),

        authors=["Christian Evenhuis", "Tom Boissonnet"],
        institutions=["MIF UTS", "CAi HHU"],
        contact="https://forum.image.sc/tag/omero"
    )

    try:
        params = parameters_parsing(client)
        print("Input parameters:")
        keys = ["Data_Type", "IDs", "Target Data_Type", "File_Annotation",
                "Namespace (leave blank for default)",
                "Separator", "Columns to exclude", "Target ID colname",
                "Target name colname", "Exclude empty values",
                "Attach csv to parents", "Split value on"]
        for k in keys:
            print(f"\t- {k}: {params[k]}")
        print("\n####################################\n")

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)
        message, robj = keyval_from_csv(conn, params)
        client.setOutput("Message", rstring(message))
        if robj is not None:
            client.setOutput("Result", robject(robj._obj))

    except AssertionError as err:
        # Display assertion errors in OMERO.web activities
        client.setOutput("ERROR", rstring(err))
        raise AssertionError(str(err))

    finally:
        client.closeSession()


def parameters_parsing(client):
    params = {}
    # Param dict with defaults for optional parameters
    params["File_Annotation"] = None
    params["Namespace (leave blank for default)"] = NSCLIENTMAPANNOTATION
    params["Split value on"] = ""

    for key in client.getInputKeys():
        if client.getInput(key):
            params[key] = client.getInput(key, unwrap=True)

    if params["Target Data_Type"] == "<on current>":
        params["Target Data_Type"] = params["Data_Type"]
    elif " " in params["Target Data_Type"]:
        # Getting rid of the trailing '---' added for the UI
        params["Target Data_Type"] = params["Target Data_Type"].split(" ")[1]

    assert params["Target Data_Type"] in ALLOWED_PARAM[params["Data_Type"]], \
           (f"{params['Target Data_Type']} is not a valid target for " +
            f"{params['Data_Type']}.")

    if params["Data_Type"] == "Tag":
        params["Data_Type"] = "TagAnnotation"
        assert None not in params["File_Annotation"], "File annotation \
            ID must be given when using Tag as source"

    if ((params["File_Annotation"]) is not None
            and ("," in params["File_Annotation"])):
        # List of ID provided, have to do the split
        params["File_Annotation"] = params["File_Annotation"].split(",")
    else:
        params["File_Annotation"] = [int(params["File_Annotation"])]
    if len(params["File_Annotation"]) == 1:
        # Poulate the parameter with None or same ID for all source
        params["File_Annotation"] *= len(params["IDs"])
    params["File_Annotation"] = list(map(int, params["File_Annotation"]))

    assert len(params["File_Annotation"]) == len(params["IDs"]), "Number of \
        Source IDs and FileAnnotation IDs must match"

    # Replacing the placeholders <ID> and <NAME> with values from params
    to_exclude = list(map(lambda x: x.replace('<ID>',
                                              params["Target ID colname"]),
                          params["Columns to exclude"]))
    to_exclude = list(map(lambda x: x.replace('<NAME>',
                                              params["Target name colname"]),
                          to_exclude))
    params["Columns to exclude"] = to_exclude

    if params["Separator"] == "guess":
        params["Separator"] = None
    elif params["Separator"] == "TAB":
        params["Separator"] = "\t"

    if params["Data_Type"] == "Run":
        params["Data_Type"] = "Acquisition"
    if params["Target Data_Type"] == "Run":
        params["Target Data_Type"] = "PlateAcquisition"

    assert (params["Separator"] is None
            or params["Separator"] not in params["Split value on"]), (
                "Cannot split cells with a character used as CSV separator"
        )

    return params


if __name__ == "__main__":
    run_script()
