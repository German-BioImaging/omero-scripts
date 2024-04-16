# coding=utf-8
"""
 KeyVals_To_Tag.py

 Adds Tags to a target object on OMERO from its key-value pairs

-----------------------------------------------------------------------------
  Copyright (C) 2018 - 2024
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
Created by Tom Boissonnet

"""

import omero
from omero.gateway import BlitzGateway, TagAnnotationWrapper
from omero.rtypes import rstring, rlong, robject
import omero.scripts as scripts
from omero.constants.metadata import NSCLIENTMAPANNOTATION, NSINSIGHTTAGSET
from omero.model import AnnotationAnnotationLinkI

from collections import defaultdict
import re


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
    "Screen": ["Screen", "Plate", "Well", "Acquisition", "Image"],
    "Plate": ["Plate", "Well", "Acquisition", "Image"],
    "Well": ["Well", "Image"],
    "Acquisition": ["Acquisition", "Image"],
    "Tag": ["Project", "Dataset", "Image",
            "Screen", "Plate", "Well", "Acquisition"]
}

P_DTYPE = "Data_Type"  # Do not change
P_FILE_ANN = "File_Annotation"  # Do not change
P_IDS = "IDs"  # Do not change
P_TARG_DTYPE = "Target Data_Type"
P_NAMESPACE = "Namespace (blank for default)"
P_IMPORT_TAGS = "Import tags"
P_OWN_TAG = "Only use personal tags"
P_ALLOW_NEWTAG = "Allow tag creation"
P_RGX_KEY = "Key name include"
P_RGX_EXCL_KEY = "Key name exclude"


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


def main_loop(conn, script_params):
    """
    For every object:
     - Find all map annotation
     - Find matching tags and create new if needed & allowed
    Finalize:
     - Attach the tags
    """
    source_type = script_params[P_DTYPE]
    target_type = script_params[P_TARG_DTYPE]
    source_ids = script_params[P_IDS]
    namespace_l = script_params[P_NAMESPACE]
    regex_l = script_params[P_RGX_KEY]
    regex_exclude_l = script_params[P_RGX_EXCL_KEY]
    use_personal_tags = script_params[P_OWN_TAG]
    create_new_tags = script_params[P_ALLOW_NEWTAG]

    result_obj = None

    # Dictionaries needed for the tags
    tag_d, tagset_d, tagtree_d, tagid_d = None, None, None, None

    # One file output per given ID
    source_objects = conn.getObjects(source_type, source_ids)
    target_obj_d, target_keyval_d = {}, {}
    pattern_l = [re.compile(r) for r in regex_l]
    pattern_excl_l = [re.compile(r) for r in regex_exclude_l]
    for source_object in source_objects:
        is_tag = source_type == "TagAnnotation"
        for target_obj in target_iterator(conn, source_object,
                                          target_type, is_tag):
            if target_obj.getId() in target_obj_d.keys():
                continue

            kv_l_tmp = get_existing_map_annotations(target_obj,
                                                    namespace_l)
            target_obj_d[target_obj.getId()] = target_obj

            kv_l = set()
            for pattern in pattern_l:
                kv_l.update([(k, v) for k, v in kv_l_tmp if pattern.search(k)])
            for pattern in pattern_excl_l:
                excl = [(k, v) for k, v in kv_l_tmp if pattern.search(k)]
                kv_l.difference_update(excl)

            target_keyval_d[target_obj.getId()] = kv_l
            if result_obj is None:
                result_obj = target_obj

    # Get existing tags
    tag_d, tagset_d, tagtree_d, tagid_d = get_tag_dict(
        conn, use_personal_tags
    )

    # Replace the tags in the CSV by the tag_id to use
    obj_totag_d, tag_d, tagset_d, tagtree_d, tagid_d = preprocess_tag_rows(
        conn, target_keyval_d, tag_d, tagset_d,
        tagtree_d, tagid_d, create_new_tags
    )

    updated_count = annotate_objects(
        conn, target_obj_d, obj_totag_d, tagid_d
    )

    message = f"Added Annotations to \
        {updated_count}/{len(target_obj_d)} {target_type}(s)"

    return message, result_obj


def get_existing_map_annotations(obj, namespace_l):
    keyval_l = set()
    for namespace in namespace_l:
        p = {} if namespace == "*" else {"ns": namespace}
        for ann in obj.listAnnotations(**p):
            if isinstance(ann, omero.gateway.MapAnnotationWrapper):
                for (k, v) in ann.getValue():
                    keyval_l.add((k.strip(), v.strip()))

    return keyval_l


def annotate_objects(conn, target_obj_d, obj_totag_d, tagid_d):
    updated_count = 0
    for obj_id, obj in target_obj_d.items():
        print(f"-->processing {obj}")
        updated = False
        tag_id_l = obj_totag_d[obj_id]
        exist_ids = [ann.getId() for ann in obj.listAnnotations()]
        for tag_id in tag_id_l:
            if tag_id not in exist_ids:
                tag_ann = tagid_d[tag_id]
                obj.linkAnnotation(tag_ann)
                exist_ids.append(tag_id)
                print(f"TagAnnotation:{tag_ann.id} created on {obj}")
                updated = True
        updated_count += int(updated)

    return updated_count


def get_tag_dict(conn, use_personal_tags):
    """
    Generate dictionnaries of the tags in the group.

    Parameters:
    --------------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    use_personal_tags: ``Boolean``, indicates the use of only tags
    owned by the user.

    Returns:
    -------------
    tag_d: dictionary of tag_ids {"tagA": [12], "tagB":[34,56]}
    tagset_d: dictionary of tagset_ids {"tagsetX":[78]}
    tagtree_d: dictionary of tags in tagsets {"tagsetX":{"tagA":[12]}}
    tagid_d: dictionary of tag objects {12:tagA_obj, 34:tagB_obj}

    """
    tagtree_d = defaultdict(lambda: defaultdict(list))
    tag_d, tagset_d = defaultdict(list), defaultdict(list)
    tagid_d = {}

    max_id = -1

    uid = conn.getUserId()
    for tag in conn.getObjects("TagAnnotation"):
        is_owner = tag.getOwner().id == uid
        if use_personal_tags and not is_owner:
            continue

        tagid_d[tag.id] = tag
        max_id = max(max_id, tag.id)
        tagname = tag.getValue()
        if (tag.getNs() == NSINSIGHTTAGSET):
            # It's a tagset
            tagset_d[tagname].append((int(is_owner), tag.id))
            for lk in conn.getAnnotationLinks("TagAnnotation",
                                              parent_ids=[tag.id]):
                # Add all tags of this tagset in the tagtree
                cname = lk.child.textValue.val
                cid = lk.child.id.val
                cown = int(lk.child.getDetails().owner.id.val == uid)
                tagtree_d[tagname][cname].append((cown, cid))
        else:
            tag_d[tagname].append((int(is_owner), tag.id))

    # Sorting the tag by index (and if owned or not)
    # to keep only one
    for k, v in tag_d.items():
        v.sort(key=lambda x: (x[0]*max_id + x[1]))
        tag_d[k] = v[0][1]
    for k, v in tagset_d.items():
        v.sort(key=lambda x: (x[0]*max_id + x[1]))
        tagset_d[k] = v[0][1]
    for k1, v1 in tagtree_d.items():
        for k2, v2 in v1.items():
            v2.sort(key=lambda x: (x[0]*max_id + x[1]))
            tagtree_d[k1][k2] = v2[0][1]

    return tag_d, tagset_d, tagtree_d, tagid_d


def preprocess_tag_rows(conn, target_keyval_d, tag_d, tagset_d,
                        tagtree_d, tagid_d,
                        create_new_tags):
    """
    Replace the tags in the rows by tag_ids.
    All done in preprocessing means that the script will fail before
    annotations process starts.
    """
    update = conn.getUpdateService()
    obj_totag_d = {}
    for obj_id, target_keyval in target_keyval_d.items():
        tagid_l = []
        for tagset, tagname in target_keyval:
            has_tagset = (tagset is not None and tagset != "")
            if tagname == "":
                continue

            if not has_tagset:
                tag_exist = tagname in tag_d.keys()
                if not (tag_exist or create_new_tags):
                    # Silently skipping non existing tags
                    continue

                if not tag_exist:
                    tag_o = TagAnnotationWrapper(conn)
                    tag_o.setValue(tagname)
                    tag_o.save()
                    tagid_d[tag_o.id] = tag_o
                    tag_d[tagname] = tag_o.id
                    print(f"creating new Tag for '{tagname}'")
                tagid_l.append(tag_d[tagname])

            else:  # has tagset
                tagset_exist = tagset in tagset_d.keys()
                tag_exist = (tagset_exist
                             and (tagname in tagtree_d[tagset].keys()))
                if not (tag_exist or create_new_tags):
                    # Silently skipping non existing tags
                    continue
                if not tag_exist:
                    tag_o = TagAnnotationWrapper(conn)
                    tag_o.setValue(tagname)
                    tag_o.save()
                    tagid_d[tag_o.id] = tag_o
                    tag_d[tagname] = tag_o.id
                    if not tagset_exist:
                        tagset_o = TagAnnotationWrapper(conn)
                        tagset_o.setValue(tagset)
                        tagset_o.setNs(NSINSIGHTTAGSET)
                        tagset_o.save()
                        tagid_d[tagset_o.id] = conn.getObject(
                            "TagAnnotation",
                            tagset_o.id
                        )
                        tagset_d[tagset] = tagset_o.id
                        print(f"Created new TagSet {tagset}:{tagset_o.id}")

                    tagset_o = tagid_d[tagset_d[tagset]]
                    link = AnnotationAnnotationLinkI()
                    link.parent = tagset_o._obj
                    link.child = tag_o._obj
                    update.saveObject(link)
                    tagtree_d[tagset][tagname] = tag_o.id
                    print(f"creating new Tag for '{tagname}' " +
                          f"in the tagset '{tagset}'")
                tagid_l.append(tagtree_d[tagset][tagname])

        # assign list of tag ids to annotate for each object
        obj_totag_d[obj_id] = tagid_l
    return obj_totag_d, tag_d, tagset_d, tagtree_d, tagid_d


def run_script():
    # Cannot add fancy layout if we want auto fill and selct of object ID
    source_types = [
                    rstring("Project"), rstring("Dataset"), rstring("Image"),
                    rstring("Screen"), rstring("Plate"), rstring("Well"),
                    rstring("Acquisition"), rstring("Image"), rstring("Tag"),
    ]

    # Duplicate Image for UI, but not a problem for script
    target_types = [
                    rstring("<on current>"), rstring("Project"),
                    rstring("- Dataset"), rstring("-- Image"),
                    rstring("Screen"), rstring("- Plate"),
                    rstring("-- Well"), rstring("-- Acquisition"),
                    rstring("--- Image")
    ]

    client = scripts.client(
        'KeyVals to Tags',
        """
    Annotate the given objects with tags from existing key-value
    pairs.
    \t
    Check the guide for more information on parameters and errors:
    https://guide-kvpairs-scripts.readthedocs.io/en/latest/index.html
    \t
    Default namespace: openmicroscopy.org/omero/client/mapAnnotation
        """,  # Tabs are needed to add line breaks in the HTML

        scripts.String(
            P_DTYPE, optional=False, grouping="1",
            description="Parent-data type of the objects to process.",
            values=source_types, default="Dataset"),

        scripts.List(
            P_IDS, optional=False, grouping="1.1",
            description="List of parent-data IDs containing" +
                        " the objects to annotate.").ofType(rlong(0)),

        scripts.String(
            P_TARG_DTYPE, optional=False, grouping="1.2",
            description="The data type which will be processed.",
            values=target_types, default="<on current>"),

        scripts.List(
            P_NAMESPACE,
            optional=True, grouping="1.3",
            description="The namespace(s) of the key-value pairs to " +
                        "process.").ofType(rstring("")),

        scripts.Bool(
            P_OWN_TAG, grouping="1.4", default=False,
            description="Determines if tags of other users in the group" +
            " can be used on objects.\n Using only personal tags might " +
            "lead to multiple tags with the same name in one OMERO-group."),

        scripts.Bool(
            P_ALLOW_NEWTAG, grouping="1.5", default=False,
            description="Creates new tags and tagsets if the entries" +
            " found in the key-value pairs do not exist as tags."),

        scripts.List(
            P_RGX_KEY, optional=True, grouping="1.6",
            description="A list of regex. If provided, only the keys" +
                        " matching at least one of the regex are" +
                        " processed.").ofType(rstring("")),

        scripts.List(
            P_RGX_EXCL_KEY, optional=True, grouping="1.7",
            description="A list of regex. All the keys matching one of the" +
                        " regex are exclude.").ofType(rstring("")),

        authors=["Tom Boissonnet"],
        institutions=["CAi HHU"],
        contact="https://forum.image.sc/tag/omero",
        version="2.0.0",
    )

    try:
        params = parameters_parsing(client)

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)
        message, robj = main_loop(conn, params)
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
    params[P_NAMESPACE] = [NSCLIENTMAPANNOTATION]
    params[P_RGX_KEY] = [".*"]
    params[P_RGX_EXCL_KEY] = []

    for key in client.getInputKeys():
        if client.getInput(key):
            params[key] = client.getInput(key, unwrap=True)

    if params[P_TARG_DTYPE] == "<on current>":
        params[P_TARG_DTYPE] = params[P_DTYPE]
    elif " " in params[P_TARG_DTYPE]:
        # Getting rid of the trailing '---' added for the UI
        params[P_TARG_DTYPE] = params[P_TARG_DTYPE].split(" ")[1]

    assert params[P_TARG_DTYPE] in ALLOWED_PARAM[params[P_DTYPE]], \
           (f"{params['Target Data_Type']} is not a valid target for " +
            f"{params['Data_Type']}.")

    # Remove duplicate entries from namespace list
    tmp = params[P_NAMESPACE]
    if "*" in tmp:
        tmp = ["*"]
    params[P_NAMESPACE] = list(set(tmp))

    print("Input parameters:")
    keys = [P_DTYPE, P_IDS, P_TARG_DTYPE, P_NAMESPACE,
            P_OWN_TAG, P_ALLOW_NEWTAG, P_RGX_KEY, P_RGX_EXCL_KEY]

    for k in keys:
        print(f"\t- {k}: {params[k]}")
    print("\n####################################\n")

    if params[P_DTYPE] == "Tag":
        params[P_DTYPE] = "TagAnnotation"
    if params[P_TARG_DTYPE] == "Acquisition":
        params[P_TARG_DTYPE] = "PlateAcquisition"

    return params


if __name__ == "__main__":
    run_script()
