# coding=utf-8
"""
 MIF/Key_Val_to_csv.py

 Reads the metadata associated with the images in a dataset
 a creates a csv file attached to dataset

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
from omero.rtypes import rstring, rlong
import omero.scripts as scripts
from omero.cmd import Delete2

import tempfile
import os
from collections import OrderedDict

CHILD_OBJECTS = {
                    "Project": "Dataset",
                    "Dataset": "Image",
                    "Screen": "Plate",
                    "Plate": "Well",
                    #"Run": ["Well", "Image"],
                    "Well": "WellSample",
                    "WellSample": "Image"
                }

ZERO_PADDING = 3 # To allow duplicated keys (3 means up to 1000 duplicate key on a single object)

def get_existing_map_annotions(obj, namespace, zero_padding):
    key_l = []
    result = OrderedDict()
    for ann in obj.listAnnotations(ns=namespace):
        if isinstance(ann, omero.gateway.MapAnnotationWrapper):
            for (k,v) in ann.getValue():
                n_occurence = key_l.count(k)
                result[f"{str(n_occurence).rjust(zero_padding, '0')}{k}"] = v
                key_l.append(k) # To count the multiple occurence of keys
    return result

def group_keyvalue_dictionaries(annotation_dicts, zero_padding):
    """ Groups the keys and values of each object into a single dictionary """
    all_key = OrderedDict() # To keep the keys in order, for what it's worth
    for annotation_dict in annotation_dicts:
        all_key.update({k:None for k in annotation_dict.keys()})
    all_key = list(all_key.keys())

    result = []
    for annotation_dict in annotation_dicts:
        obj_dict = OrderedDict((k, "") for k in all_key)
        obj_dict.update(annotation_dict)
        for k,v in obj_dict.items():
            if v is None:
                obj_dict[k]
        result.append(list(obj_dict.values()))

    all_key = [key[zero_padding:] for key in all_key] # Removing temporary padding
    return all_key, result

def get_children_recursive(source_object, target_type):
    if CHILD_OBJECTS[source_object.OMERO_CLASS] == target_type: # Stop condition, we return the source_obj children
        if source_object.OMERO_CLASS != "WellSample":
            return source_object.listChildren()
        else:
            return [source_object.getImage()]
    else:
        result = []
        for child_obj in source_object.listChildren():
            # Going down in the Hierarchy list for all childs that aren't yet the target
            result.extend(get_children_recursive(child_obj, target_type))
        return result

def attach_csv_file(conn, source_object, obj_id_l, obj_name_l, obj_ancestry_l, annotation_dicts, separator):
    def to_csv(ll):
        """convience function to write a csv line"""
        nl = len(ll)
        fmstr = ("{}"+separator+" ")*(nl-1)+"{}\n"
        return fmstr.format(*ll)

    all_key, whole_values_l = group_keyvalue_dictionaries(annotation_dicts, ZERO_PADDING)

    counter = 0
    if len(obj_ancestry_l)>0: # If there's anything to add at all
        for (parent_type, _) in obj_ancestry_l[0]:
            all_key.insert(counter, parent_type); counter += 1
    all_key.insert(counter, "target_id")
    all_key.insert(counter + 1, "target_name")
    for k, (obj_id, obj_name, whole_values) in enumerate(zip(obj_id_l, obj_name_l, whole_values_l)):
        counter = 0
        if len(obj_ancestry_l)>0: # If there's anything to add at all
            for (_, parent_name) in obj_ancestry_l[k]:
                whole_values.insert(counter, parent_name); counter += 1
        whole_values.insert(counter, obj_id)
        whole_values.insert(counter + 1, obj_name)

    # create the tmp directory
    tmp_dir = tempfile.mkdtemp(prefix='MIF_meta')
    (fd, tmp_file) = tempfile.mkstemp(dir=tmp_dir, text=True)
    tfile = os.fdopen(fd, 'w')
    tfile.write(to_csv(all_key))
    # write the keys values for each file
    for whole_values in whole_values_l:
        tfile.write(to_csv(whole_values))
    tfile.close()

    source_name = source_object.getWellPos() if source_object.OMERO_CLASS == "Well" else source_object.getName()
    name = "{}_metadata_out.csv".format(source_name)
    # link it to the object
    ann = conn.createFileAnnfromLocalFile(
        tmp_file, origFilePathAndName=name,
        ns='MIF_test')
    ann = source_object.linkAnnotation(ann)

    # remove the tmp file
    os.remove(tmp_file)
    os.rmdir(tmp_dir)
    return "done"


def main_loop(conn, script_params):
    ''' writes the data (list of dicts) to a file
    @param conn:             Blitz Gateway connection wrapper
    @param script_params:     A map of the input parameters
    '''
    source_type = script_params["Source_object_type"]
    target_type = script_params["Target_object_type"]
    source_ids = script_params["Source_IDs"]
    namespace = script_params["Namespace (leave blank for default)"]
    separator = script_params["Separator"]
    include_parent = script_params["Include column(s) for parent objects name"]

    # One file output per given ID
    for source_object in conn.getObjects(source_type, source_ids):
        obj_ancestry_l = []
        if source_type == target_type:
            print("Processing object:", source_object)
            annotation_dicts = [get_existing_map_annotions(source_object, namespace, ZERO_PADDING)]
            obj_id_l = [source_object.getId()]
            obj_name_l = [source_object.getWellPos() if source_object.OMERO_CLASS == "Well" else source_object.getName()]
        else:
            annotation_dicts = []
            obj_id_l, obj_name_l = [], []

            if source_type == "TagAnnotation":
                target_obj_l = conn.getObjectsByAnnotations(target_type, [source_object.getId()])
                target_obj_l = list(conn.getObjects(target_type, [o.getId() for o in target_obj_l])) # Need that to load annotations later
                source_object = target_obj_l[0] # Putting the csv file on the first child
            else:
                print(source_object, target_type)
                target_obj_l = get_children_recursive(source_object, target_type)
            # Listing all target children to the source object (eg all images (target) in all datasets of the project (source))
            for target_obj in target_obj_l:
                print("Processing object:", target_obj)
                annotation_dicts.append(get_existing_map_annotions(target_obj, namespace, ZERO_PADDING))
                obj_id_l.append(target_obj.getId())
                obj_name_l.append(target_obj.getWellPos() if target_obj.OMERO_CLASS == "Well" else target_obj.getName())
                if include_parent:
                    ancestry = [(o.OMERO_CLASS, o.getWellPos() if o.OMERO_CLASS == "Well" else o.getName())
                                for o in target_obj.getAncestry() if o.OMERO_CLASS != "WellSample"][::-1]
                    obj_ancestry_l.append(ancestry)


        mess = attach_csv_file(conn, source_object, obj_id_l, obj_name_l, obj_ancestry_l, annotation_dicts, separator)
        print(mess)

        # for ds in datasets:
        #     # name of the file
        #     csv_name = "{}_metadata_out.csv".format(ds.getName())
        #     print(csv_name)

            # # remove the csv if it exists
            # for ann in ds.listAnnotations():
            #     if(isinstance(ann, omero.gateway.FileAnnotationWrapper)):
            #         if(ann.getFileName() == csv_name):
            #             # if the name matches delete it
            #             try:
            #                 delete = Delete2(
            #                     targetObjects={'FileAnnotation':
            #                                    [ann.getId()]})
            #                 handle = conn.c.sf.submit(delete)
            #                 conn.c.waitOnCmd(
            #                     handle, loops=10,
            #                     ms=500, failonerror=True,
            #                     failontimeout=False, closehandle=False)
            #                 print("Deleted existing csv")
            #             except Exception as ex:
            #                 print("Failed to delete existing csv: {}".format(
            #                     ex.message))
            #     else:
            #         print("No exisiting file")

            # assemble the metadata into an OrderedDict

def run_script():
    """
    The main entry point of the script, as called by the client via the
    scripting service, passing the required parameters.
    """

    source_types = [rstring("Project"), rstring("- Dataset"), rstring("-- Image"),
                    rstring("Screen"), rstring("- Plate"),
                    rstring("-- Well"), rstring("--- Image"), #Duplicate Image for UI, but not a problem for script
                    rstring("Tag")]

    target_types = [rstring("Project"),
                    rstring("- Dataset"), rstring("-- Image"),
                    rstring("Screen"), rstring("- Plate"),
                    rstring("-- Well"), rstring("--- Image")]

    agreement = "I understand what I am doing and that this will result in a batch deletion of key-value pairs from the server"
    separators = [";", ","]
    # Here we define the script name and description.
    # Good practice to put url here to give users more guidance on how to run
    # your script.
    client = scripts.client(
        'KeyVal_to_csv.py',
        ("Export key-value pairs of targets to .csv file"
         " \nSee"
         " http://www.openmicroscopy.org/site/support/omero5.2/developers/"
         "scripts/user-guide.html for the tutorial that uses this script."),

        scripts.String(
            "Source_object_type", optional=False, grouping="1",
            description="Choose the object type containing the objects to delete annotation from",
            values=source_types, default="- Dataset"),

        scripts.List(
            "Source_IDs", optional=False, grouping="1.1",
            description="List of source IDs").ofType(rlong(0)),

        scripts.String(
            "Target_object_type", optional=True, grouping="1.2",
            description="Choose the object type to delete annotation from",
            values=target_types, default="-- Image"),

        scripts.String(
            "Namespace (leave blank for default)", optional=True, grouping="1.3",
            description="Choose a namespace for the annotations"),

        scripts.String(
            "Separator", optional=False, grouping="3",
            description="Choose the .csv separator",
            values=separators, default=";"),

        scripts.Bool(
            "Include column(s) for parent objects name", optional=False, grouping="3",
            description="Weather to include or not the name of the parent(s) objects as columns in the .csv", default=False),

        authors=["Christian Evenhuis", "MIF", "Tom Boissonnet"],
        institutions=["University of Technology Sydney", "CAi HHU"],
        contact="https://forum.image.sc/tag/omero",
        version="2.0.0"
    )

    try:
        script_params = {
            "Namespace (leave blank for default)": omero.constants.metadata.NSCLIENTMAPANNOTATION
        }
        for key in client.getInputKeys():
            if client.getInput(key):
                # unwrap rtypes to String, Integer etc
                script_params[key] = client.getInput(key, unwrap=True)

        # Getting rid of the trailing '---' added for the UI
        tmp_src = script_params["Source_object_type"]
        script_params["Source_object_type"] = tmp_src.split(" ")[1] if " " in tmp_src else tmp_src
        tmp_trg = script_params["Target_object_type"]
        script_params["Target_object_type"] = tmp_trg.split(" ")[1] if " " in tmp_trg else tmp_trg

        print(script_params)   # handy to have inputs in the std-out log

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)

        # do the editing...
        message = main_loop(conn, script_params)
        client.setOutput("Message", rstring(message))

    except AssertionError as err: #Display assertion errors in OMERO.web activities
        client.setOutput("ERROR", rstring(err))
        raise AssertionError(str(err))
    finally:
        client.closeSession()

if __name__ == "__main__":
    run_script()