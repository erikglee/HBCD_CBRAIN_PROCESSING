import requests
import os, json, re

def fetch_json_data(tool_config_id):
    url = 'https://portal.cbrain.mcgill.ca/tool_configs/{}/boutiques_descriptor.json'.format(tool_config_id)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json(), url
    else:
        raise Exception(f"Failed to fetch JSON data. Status code: {response.status_code}")


def escape_rst_special_chars(input_string):
    # Define a dictionary of special RST characters to escape
    special_chars = r"[*`_|\\:]"
    
    # Escape each special character by prefixing it with a backslash
    if isinstance(input_string, str):
        escaped_string = re.sub(f"({special_chars})", r"\\\1", input_string)
    else:
        escaped_string = input_string
    
    return escaped_string

def generate_rst(json_data, tool_config_id, tool_name, url, ancestor_pipelines_dict):
    # Example processing: create a new rst file using json data
    keys_to_query = ['name', 'description']
    with open(f'tools/{tool_name}.rst', 'w') as f:
        initial_text = f"{tool_name} (`CBRAIN Tool Config ID: {tool_config_id} <{url}>`_)\n"
        f.write(initial_text)
        f.write(f"{'-'*len(initial_text)}\n\n")
        #f.write('Current boutiques `descriptor <{}>`_ \n'.format(url))
        #f.write('************************************\n\n')
        #f.write(".. list-table::\n")
        #f.write("   :header-rows: 1\n\n")
        f.write(f"* **Container**: {json_data["container-image"]["index"] + json_data["container-image"]["image"]}\n")
        if 'url' in json_data.keys():
            f.write(f"* **Documentation**: {json_data['url']}\n")
        f.write(f"* **Boutiques Descriptor**: {url}\n")
        for temp_key in keys_to_query:
            f.write(f"* **{temp_key}**: {escape_rst_special_chars(json_data[temp_key])}\n")
        f.write("\n\n")

        with open('../../external_requirements/{}.json'.format(tool_name), 'r') as f2:
            external_requirements = json.load(f2)
        with open('../../processing_configurations/{}.json'.format(tool_name), 'r') as f3:
            processing_configurations = json.load(f3)

        f.write("External Requirements\n")
        f.write("*********************\n\n")
        f.write("These are file-based requirements that must be satisfied for processing to occur.\n")
        f.write('"Argument IDs" correspond to input "id"s within the tool Boutiques Descriptor. "Value"s correspond\n')
        f.write('to either specific configuration files (if there is a numeric value), or otherwise broader file types\n')
        f.write('within CBRAIN (if the entry is text-based).\n\n')
        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        f.write("   * - Argument ID\n")
        f.write("     - Flag\n")
        f.write("     - Value\n")
        f.write("     - Description\n")
        for temp_key in external_requirements.keys():
            relevant_input = None
            for temp_input in json_data['inputs']:
                if temp_input['id'] == temp_key:
                    relevant_input = temp_input
                    if 'command-line-flag' in temp_input.keys():
                        flag = temp_input['command-line-flag']
                    else:
                        flag = 'n/a'
            if relevant_input is None:
                raise Exception(f"Could not find input with ID {temp_key} in descriptor")
            f.write(f"   * - {escape_rst_special_chars(temp_key)}\n")
            f.write(f"     - {flag}\n")
            if external_requirements[temp_key].isnumeric() and os.path.exists(os.path.join('cbrain_files', external_requirements[temp_key])):
                f.write(f"     - :download:`{external_requirements[temp_key]} <../cbrain_files/{external_requirements[temp_key]}>`\n")
            else:
                f.write(f"     - {escape_rst_special_chars(external_requirements[temp_key])}\n")
            f.write(f"     - {escape_rst_special_chars(relevant_input['description'])}\n")
        f.write("\n\n")
        if tool_name in ancestor_pipelines_dict.keys():
            if len(ancestor_pipelines_dict[tool_name]) > 0:
                f.write("Ancestor Pipelines\n")
                f.write("******************\n\n")
                f.write("This pipeline utilizes outputs from the following pipelines that\n")
                f.write("are also ran in CBRAIN:\n\n")
                for temp_tool in ancestor_pipelines_dict[tool_name]:
                    f.write(f"- {temp_tool}\n")
                f.write("\n\n")

        f.write("Other Processing Settings\n")
        f.write("*************************\n\n")
        f.write("These additional settings cover everything outside of files that are used as inputs during processing.\n")
        f.write("The settings cover numeric values, flags, outputs directories, and other settings that are used to\n")
        f.write("configure processing.\n\n")
        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        f.write("   * - Argument ID\n")
        f.write("     - Flag\n")
        f.write("     - Value\n")
        f.write("     - Description\n")
        for temp_key in processing_configurations.keys():
            relevant_input = None
            for temp_input in json_data['inputs']:
                if temp_input['id'] == temp_key:
                    relevant_input = temp_input
                    if 'command-line-flag' in temp_input.keys():
                        flag = temp_input['command-line-flag']
                    else:
                        flag = 'n/a'
            if relevant_input is None:
                raise Exception(f"Could not find input with ID {temp_key} in descriptor")
            f.write(f"   * - {escape_rst_special_chars(temp_key)}\n")
            f.write(f"     - {escape_rst_special_chars(flag)}\n")
            f.write(f"     - {escape_rst_special_chars(processing_configurations[temp_key])}\n")
            f.write(f"     - {escape_rst_special_chars(relevant_input['description'])}\n")
        f.write("\n\n\n\n")

        f.write("Pipeline Outputs\n")
        f.write("*************************\n\n")
        f.write("Following processing, a number of files and folders are identified as outputs\n")
        f.write("that should be saved for future reference. In the following table the 'Path Relative to\n")
        f.write("Working Directory' column specifies the location of files that should be saved (with '*'\n")
        f.write("denoting wildcards). The output location for these files in the final 'DataProvider' is specified\n")
        f.write("in the 'Path in Output Data Provider' column. In the case of HBCD the [DERIVATIVES_PREFIX]\n")
        f.write("entry is empty.\n\n")
        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        f.write("   * - ID\n")
        f.write("     - Path Relative to Working Directory\n")
        f.write("     - Path in Output DataProvider\n")
        f.write("     - Description\n")
        for temp_output in json_data['output-files']:
            relevant_input = None
            for temp_input in json_data['inputs']:
                if temp_input['id'] == temp_key:
                    relevant_input = temp_input
                    description = relevant_input['description'].replace('\n', ' ').replace('\r', '')
            if relevant_input is None:
                raise Exception(f"Could not find input with ID {temp_key} in descriptor")
            f.write(f"   * - {escape_rst_special_chars(temp_output['id'])}\n")
            f.write(f"     - {escape_rst_special_chars(temp_output['path-template'])}\n")
            try:
                output_path = json_data["custom"]["cbrain:integrator_modules"]["BoutiquesForcedOutputBrowsePath"][temp_output['id']]
                f.write(f"     - {escape_rst_special_chars(output_path)}\n")
            except:
                f.write(f"     - NA\n")
            f.write(f"     - {escape_rst_special_chars(temp_output['description'])}\n")

        f.write("\n\n\n")
        f.write("Command Line Template\n")
        f.write("*********************\n\n")
        f.write("The following code-snippet highlights how the tool is referenced on the command-line.\n")
        f.write("The code being displayed is executed within the tool's container. Some of the directives\n")
        f.write("may describe file manipulations to prepare for processing, and other directives will describe\n")
        f.write("the primary processing command. At the time of processing, the text in brackets will be replaced\n")
        f.write("by text that has been provided to configure processing. ::\n\n")
        f.write("   {}\n\n".format(json_data['command-line']))
        
        comp_proc_recs_dir = '../../comprehensive_processing_prerequisites'
        requirements_files = []
        for filename in os.listdir(comp_proc_recs_dir):
            # Use regular expression to match pipeline name with or without underscore and number
            match = re.match(rf"^{tool_name}(?:_[0-9]+)?\.json$", filename)
            if match:
                requirements_files.append(os.path.join(comp_proc_recs_dir, filename))
        requirements_files.sort()
        if len(requirements_files) > 0:
            is_qc_used = False
            f.write("File Selection For Processing\n")
            f.write("*****************************\n\n")
            #f.write("The data for one imaging session (i.e. sub-1, ses-V02) must\n")
            #f.write("satisfy at least one of the following requirements for processing.\n")
            #f.write("Processing can occur if at least one requirement is satisfied.\n")
            #f.write("If processing can occur, then any files that match any requirements \n")
            #f.write("will be considered for processing. If only one table is seen below,\n")
            #f.write("this means the pipeline only has one set of possible requirements.\n\n")

            f.write("The first step of selecting a candidate for processing is determining whether\n")
            f.write("the right files are present. In HBCD processing, pipelines are always run on\n")
            f.write("one session worth of data at a time. With that in mind, we (mostly) query the\n")
            f.write("contents of a subject's session folder to determine both if processing should occur\n")
            f.write("and also which files should be included in processing. For every pipeline there will be\n")
            f.write("at least one requirement group that determines what files are needed for processing to occur. Within\n")
            f.write("a requirement group, there may be criteria that address multiple file (or modality) types, which are known as 'File Groups'.\n")
            f.write("For processing to occur, the correct number of files surviving all Included/Excluded\n")
            f.write("terms for a given 'File Group' must be present. To allow for more flexible selection of\n")
            f.write("files for processing, there are often multiple 'Requirement Groups'. The contents of each 'File Group'\n")
            f.write("across 'Requirement Groups' must be the same, but which 'File Groups' are defined can be different.\n")
            f.write("If multiple 'Requirement Groups' are present for the current pipeline, there will be multiple tables\n")
            f.write("in this section. Only one 'Requirement Group' needs to be satisfied for processing to occur. If one\n")
            f.write("requirement group is satisfied, then files from any 'File Group' (across requirement groups) will be included in processing.\n\n")

            f.write("Beyond the files chosen from this procedure, associated files defined via the table :doc:`here <../associated_files>` will also\n")
            f.write("be included in processing.\n\n")

            for filename in requirements_files:
                with open(filename, 'r') as f2:
                    temp_processing_reqs = json.load(f2)
                requirement_name = 'Requirement Group: ' + os.path.basename(filename).replace('.json', '')
                f.write(f"{requirement_name}\n")
                f.write(f"{'~'*len(os.path.basename(filename))}\n\n")
                f.write(".. list-table::\n")
                f.write("   :header-rows: 1\n\n")
                f.write("   * - File Group\n")
                f.write("     - Term\n")
                f.write("     - Included (True)/Excluded (False)\n")
                for temp_key in temp_processing_reqs.keys():
                    file_naming_dict = temp_processing_reqs[temp_key]['file_naming']
                    if 'qc_criteria' in temp_processing_reqs[temp_key].keys():
                        is_qc_used = True
                    for i, temp_inner_key in enumerate(file_naming_dict.keys()):
                        if i == 0:
                            f.write(f"   * - {escape_rst_special_chars(temp_key)}\n")
                        else:
                            f.write(f"   * -   \n")
                        f.write(f"     - {escape_rst_special_chars(temp_inner_key)}\n")
                        f.write(f"     - {escape_rst_special_chars(file_naming_dict[temp_inner_key])}\n")



                f.write("\n\n")
            if is_qc_used:
                f.write("Quality Control Selection Information\n")
                f.write("**************************************\n\n")
                f.write("The requirements listed in this section are used to determine if processing\n")
                f.write("should occur and to determine which files are best suited for processing. If a 'File\n")
                f.write("Group' has a table in this section, the file selection procedure will go row-by-row through\n")
                f.write("the table to evaluate files for processing. The first row is considered the\n")
                f.write("most important during this procedure, and the last row the least important.\n")
                f.write("To make a proper comparison, all fields must be defined for all files in a group. In the\n")
                f.write("case that a field is not defined for at least one file, a backup set ofcriteria may\n")
                f.write("be used. If a back-up set of criteria is defined, that will be represented as a second table.\n\n")
                for temp_key in temp_processing_reqs.keys():
                    if 'qc_criteria' in temp_processing_reqs[temp_key].keys():
                        f.write(f"File Group: {temp_key}\n")
                        f.write(f"{'~'*(len(temp_key) + 12)}\n\n")
                        if "num_to_keep" in temp_processing_reqs[temp_key].keys():
                            f.write("Processing will look for best {} file(s) to keep using the following criteria.\n\n".format(temp_processing_reqs[temp_key]["num_to_keep"]))
                        else:
                            f.write("Processing will include all files passing the following criteria.\n\n")
                        outer_qc_list = temp_processing_reqs[temp_key]['qc_criteria']
                        for j, inner_qc_list in enumerate(outer_qc_list):
                            f.write(".. list-table::\n")
                            f.write("   :header-rows: 1\n\n")
                            f.write("   * - scans.tsv Field\n")
                            f.write("     - Operator\n")
                            f.write("     - Value\n")
                            for temp_qc in inner_qc_list:
                                key = next(iter(temp_qc))
                                f.write(f"   * - {escape_rst_special_chars(key)}\n")
                                f.write(f"     - {escape_rst_special_chars(temp_qc[key][1])}\n")
                                f.write(f"     - {escape_rst_special_chars(temp_qc[key][0])}\n")
                            f.write("\n\n\n")

        #f.write(".. toctree::\n")
        #f.write("   :maxdepth: 2\n")
        #f.write("   :caption: Related Files\n\n")
        #f.write("   ../associated_files\n")


    
def make_associated_files_rst():

    with open('../../associated_files.json') as f:
        associated_files = json.load(f)

    with open('associated_files.rst', 'w') as f:
        f.write("Associated Files\n")
        f.write("****************\n\n")
        f.write("During HBCD processing, we search for files with specific names or\n")
        f.write("quality scores to include in processing. Beyond these files that are\n")
        f.write("explicitly selected, there are a number of associated files that are implicitly\n")
        f.write("selected. The following table highlights the convention for this implicit selection.\n")
        f.write("For any file included in processing, the file selection routines will check if the\n")
        f.write("file has one of the endings listed in the 'Original File Name Term' column. Then if\n")
        f.write("a replacement of that term with the entry in the 'Associated File Term' column yields\n")
        f.write("the name of another file in the BIDS dataset, that file will also be included\n")
        f.write("during processing.\n\n")
        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        f.write("   * - Original File Name Term\n")
        f.write("     - Associated File Term\n")

        for temp_key in associated_files.keys():
            for i, temp_element in enumerate(associated_files[temp_key]):
                if i == 0:
                    f.write(f"   * - {temp_key}\n")
                    f.write(f"     - {temp_element}\n")
                else:
                    f.write(f"   * -   \n")
                    f.write(f"     - {temp_element}\n")
        f.write("\n\n\n")


def main():

    print(os.listdir())

    with open('tools_to_feature_in_documentation.txt', 'r') as f:
        tools_for_documentation = [line.strip() for line in f]
    print(tools_for_documentation)
    with open('../../tool_config_ids.json') as f:
        tool_config_ids = json.load(f)
    with open('../../ancestor_pipelines.json') as f:
        ancestor_pipelines = json.load(f)

    os.makedirs('tools')
    for temp_tool in tools_for_documentation:
        print('Adding {} to documentation'.format(temp_tool))
        #with open('tool_details.rst', 'a') as f:
        #    f.write(f"* :doc:`tools/{temp_tool}`\n")
        json_data, url = fetch_json_data(tool_config_ids[temp_tool])
        generate_rst(json_data, tool_config_ids[temp_tool], temp_tool, url, ancestor_pipelines)

    with open('tool_details.rst', 'a') as f:
        f.write("\n\n\n")
        f.write(".. toctree::\n")
        f.write("   :maxdepth: 2\n\n")
        for temp_tool in tools_for_documentation:
            f.write(f"   tools/{temp_tool}\n")
        #f.write("   ../associated_files\n")


    make_associated_files_rst()
    print('Made associated files RST')

if __name__ == "__main__":
    main()